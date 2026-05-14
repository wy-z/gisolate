"""ProcessPublisher / ProcessSubscriber: ZMQ PUB/SUB one-way fan-out.

Use ``ProcessBridge`` when you need request/response RPC.
Use these when you need one-way fan-out (snapshots, signals, heartbeats) where
message loss is acceptable — PUB drops messages for slow subscribers.
"""

import asyncio
import contextlib
import logging
import os
from typing import Any, Awaitable, Callable

from ._internal import Serializer, SmartPickle

log = logging.getLogger(__name__)

Handler = Callable[[str, Any], Awaitable[None]]


def _safe_close(sock: Any, ctx: Any) -> None:
    """Best-effort tear down of a ZMQ socket + context. Swallows errors."""
    with contextlib.suppress(Exception):
        if sock is not None:
            sock.close(linger=0)
    with contextlib.suppress(Exception):
        if ctx is not None:
            ctx.term()


# ---------------------------------------------------------------------------
# Publisher (gevent side)
# ---------------------------------------------------------------------------


class ProcessPublisher:
    """ZMQ PUB socket for one-way fan-out from a gevent producer.

    Topic-based dispatch with a pluggable serializer (default
    :class:`SmartPickle`). ``publish`` is non-blocking — slow subscribers
    cause messages to be dropped once the high-water mark is hit, matching
    standard PUB semantics.

    Args:
        address: IPC/TCP address (e.g., ``"ipc:///tmp/stream.sock"``).
        serializer: Optional serializer; defaults to :class:`SmartPickle`.
        sndhwm: Send high-water mark. Beyond this, messages are dropped.

    Example::

        pub = ProcessPublisher("ipc:///tmp/stream.sock").start()
        pub.publish("v1.snapshot.AAPL", {"price": 150.0})
        pub.close()
    """

    def __init__(
        self,
        address: str,
        *,
        serializer: Serializer = SmartPickle,
        sndhwm: int = 1000,
    ):
        self._addr = address
        self._serializer = serializer
        self._sndhwm = sndhwm
        self._started = False
        self._sock: Any = None
        self._ctx: Any = None
        self._send_lock: Any = None

    def __del__(self):
        # Best-effort sync cleanup. Avoid full close() — at GC time the
        # gevent hub may be torn down, so acquiring the send-lock can fail.
        # The ipc file is left on disk (process is exiting anyway).
        _safe_close(getattr(self, "_sock", None), getattr(self, "_ctx", None))

    def __enter__(self) -> "ProcessPublisher":
        return self.start()

    def __exit__(self, *_) -> None:
        self.close()

    @property
    def address(self) -> str:
        """IPC/TCP address."""
        return self._addr

    def start(self) -> "ProcessPublisher":
        """Bind the PUB socket. Idempotent. Returns self for chaining."""
        if self._started:
            return self

        import gevent.lock
        import zmq.green as zmq_mod

        ctx = zmq_mod.Context()
        sock = None
        try:
            sock = ctx.socket(zmq_mod.PUB)
            sock.setsockopt(zmq_mod.LINGER, 0)
            sock.setsockopt(zmq_mod.SNDHWM, self._sndhwm)
            sock.bind(self._addr)
        except Exception:
            _safe_close(sock, ctx)
            raise

        self._ctx = ctx
        self._sock = sock
        self._send_lock = gevent.lock.Semaphore()
        self._started = True
        return self

    def publish(self, topic: str, payload: Any) -> None:
        """Publish ``payload`` under ``topic``. Non-blocking.

        Drops the message silently if the send queue is full (slow subscribers).
        Safe to call concurrently from multiple greenlets.
        """
        if not self._started:
            raise RuntimeError("ProcessPublisher.publish() called before start()")

        import zmq.green as zmq_mod

        data = self._serializer.dumps(payload)
        with self._send_lock:
            # Concurrent close() may have torn the socket down between our
            # _started check above and acquiring the lock; re-check.
            if not self._started:
                return
            try:
                self._sock.send_multipart(
                    [topic.encode("utf-8"), data], flags=zmq_mod.NOBLOCK
                )
            except zmq_mod.Again:
                # SNDHWM hit — slow subscribers. Drop, matching PUB semantics.
                log.debug("publisher dropped message for topic %s (HWM hit)", topic)
            except zmq_mod.ZMQError as exc:
                log.warning("publisher send failed for topic %s: %s", topic, exc)

    def close(self) -> None:
        """Tear down the socket. Idempotent."""
        if not self._started:
            return
        # Serialize with publish(): closing while a greenlet is mid-send is
        # undefined. publish() re-checks _started inside the same lock.
        with self._send_lock:
            self._started = False
            _safe_close(self._sock, self._ctx)
        if self._addr.startswith("ipc://"):
            with contextlib.suppress(OSError):
                os.unlink(self._addr[6:])


# ---------------------------------------------------------------------------
# Subscriber (asyncio side)
# ---------------------------------------------------------------------------


class ProcessSubscriber:
    """ZMQ SUB socket for asyncio consumers.

    Register topic-prefix handlers; a single reader task dispatches incoming
    messages. Multiple handlers may share a prefix and are invoked
    concurrently. An exception in one handler is logged but does not kill
    the reader.

    Args:
        address: IPC/TCP address (e.g., ``"ipc:///tmp/stream.sock"``).
        serializer: Optional serializer; defaults to :class:`SmartPickle`.

    Example::

        sub = ProcessSubscriber("ipc:///tmp/stream.sock")

        async def on_snapshot(topic, payload): ...

        sub.subscribe("v1.snapshot.", on_snapshot)
        sub.start()
        ...
        await sub.close()
    """

    def __init__(
        self,
        address: str,
        *,
        serializer: Serializer = SmartPickle,
    ):
        self._addr = address
        self._serializer = serializer
        self._started = False
        self._sock: Any = None
        self._ctx: Any = None
        self._reader_task: Any = None
        # Tasks currently running ProcessSubscriber._invoke. close() uses
        # this to detect "called from a handler my reader is awaiting" and
        # skip the reader-join (which would self-deadlock).
        self._handler_tasks: set[Any] = set()
        self._handlers: dict[str, list[Handler]] = {}

    def __del__(self):
        # Best-effort sync cleanup from finalizer. Reader task is leaked here;
        # users should ``await close()`` explicitly for deterministic cleanup.
        _safe_close(getattr(self, "_sock", None), getattr(self, "_ctx", None))

    async def __aenter__(self) -> "ProcessSubscriber":
        return self.start()

    async def __aexit__(self, *_) -> None:
        await self.close()

    @property
    def address(self) -> str:
        """IPC/TCP address."""
        return self._addr

    def start(self) -> "ProcessSubscriber":
        """Connect the SUB socket and spawn the reader task. Idempotent.

        Must be called with a running asyncio loop. The reader task is bound
        to that loop; subsequent :meth:`subscribe`, :meth:`unsubscribe`, and
        :meth:`close` calls must run on the same loop/thread (ZMQ sockets
        are not thread-safe).
        """
        if self._started:
            return self

        import zmq.asyncio

        # Require a running loop *before* allocating ZMQ resources, so a
        # caller misusing the API doesn't leave the subscriber half-built
        # (sock/ctx allocated, _started=True, no reader task).
        loop = asyncio.get_running_loop()

        # Fresh context per subscriber (mirrors ProcessBridge), so ``close()``
        # fully releases libzmq resources and restart is clean.
        ctx = zmq.asyncio.Context()
        sock = None
        try:
            sock = ctx.socket(zmq.SUB)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(self._addr)
            # Re-subscribe to any prefixes registered before start().
            for prefix in self._handlers:
                sock.setsockopt(zmq.SUBSCRIBE, prefix.encode("utf-8"))
        except Exception:
            _safe_close(sock, ctx)
            raise

        self._ctx = ctx
        self._sock = sock
        self._started = True
        # Bind the reader to *this* socket. A close()+start() restart from
        # inside a handler swaps ``self._sock``; the stale reader must not
        # resume against the new socket (would race the new reader's recv).
        self._reader_task = loop.create_task(self._read_loop(sock))
        return self

    def subscribe(self, topic_prefix: str, handler: Handler) -> None:
        """Register ``handler`` for messages whose topic starts with ``topic_prefix``.

        Multiple handlers may share a prefix; they are invoked concurrently.
        Safe to call before or after :meth:`start`.

        After :meth:`start`, call only from the same thread that owns the
        asyncio loop (ZMQ sockets are not thread-safe).
        """
        new_prefix = topic_prefix not in self._handlers
        self._handlers.setdefault(topic_prefix, []).append(handler)
        if new_prefix and self._started:
            import zmq

            self._sock.setsockopt(zmq.SUBSCRIBE, topic_prefix.encode("utf-8"))

    def unsubscribe(
        self, topic_prefix: str, handler: Handler | None = None
    ) -> None:
        """Remove ``handler`` (or all handlers) for ``topic_prefix``.

        When the last handler for a prefix is removed, the ZMQ-level
        subscription is also dropped. After :meth:`start`, call only from
        the same thread that owns the asyncio loop.
        """
        handlers = self._handlers.get(topic_prefix)
        if not handlers:
            return
        if handler is None:
            handlers.clear()
        else:
            with contextlib.suppress(ValueError):
                handlers.remove(handler)
        if not handlers:
            self._handlers.pop(topic_prefix, None)
            if self._started:
                import zmq

                with contextlib.suppress(Exception):
                    self._sock.setsockopt(
                        zmq.UNSUBSCRIBE, topic_prefix.encode("utf-8")
                    )

    def _match(self, topic: str) -> list[Handler]:
        return [
            h
            for prefix, handlers in self._handlers.items()
            if topic.startswith(prefix)
            for h in handlers
        ]

    async def _read_loop(self, sock: Any) -> None:
        """Single reader task: dispatch messages to matched handlers.

        ``sock`` is captured at task creation time so a close+restart cycle
        leaves stale readers pointing at the already-closed socket — their
        recv fails immediately and they exit without touching the new socket.
        """
        try:
            while True:
                # Check before each recv: close() or a close+start restart
                # may have swapped ``self._sock`` while we were suspended in
                # the previous gather() (pyzmq does not necessarily wake a
                # pending recv future when the socket is closed, so we can't
                # rely solely on recv raising).
                if not self._started or sock is not self._sock:
                    return
                try:
                    parts = await sock.recv_multipart()
                except Exception:
                    if not self._started or sock is not self._sock:
                        return
                    raise
                if len(parts) < 2:
                    continue
                topic_bytes, data, *_ = parts
                topic = topic_bytes.decode("utf-8", errors="replace")
                handlers = self._match(topic)
                if not handlers:
                    continue
                try:
                    payload = self._serializer.loads(data)
                except Exception:
                    log.exception(
                        "subscriber failed to deserialize topic %s", topic
                    )
                    continue
                # return_exceptions=True isolates the reader from a
                # handler's CancelledError or any BaseException leaking
                # past _invoke.
                results = await asyncio.gather(
                    *(self._invoke(h, topic, payload) for h in handlers),
                    return_exceptions=True,
                )
                for r in results:
                    if isinstance(r, (SystemExit, KeyboardInterrupt)):
                        # Honor process-exit intent from a handler.
                        raise r
                    # CancelledError is BaseException (not Exception) since
                    # 3.8, so ``isinstance(r, Exception)`` naturally skips it.
                    if isinstance(r, Exception):
                        log.error(
                            "subscriber handler raised %s for topic %s",
                            type(r).__name__,
                            topic,
                            exc_info=r,
                        )
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("subscriber reader task crashed")
            # Reader is dying for an unrelated reason (malformed frame,
            # libzmq error, etc.). Tear our resources down so the
            # subscriber doesn't look alive while silently dropping
            # messages, and so ``start()`` can rebuild from scratch.
            if sock is self._sock and self._started:
                self._started = False
                ctx = self._ctx
                self._sock = None
                self._ctx = None
                self._reader_task = None
                _safe_close(sock, ctx)

    async def _invoke(self, handler: Handler, topic: str, payload: Any) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._handler_tasks.add(task)
        try:
            await handler(topic, payload)
        except Exception:
            log.exception("subscriber handler failed for topic %s", topic)
        finally:
            if task is not None:
                self._handler_tasks.discard(task)

    async def close(self) -> None:
        """Tear down the socket and join the reader task. Idempotent.

        Must be awaited from the asyncio loop that owns this subscriber
        (the one that called :meth:`start`). Calling concurrently from
        multiple coroutines is safe; the second caller returns immediately.

        Safe to call from inside a handler: the reader task is not joined
        in that case (joining yourself would deadlock), and sibling handlers
        in the current dispatch are allowed to finish — we never
        ``task.cancel()`` the reader, so ``asyncio.gather`` is not torn down.
        """
        if not self._started:
            return
        # Snapshot owned resources into locals, then null on self so a
        # concurrent start() cannot have its fresh ctx/sock closed by us.
        self._started = False
        sock, ctx, task = self._sock, self._ctx, self._reader_task
        self._sock = None
        self._ctx = None
        self._reader_task = None

        # Close socket first. Any in-flight recv fails; the reader sees
        # _started=False and exits cleanly. Avoid task.cancel() — it would
        # propagate through asyncio.gather and cancel sibling handlers
        # mid-execution when close() is called from inside a handler.
        _safe_close(sock, ctx)

        # Join the reader for deterministic cleanup. Skip when the caller
        # IS the reader, or is a handler the reader is currently awaiting —
        # both deadlock. In those cases the reader exits on its own once
        # gather completes (closed socket + _started=False).
        current = asyncio.current_task()
        if (
            task is not None
            and task is not current
            and current not in self._handler_tasks
            and not task.done()
        ):
            with contextlib.suppress(BaseException):
                await asyncio.wait({task}, timeout=2.0)


__all__ = ["ProcessPublisher", "ProcessSubscriber"]
