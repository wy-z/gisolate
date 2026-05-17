"""ZMQ PUB/SUB one-way fan-out, gevent + asyncio on either side.

Use ``ProcessBridge`` when you need request/response RPC.
Use these when you need one-way fan-out (snapshots, signals, heartbeats) where
message loss is acceptable — PUB drops messages for slow subscribers.

:class:`ProcessPublisher` and :class:`ProcessSubscriber` each take a
``runtime`` kwarg (:attr:`Runtime.GEVENT` / :attr:`Runtime.ASYNC`) that
selects the concurrency backend. The wire format is identical, so any
publisher pairs with any subscriber regardless of either side's runtime.

In GEVENT mode, ``publish`` / ``close`` are synchronous; in ASYNC mode they
return awaitables.
"""

import asyncio
import contextlib
import enum
import inspect
import logging
import os
from typing import Any, Awaitable, Callable

from ._internal import Serializer, SmartPickle

log = logging.getLogger(__name__)

Handler = Callable[[str, Any], Awaitable[None] | None]


class Runtime(enum.StrEnum):
    """Concurrency backend a publisher / subscriber binds to."""

    GEVENT = "gevent"
    ASYNC = "asyncio"


def _safe_close(sock: Any, ctx: Any) -> None:
    """Best-effort tear down of a ZMQ socket + context. Swallows errors."""
    with contextlib.suppress(Exception):
        if sock is not None:
            sock.close(linger=0)
    with contextlib.suppress(Exception):
        if ctx is not None:
            ctx.term()


# ---------------------------------------------------------------------------
# Publisher
# ---------------------------------------------------------------------------


class ProcessPublisher:
    """ZMQ PUB socket for one-way fan-out.

    Topic-based dispatch with a pluggable serializer (default
    :class:`SmartPickle`). ``publish`` is non-blocking — slow subscribers
    cause messages to be dropped once the high-water mark is hit, matching
    standard PUB semantics.

    Args:
        address: IPC/TCP address (e.g., ``"ipc:///tmp/stream.sock"``).
        runtime: :attr:`Runtime.GEVENT` (default) or :attr:`Runtime.ASYNC`.
            Selects the concurrency backend; ``publish`` and ``close`` are
            sync in GEVENT mode, awaitable in ASYNC mode.
        serializer: Optional serializer; defaults to :class:`SmartPickle`.
        sndhwm: Send high-water mark. Beyond this, messages are dropped.

    Example::

        # gevent
        pub = ProcessPublisher(addr).start()
        pub.publish("v1.snapshot.AAPL", {"price": 150.0})
        pub.close()

        # asyncio
        pub = ProcessPublisher(addr, runtime=Runtime.ASYNC).start()
        await pub.publish("v1.snapshot.AAPL", {"price": 150.0})
        await pub.close()
    """

    def __init__(
        self,
        address: str,
        *,
        runtime: Runtime | str = Runtime.GEVENT,
        serializer: Serializer = SmartPickle,
        sndhwm: int = 1000,
    ):
        self._addr = address
        # Normalize so a stray ``"gevent"`` / ``"asyncio"`` string doesn't
        # silently fall through to the else-branch in mode dispatch.
        self._runtime = Runtime(runtime)
        self._serializer = serializer
        self._sndhwm = sndhwm
        self._started = False
        self._sock: Any = None
        self._ctx: Any = None
        self._send_lock: Any = None

    def __del__(self):
        # Best-effort sync cleanup. Avoid full close() — at GC time the
        # loop/hub may be torn down, so acquiring the send-lock can fail.
        # The ipc file is left on disk (process is exiting anyway).
        _safe_close(getattr(self, "_sock", None), getattr(self, "_ctx", None))

    def __enter__(self) -> "ProcessPublisher":
        if self._runtime is not Runtime.GEVENT:
            raise RuntimeError("Use `async with` for ASYNC runtime")
        return self.start()

    def __exit__(self, *_) -> None:
        self.close()

    async def __aenter__(self) -> "ProcessPublisher":
        if self._runtime is not Runtime.ASYNC:
            raise RuntimeError("Use `with` for GEVENT runtime")
        return self.start()

    async def __aexit__(self, *_) -> None:
        await self.close()

    @property
    def address(self) -> str:
        """IPC/TCP address."""
        return self._addr

    @property
    def runtime(self) -> Runtime:
        """Concurrency backend selected at construction time."""
        return self._runtime

    def start(self) -> "ProcessPublisher":
        """Bind the PUB socket. Idempotent. Returns self for chaining.

        In ASYNC mode, must be called with a running asyncio loop.
        """
        if self._started:
            return self
        if self._runtime is Runtime.GEVENT:
            self._bind_gevent()
        else:
            self._bind_async()
        self._started = True
        return self

    def _bind_gevent(self) -> None:
        import gevent.lock
        import zmq.green

        ctx = zmq.green.Context()
        self._ctx, self._sock = ctx, self._bind_pub(ctx)
        self._send_lock = gevent.lock.Semaphore()

    def _bind_async(self) -> None:
        import zmq.asyncio

        # Require a running loop *before* allocating ZMQ resources, so a
        # caller misusing the API doesn't leave the publisher half-built.
        asyncio.get_running_loop()

        ctx = zmq.asyncio.Context()
        self._ctx, self._sock = ctx, self._bind_pub(ctx)
        self._send_lock = asyncio.Lock()

    def _bind_pub(self, ctx: Any) -> Any:
        """Create + configure + bind a PUB socket on ``ctx``. Closes on failure."""
        import zmq

        sock = None
        try:
            sock = ctx.socket(zmq.PUB)
            sock.setsockopt(zmq.LINGER, 0)
            sock.setsockopt(zmq.SNDHWM, self._sndhwm)
            sock.bind(self._addr)
        except Exception:
            _safe_close(sock, ctx)
            raise
        return sock

    def publish(self, topic: str, payload: Any) -> Any:
        """Publish ``payload`` under ``topic``. Non-blocking.

        Drops the message silently if the send queue is full (slow subscribers).
        Safe to call concurrently from multiple greenlets/coroutines.

        Returns ``None`` in GEVENT mode; returns a coroutine in ASYNC mode —
        the caller must ``await`` it.
        """
        if not self._started:
            raise RuntimeError("ProcessPublisher.publish() called before start()")
        if self._runtime is Runtime.GEVENT:
            self._publish_gevent(topic, payload)
            return None
        return self._publish_async(topic, payload)

    def _publish_gevent(self, topic: str, payload: Any) -> None:
        import zmq

        data = self._serializer.dumps(payload)
        with self._send_lock:
            # Concurrent close() may have torn the socket down between our
            # _started check above and acquiring the lock; re-check.
            if not self._started:
                return
            try:
                self._sock.send_multipart(
                    [topic.encode("utf-8"), data], flags=zmq.NOBLOCK
                )
            except zmq.Again:
                # SNDHWM hit — slow subscribers. Drop, matching PUB semantics.
                log.debug("publisher dropped message for topic %s (HWM hit)", topic)
            except zmq.ZMQError as exc:
                log.warning("publisher send failed for topic %s: %s", topic, exc)

    async def _publish_async(self, topic: str, payload: Any) -> None:
        import zmq

        data = self._serializer.dumps(payload)
        async with self._send_lock:
            if not self._started:
                return
            try:
                await self._sock.send_multipart(
                    [topic.encode("utf-8"), data], flags=zmq.NOBLOCK
                )
            except zmq.Again:
                log.debug("publisher dropped message for topic %s (HWM hit)", topic)
            except zmq.ZMQError as exc:
                log.warning("publisher send failed for topic %s: %s", topic, exc)

    def close(self) -> Any:
        """Tear down the socket. Idempotent.

        Returns ``None`` in GEVENT mode; returns a coroutine in ASYNC mode —
        the caller must ``await`` it.
        """
        if self._runtime is Runtime.GEVENT:
            self._close_gevent()
            return None
        return self._close_async()

    def _close_gevent(self) -> None:
        if not self._started:
            return
        # Serialize with publish(): closing while a greenlet is mid-send is
        # undefined. publish() re-checks _started inside the same lock.
        with self._send_lock:
            self._started = False
            _safe_close(self._sock, self._ctx)
        self._unlink_ipc()

    async def _close_async(self) -> None:
        if not self._started:
            return
        async with self._send_lock:
            self._started = False
            _safe_close(self._sock, self._ctx)
        self._unlink_ipc()

    def _unlink_ipc(self) -> None:
        if self._addr.startswith("ipc://"):
            with contextlib.suppress(OSError):
                os.unlink(self._addr[6:])


# ---------------------------------------------------------------------------
# Subscriber
# ---------------------------------------------------------------------------


class ProcessSubscriber:
    """ZMQ SUB socket. Register topic-prefix handlers; a single reader
    dispatches incoming messages.

    Multiple handlers may share a prefix and are invoked concurrently. An
    exception in one handler is logged but does not kill the reader.

    Args:
        address: IPC/TCP address (e.g., ``"ipc:///tmp/stream.sock"``).
        runtime: :attr:`Runtime.GEVENT` or :attr:`Runtime.ASYNC` (default).
            In GEVENT mode handlers are sync callables; in ASYNC mode they
            are ``async def`` and ``close`` is awaitable.
        serializer: Optional serializer; defaults to :class:`SmartPickle`.

    Example::

        # asyncio
        sub = ProcessSubscriber(addr)
        async def on_snapshot(topic, payload): ...
        sub.subscribe("v1.snapshot.", on_snapshot)
        sub.start()
        await sub.close()

        # gevent
        sub = ProcessSubscriber(addr, runtime=Runtime.GEVENT)
        def on_snapshot(topic, payload): ...
        sub.subscribe("v1.snapshot.", on_snapshot)
        sub.start()
        sub.close()
    """

    def __init__(
        self,
        address: str,
        *,
        runtime: Runtime | str = Runtime.ASYNC,
        serializer: Serializer = SmartPickle,
    ):
        self._addr = address
        # Normalize so a stray ``"gevent"`` / ``"asyncio"`` string doesn't
        # silently fall through to the else-branch in mode dispatch.
        self._runtime = Runtime(runtime)
        self._serializer = serializer
        self._started = False
        self._sock: Any = None
        self._ctx: Any = None
        self._reader: Any = None  # asyncio.Task in ASYNC, Greenlet in GEVENT
        # Tasks/greenlets currently running ._invoke. close() consults this
        # to detect "called from a handler my reader is awaiting" and skip
        # the reader-join (which would self-deadlock).
        self._handler_workers: set[Any] = set()
        self._handlers: dict[str, list[Handler]] = {}

    def __del__(self):
        # Best-effort sync cleanup from finalizer. Reader is leaked here;
        # users should call close() explicitly for deterministic cleanup.
        _safe_close(getattr(self, "_sock", None), getattr(self, "_ctx", None))

    def __enter__(self) -> "ProcessSubscriber":
        if self._runtime is not Runtime.GEVENT:
            raise RuntimeError("Use `async with` for ASYNC runtime")
        return self.start()

    def __exit__(self, *_) -> None:
        self.close()

    async def __aenter__(self) -> "ProcessSubscriber":
        if self._runtime is not Runtime.ASYNC:
            raise RuntimeError("Use `with` for GEVENT runtime")
        return self.start()

    async def __aexit__(self, *_) -> None:
        await self.close()

    @property
    def address(self) -> str:
        """IPC/TCP address."""
        return self._addr

    @property
    def runtime(self) -> Runtime:
        """Concurrency backend selected at construction time."""
        return self._runtime

    def start(self) -> "ProcessSubscriber":
        """Connect the SUB socket and spawn the reader. Idempotent.

        In ASYNC mode must be called with a running asyncio loop; in GEVENT
        mode must be called from a greenlet context. Subsequent
        :meth:`subscribe`, :meth:`unsubscribe`, :meth:`close` calls must run
        on that same loop/hub (ZMQ sockets are not thread-safe).
        """
        if self._started:
            return self
        if self._runtime is Runtime.GEVENT:
            self._start_gevent()
        else:
            self._start_async()
        return self

    def _start_async(self) -> None:
        import zmq.asyncio

        loop = asyncio.get_running_loop()
        ctx = zmq.asyncio.Context()
        sock = self._connect_sub(ctx)
        self._ctx = ctx
        self._sock = sock
        self._started = True
        # Bind the reader to *this* socket. A close()+start() restart from
        # inside a handler swaps ``self._sock``; the stale reader must not
        # resume against the new socket (would race the new reader's recv).
        self._reader = loop.create_task(self._read_loop_async(sock))

    def _start_gevent(self) -> None:
        import gevent
        import zmq.green

        ctx = zmq.green.Context()
        sock = self._connect_sub(ctx)
        self._ctx = ctx
        self._sock = sock
        self._started = True
        self._reader = gevent.spawn(self._read_loop_gevent, sock)

    def _connect_sub(self, ctx: Any) -> Any:
        """Create + configure + connect a SUB socket on ``ctx``. Closes on failure."""
        import zmq

        sock = None
        try:
            sock = ctx.socket(zmq.SUB)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(self._addr)
            for prefix in self._handlers:
                sock.setsockopt(zmq.SUBSCRIBE, prefix.encode("utf-8"))
        except Exception:
            _safe_close(sock, ctx)
            raise
        return sock

    def subscribe(self, topic_prefix: str, handler: Handler) -> None:
        """Register ``handler`` for topics starting with ``topic_prefix``.

        Multiple handlers may share a prefix; they are invoked concurrently.
        Safe to call before or after :meth:`start`. Handler must be sync in
        GEVENT mode and ``async def`` in ASYNC mode.
        """
        import zmq

        new_prefix = topic_prefix not in self._handlers
        self._handlers.setdefault(topic_prefix, []).append(handler)
        if new_prefix and self._started:
            self._sock.setsockopt(zmq.SUBSCRIBE, topic_prefix.encode("utf-8"))

    def unsubscribe(
        self, topic_prefix: str, handler: Handler | None = None
    ) -> None:
        """Remove ``handler`` (or all handlers) for ``topic_prefix``.

        When the last handler for a prefix is removed, the ZMQ-level
        subscription is also dropped.
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

    def _decode(self, parts: list[bytes]) -> tuple[str, Any] | None:
        """Decode a multipart frame. Returns (topic, payload) or None on error."""
        if len(parts) < 2:
            return None
        topic_bytes, data, *_ = parts
        topic = topic_bytes.decode("utf-8", errors="replace")
        try:
            payload = self._serializer.loads(data)
        except Exception:
            log.exception("subscriber failed to deserialize topic %s", topic)
            return None
        return topic, payload

    # ----- reader loops --------------------------------------------------

    async def _read_loop_async(self, sock: Any) -> None:
        """Asyncio reader: dispatch messages to matched handlers.

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
                decoded = self._decode(parts)
                if decoded is None:
                    continue
                topic, payload = decoded
                handlers = self._match(topic)
                if not handlers:
                    continue
                # return_exceptions=True isolates the reader from a
                # handler's CancelledError or any BaseException leaking
                # past _invoke_async.
                results = await asyncio.gather(
                    *(self._invoke_async(h, topic, payload) for h in handlers),
                    return_exceptions=True,
                )
                for r in results:
                    if isinstance(r, (SystemExit, KeyboardInterrupt)):
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
            self._self_destruct(sock)

    def _read_loop_gevent(self, sock: Any) -> None:
        """Gevent reader: spawn a greenlet per handler invocation.

        Uses a 100ms poll so close() is observed promptly without depending
        on close-during-recv waking the greenlet.
        """
        import gevent

        try:
            while True:
                if not self._started or sock is not self._sock:
                    return
                try:
                    if not sock.poll(100):
                        continue
                    parts = sock.recv_multipart()
                except gevent.GreenletExit:
                    return
                except Exception:
                    # Silence only when caused by close()/stale-socket;
                    # otherwise fall through to ``_self_destruct`` so the
                    # subscriber doesn't look alive with a dead reader.
                    if not self._started or sock is not self._sock:
                        return
                    raise
                decoded = self._decode(parts)
                if decoded is None:
                    continue
                topic, payload = decoded
                handlers = self._match(topic)
                if not handlers:
                    continue
                # Spawn-and-forget: matches asyncio.gather(...) semantics of
                # isolating handlers from the reader and from each other.
                # Errors are logged in _invoke_gevent.
                for h in handlers:
                    gevent.spawn(self._invoke_gevent, h, topic, payload)
        except gevent.GreenletExit:
            pass
        except Exception:
            log.exception("subscriber reader greenlet crashed")
            self._self_destruct(sock)

    def _self_destruct(self, sock: Any) -> None:
        """Reader is dying for an unrelated reason — tear our resources down so
        the subscriber doesn't look alive while silently dropping messages.
        The ``sock is self._sock`` guard skips stale readers left over from a
        close+restart cycle (we'd otherwise close the *new* subscriber's
        resources)."""
        if sock is not self._sock or not self._started:
            return
        self._started = False
        sock, ctx, _ = self._snapshot_owned()
        _safe_close(sock, ctx)

    # ----- handler invocation -------------------------------------------

    async def _invoke_async(self, handler: Handler, topic: str, payload: Any) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._handler_workers.add(task)
        try:
            result = handler(topic, payload)
            # ``Handler`` type allows ``Awaitable[None]``; use isawaitable so
            # custom awaitables (not just coroutines) are supported.
            if inspect.isawaitable(result):
                await result
        except Exception:
            log.exception("subscriber handler failed for topic %s", topic)
        finally:
            if task is not None:
                self._handler_workers.discard(task)

    def _invoke_gevent(self, handler: Handler, topic: str, payload: Any) -> None:
        import gevent

        g = gevent.getcurrent()
        self._handler_workers.add(g)
        try:
            result = handler(topic, payload)
            # GEVENT mode has no event loop to drive the awaitable. Silent
            # no-op would mask a real bug; log loudly and dispose any
            # coroutine to suppress "never awaited" runtime warnings.
            if inspect.isawaitable(result):
                log.error(
                    "subscriber handler for topic %s returned an awaitable in "
                    "GEVENT mode (must be sync); discarded",
                    topic,
                )
                if inspect.iscoroutine(result):
                    with contextlib.suppress(Exception):
                        result.close()
        except Exception:
            log.exception("subscriber handler failed for topic %s", topic)
        finally:
            self._handler_workers.discard(g)

    # ----- shutdown ------------------------------------------------------

    def close(self) -> Any:
        """Tear down the socket and join the reader. Idempotent.

        Returns ``None`` in GEVENT mode; returns a coroutine in ASYNC mode —
        the caller must ``await`` it.

        Safe to call from inside a handler: the reader is not joined in that
        case (joining yourself would deadlock); sibling handlers in the
        current dispatch are allowed to finish — we never cancel the reader,
        so the dispatch gather is not torn down.
        """
        if self._runtime is Runtime.GEVENT:
            self._close_gevent()
            return None
        return self._close_async()

    def _snapshot_owned(self) -> tuple[Any, Any, Any]:
        """Move owned sock/ctx/reader off ``self`` so a concurrent start()
        can't have its fresh resources closed by us."""
        sock, ctx, reader = self._sock, self._ctx, self._reader
        self._sock = None
        self._ctx = None
        self._reader = None
        return sock, ctx, reader

    async def _close_async(self) -> None:
        if not self._started:
            return
        self._started = False
        sock, ctx, reader = self._snapshot_owned()
        # Close socket first. Any in-flight recv fails; the reader sees
        # _started=False and exits cleanly. Avoid task.cancel() — it would
        # propagate through asyncio.gather and cancel sibling handlers
        # mid-execution when close() is called from inside a handler.
        _safe_close(sock, ctx)
        current = asyncio.current_task()
        if (
            reader is not None
            and reader is not current
            and current not in self._handler_workers
            and not reader.done()
        ):
            with contextlib.suppress(BaseException):
                await asyncio.wait({reader}, timeout=2.0)

    def _close_gevent(self) -> None:
        import gevent

        if not self._started:
            return
        self._started = False
        sock, ctx, reader = self._snapshot_owned()
        _safe_close(sock, ctx)
        current = gevent.getcurrent()
        if (
            reader is not None
            and reader is not current
            and current not in self._handler_workers
            and not reader.dead
        ):
            with contextlib.suppress(BaseException):
                reader.join(timeout=2.0)


__all__ = ["ProcessPublisher", "ProcessSubscriber", "Runtime"]
