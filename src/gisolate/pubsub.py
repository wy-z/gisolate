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

import abc
import asyncio
import contextlib
import enum
import inspect
import logging
from typing import Any, Awaitable, Callable, Self

from . import _internal

log = logging.getLogger(__name__)

Handler = Callable[[str, Any], Awaitable[None] | None]


class Runtime(enum.StrEnum):
    """Concurrency backend a publisher / subscriber binds to."""

    GEVENT = "gevent"
    ASYNC = "asyncio"


class _PubSubBase(abc.ABC):
    # _start_* implementations own setting ``self._started = True`` once
    # their resources are live. start() must NOT set it afterwards: with an
    # eager task factory a handler may run close() *inside* _start_async,
    # and a trailing flag-set here would resurrect the closed instance.
    @abc.abstractmethod
    def _start_gevent(self) -> None: ...

    @abc.abstractmethod
    def _start_async(self) -> None: ...

    @abc.abstractmethod
    def _close_gevent(self) -> None: ...

    @abc.abstractmethod
    async def _close_async(self) -> None: ...

    def __init__(
        self,
        address: str,
        runtime: Runtime | str,
        serializer: _internal.Serializer,
    ):
        self._addr = address
        # Normalize so a stray ``"gevent"`` / ``"asyncio"`` string doesn't
        # silently fall through to the else-branch in mode dispatch.
        self._runtime = Runtime(runtime)
        self._serializer = serializer
        self._started = False
        self._transport: _internal.ZmqTransport | None = None

    def __del__(self):
        # Best-effort sync cleanup from finalizer. Avoid full close(): at GC
        # time the loop/hub may be torn down (locks can fail, readers leak).
        # Call close() explicitly for deterministic cleanup.
        if (transport := getattr(self, "_transport", None)) is not None:
            transport.close()

    def __enter__(self) -> Self:
        if self._runtime is not Runtime.GEVENT:
            raise RuntimeError("Use `async with` for ASYNC runtime")
        return self.start()

    def __exit__(self, *_) -> None:
        self.close()

    async def __aenter__(self) -> Self:
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

    def start(self) -> Self:
        """Open the socket and start backend resources. Idempotent.

        Returns self for chaining. In ASYNC mode must be called with a
        running asyncio loop; in GEVENT mode from a greenlet context.
        Subsequent calls on the instance must run on that same loop/hub
        (ZMQ sockets are not thread-safe).
        """
        if self._started:
            return self
        if self._runtime is Runtime.GEVENT:
            self._start_gevent()
        else:
            self._start_async()
        return self

    def close(self) -> Any:
        """Tear down the socket. Idempotent.

        Returns ``None`` in GEVENT mode; returns a coroutine in ASYNC mode —
        the caller must ``await`` it.
        """
        if self._runtime is Runtime.GEVENT:
            self._close_gevent()
            return None
        return self._close_async()


# ---------------------------------------------------------------------------
# Publisher
# ---------------------------------------------------------------------------


class ProcessPublisher(_PubSubBase):
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
        serializer: _internal.Serializer = _internal.SmartPickle,
        sndhwm: int = 1000,
    ):
        super().__init__(address, runtime, serializer)
        self._sndhwm = sndhwm
        self._send_lock: Any = None

    def _start_gevent(self) -> None:
        import gevent.lock
        import zmq.green

        # Before the bind, like the running-loop check in the async start: the
        # bind is the step that creates something to release, and anything
        # failing after it — a MemoryError here is the realistic one — leaves a
        # bound socket that only the traceback can reach, since _transport is
        # still None for close() and __del__.
        self._send_lock = gevent.lock.Semaphore()
        self._transport = self._bind_pub(zmq.green.Context)
        self._started = True

    def _start_async(self) -> None:
        import zmq.asyncio

        # Require a running loop *before* allocating ZMQ resources, so a
        # caller misusing the API doesn't leave the publisher half-built. The
        # lock is here for the same reason — see the gevent start.
        asyncio.get_running_loop()
        self._send_lock = asyncio.Lock()

        self._transport = self._bind_pub(zmq.asyncio.Context)
        self._started = True

    def _bind_pub(self, context_factory: Any) -> _internal.ZmqTransport:
        import zmq

        return _internal.ZmqTransport.open(
            context_factory,
            zmq.PUB,
            self._addr,
            bind=True,
            options=[(zmq.SNDHWM, self._sndhwm)],
        )

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
        # Bound to THIS generation, like the bridge's call(): waiting on the
        # lock is where a close+start lands, and re-reading ``self`` after it
        # sent on the NEW generation's socket while holding the OLD lock —
        # beside a publish legitimately holding the new one, interleaving
        # their multipart frames. A publish accepted before the close is that
        # generation's message; it drops with it.
        transport, lock = self._transport, self._send_lock
        if transport is None:
            return
        with lock:
            # Concurrent close() may have torn the socket down between our
            # _started check above and acquiring the lock; re-check — against
            # the generation we captured, not whatever replaced it.
            if not self._started or transport is not self._transport:
                return
            try:
                transport.sock.send_multipart(
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
        # Generation-bound — see the gevent path.
        transport, lock = self._transport, self._send_lock
        if transport is None:
            return
        async with lock:
            if not self._started or transport is not self._transport:
                return
            try:
                await transport.sock.send_multipart(
                    [topic.encode("utf-8"), data], flags=zmq.NOBLOCK
                )
            except zmq.Again:
                log.debug("publisher dropped message for topic %s (HWM hit)", topic)
            except zmq.ZMQError as exc:
                log.warning("publisher send failed for topic %s: %s", topic, exc)

    def _close_gevent(self) -> None:
        if not self._started:
            return
        # Serialize with publish(): closing while a greenlet is mid-send is
        # undefined. publish() re-checks _started inside the same lock.
        with self._send_lock:
            self._started = False
            transport, self._transport = self._transport, None
        if transport is not None:
            transport.close()

    async def _close_async(self) -> None:
        if not self._started:
            return
        async with self._send_lock:
            self._started = False
            transport, self._transport = self._transport, None
        if transport is not None:
            transport.close()


# ---------------------------------------------------------------------------
# Subscriber
# ---------------------------------------------------------------------------


class ProcessSubscriber(_PubSubBase):
    """ZMQ SUB socket. Register topic-prefix handlers; a single reader
    dispatches incoming messages.

    Multiple handlers may share a prefix and are invoked concurrently. An
    exception in one handler is logged but does not kill the reader.

    ``close`` is safe to call from inside a handler: the reader is not
    joined in that case (joining yourself would deadlock); sibling handlers
    in the current dispatch are allowed to finish — the reader is never
    cancelled, so the dispatch is not torn down. That guarantee is why close
    never kills a handler, so one that does not return outlives it, holding a
    greenlet or a task until the process ends. Only the batch in flight can be
    outstanding, though: the reader waits for each dispatch before receiving
    the next.

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
        serializer: _internal.Serializer = _internal.SmartPickle,
    ):
        super().__init__(address, runtime, serializer)
        self._reader: Any = None  # asyncio.Task in ASYNC, Greenlet in GEVENT
        # Tasks/greenlets currently running ._invoke. close() consults this
        # to detect "called from a handler my reader is awaiting" and skip
        # the reader-join (which would self-deadlock).
        self._handler_workers: set[Any] = set()
        self._handlers: dict[str, list[Handler]] = {}

    def _start_async(self) -> None:
        import zmq.asyncio

        loop = asyncio.get_running_loop()
        transport = self._connect_sub(zmq.asyncio.Context)
        self._transport = transport
        # Set before spawning the reader: an eager task factory (3.12+,
        # asyncio.eager_task_factory) runs the task synchronously inside
        # create_task, so the reader's gate must already see started=True.
        self._started = True
        try:
            # Bind the reader to *this* transport. A close()+start() restart
            # from inside a handler swaps ``self._transport``; the stale reader
            # must not resume against the new one (would race its recv).
            self._reader = loop.create_task(self._read_loop_async(transport))
        except BaseException:
            # An installed task factory can refuse. Nothing may stay claimed:
            # start() would no-op on the retry, over a live transport with no
            # reader behind it.
            self._started = False
            self._transport = None
            transport.close()
            raise

    def _start_gevent(self) -> None:
        import gevent
        import zmq.green

        transport = self._connect_sub(zmq.green.Context)
        self._transport = transport
        self._started = True  # before spawn, mirroring _start_async
        try:
            self._reader = gevent.spawn(self._read_loop_gevent, transport)
        except BaseException:
            # Same rollback as _start_async's, and for the same reason: a
            # refused spawn would otherwise leave start() to no-op on the retry
            # over a live transport with no reader behind it.
            self._started = False
            self._transport = None
            transport.close()
            raise

    def _connect_sub(self, context_factory: Any) -> _internal.ZmqTransport:
        import zmq

        return _internal.ZmqTransport.open(
            context_factory,
            zmq.SUB,
            self._addr,
            bind=False,
            options=[
                (zmq.SUBSCRIBE, prefix.encode("utf-8")) for prefix in self._handlers
            ],
        )

    def subscribe(self, topic_prefix: str, handler: Handler) -> None:
        """Register ``handler`` for topics starting with ``topic_prefix``.

        Multiple handlers may share a prefix; they are invoked concurrently.
        Safe to call before or after :meth:`start`. Handler must be sync in
        GEVENT mode and ``async def`` in ASYNC mode.
        """
        import zmq

        new_prefix = topic_prefix not in self._handlers
        self._handlers.setdefault(topic_prefix, []).append(handler)
        if new_prefix and self._started and self._transport is not None:
            self._transport.sock.setsockopt(
                zmq.SUBSCRIBE, topic_prefix.encode("utf-8")
            )

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
            if self._started and self._transport is not None:
                import zmq

                with contextlib.suppress(Exception):
                    self._transport.sock.setsockopt(
                        zmq.UNSUBSCRIBE, topic_prefix.encode("utf-8")
                    )

    def _decode_and_match(
        self, parts: list[bytes]
    ) -> tuple[str, Any, list[Handler]] | None:
        """Decode a frame and collect matching handlers.

        Returns ``(topic, payload, handlers)``, or ``None`` when the frame
        is malformed, undecodable, or matches no handler. Matching happens
        before payload deserialization so unmatched topics cost nothing.
        """
        if len(parts) < 2:
            return None
        topic = parts[0].decode("utf-8", errors="replace")
        handlers = [
            h
            for prefix, hs in self._handlers.items()
            if topic.startswith(prefix)
            for h in hs
        ]
        if not handlers:
            return None
        try:
            payload = self._serializer.loads(parts[1])
        except KeyboardInterrupt:
            # The operator's, arriving while a legitimate payload is being
            # reconstructed. Logging it as a bad payload absorbs Ctrl-C.
            raise
        # BaseException otherwise: reconstruction runs the publisher's code, and
        # one payload raising SystemExit would take the reader down — silently,
        # in gevent, leaving a subscriber that still calls itself started.
        except BaseException:
            log.exception("subscriber failed to deserialize topic %s", topic)
            return None
        return topic, payload, handlers

    # ----- reader loops --------------------------------------------------

    async def _read_loop_async(self, transport: _internal.ZmqTransport) -> None:
        """Asyncio reader: dispatch messages to matched handlers.

        ``transport`` is captured at task creation time so a close+restart
        cycle leaves stale readers pointing at the already-closed one — their
        recv fails immediately and they exit without touching the new one.
        """
        sock = transport.sock
        try:
            while True:
                # Check before each recv: close() or a close+start restart
                # may have swapped ``self._transport`` while we were suspended
                # in the previous gather() (pyzmq does not necessarily wake a
                # pending recv future when the socket is closed, so we can't
                # rely solely on recv raising).
                if not self._started or transport is not self._transport:
                    return
                try:
                    parts = await sock.recv_multipart()
                except Exception:
                    if not self._started or transport is not self._transport:
                        return
                    raise
                matched = self._decode_and_match(parts)
                if matched is None:
                    continue
                topic, payload, handlers = matched
                # return_exceptions=True isolates the reader from a handler's
                # CancelledError or any BaseException leaking past
                # _invoke_async (plain Exceptions are caught + logged there).
                results = await asyncio.gather(
                    *(self._invoke_async(h, topic, payload) for h in handlers),
                    return_exceptions=True,
                )
                for r in results:
                    # The operator's interrupt is the one thing a handler may
                    # not absorb on the host's behalf.
                    if isinstance(r, KeyboardInterrupt):
                        raise r
        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            # Passed on, but not silently: a host that catches it and carries on
            # would otherwise hold a subscriber that still calls itself started,
            # over a dead reader and a live socket — and start() would no-op.
            self._self_destruct(transport)
            raise
        except Exception:
            log.exception("subscriber reader task crashed")
            self._self_destruct(transport)

    def _read_loop_gevent(self, transport: _internal.ZmqTransport) -> None:
        """Gevent reader: spawn a greenlet per handler invocation.

        Uses a 100ms poll so close() is observed promptly without depending
        on close-during-recv waking the greenlet.
        """
        import gevent

        sock = transport.sock
        try:
            while True:
                if not self._started or transport is not self._transport:
                    return
                try:
                    if not sock.poll(100):
                        continue
                    parts = sock.recv_multipart()
                except gevent.GreenletExit:
                    return
                except Exception:
                    # Silence only when caused by close()/stale-transport;
                    # otherwise fall through to ``_self_destruct`` so the
                    # subscriber doesn't look alive with a dead reader.
                    if not self._started or transport is not self._transport:
                        return
                    raise
                matched = self._decode_and_match(parts)
                if matched is None:
                    continue
                topic, payload, handlers = matched
                # Spawned for concurrency, then waited for — the same shape as
                # the asyncio reader's gather, and for a reason spawn-and-forget
                # lost: a handler slower than the stream is what SUB's receive
                # queue and PUB's high-water mark exist to answer, by dropping
                # messages at the publisher. Draining ahead of the handlers
                # defeats both, and turns a slow handler into unbounded live
                # greenlets, each holding its payload. Errors are logged in
                # _invoke_gevent, so joinall cannot raise.
                gevent.joinall(
                    [gevent.spawn(self._invoke_gevent, h, topic, payload)
                     for h in handlers]
                )
        except gevent.GreenletExit:
            pass
        except KeyboardInterrupt:
            self._self_destruct(transport)  # see the asyncio reader
            raise
        except Exception:
            log.exception("subscriber reader greenlet crashed")
            self._self_destruct(transport)

    def _self_destruct(self, transport: _internal.ZmqTransport) -> None:
        """Reader is dying for an unrelated reason — tear our resources down so
        the subscriber doesn't look alive while silently dropping messages.
        The ``transport is self._transport`` guard skips stale readers left over
        from a close+restart cycle (we'd otherwise close the *new* subscriber's
        resources)."""
        if transport is not self._transport or not self._started:
            return
        # Snapshot first: it allocates its pair, and clearing the flag before
        # a refused allocation left the transport attached behind
        # _started=False — every later close returned without releasing it.
        owned, _ = self._snapshot_owned()
        self._started = False
        if owned is not None:
            owned.close()

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
        except asyncio.CancelledError:
            # Ours only. A cancellation aimed at this task is the loop shutting
            # down; anything else is the handler's own leak, and isolating it is
            # the whole point of running handlers separately.
            if task is None or task.cancelling():
                raise
            log.exception("subscriber handler failed for topic %s", topic)
        except KeyboardInterrupt:
            raise  # the operator's — see _invoke_gevent
        except BaseException:
            # BaseException, not Exception: SystemExit raised inside a task is
            # re-raised into the event loop by Task.__step and unwinds
            # asyncio.run, so a handler dependency calling sys.exit(2) would end
            # the whole subscriber host — the opposite of what "an exception in
            # one handler is logged but does not kill the reader" promises.
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
        except gevent.GreenletExit:
            raise
        except KeyboardInterrupt:
            # The operator's, not the handler's. Measured: a real SIGINT is
            # raised in whatever greenlet is running on the main OS thread, so
            # it lands HERE rather than in the main greenlet, and catching it as
            # a client failure is how Ctrl-C stops working.
            raise
        except BaseException:
            # BaseException, for the reason the asyncio side gives: gevent
            # forwards a greenlet's SystemExit or KeyboardInterrupt to the main
            # greenlet, which ends the process — so a handler dependency's
            # sys.exit(2) took the subscriber host with it.
            log.exception("subscriber handler failed for topic %s", topic)
        finally:
            self._handler_workers.discard(g)

    # ----- shutdown ------------------------------------------------------

    def _snapshot_owned(self) -> tuple[_internal.ZmqTransport | None, Any]:
        """Move the owned transport and reader off ``self`` so a concurrent
        start() can't have its fresh resources closed by us."""
        # The pair is built BEFORE the stores: it is the one allocation here,
        # and building it after detaching meant its MemoryError stranded a
        # transport neither close() nor __del__ could reach any more.
        snapshot = (self._transport, self._reader)
        self._transport = None
        self._reader = None
        return snapshot

    async def _close_async(self) -> None:
        if not self._started:
            return
        # Snapshot before the flag — see _self_destruct.
        transport, reader = self._snapshot_owned()
        self._started = False
        # Close the transport first. Any in-flight recv fails; the reader sees
        # _started=False and exits cleanly. Avoid task.cancel() — it would
        # propagate through asyncio.gather and cancel sibling handlers
        # mid-execution when close() is called from inside a handler.
        if transport is not None:
            transport.close()
        current = asyncio.current_task()
        if (
            reader is not None
            and reader is not current
            and current not in self._handler_workers
            and not reader.done()
        ):
            # suppress(Exception), not BaseException: asyncio.wait never
            # raises task failures, so anything wider could only swallow
            # the closer's own CancelledError — the caller's cancellation.
            with contextlib.suppress(Exception):
                await asyncio.wait({reader}, timeout=2.0)

    def _close_gevent(self) -> None:
        import gevent

        if not self._started:
            return
        # Snapshot before the flag — see _self_destruct.
        transport, reader = self._snapshot_owned()
        self._started = False
        if transport is not None:
            transport.close()
        current = gevent.getcurrent()
        if (
            reader is not None
            and reader is not current
            and current not in self._handler_workers
            and not reader.dead
        ):
            # suppress(Exception), not BaseException: join(timeout=...) never
            # raises its own timeout (gevent identity-checks its internal
            # timer), so anything wider could only swallow a caller's
            # enclosing gevent.Timeout or GreenletExit — their cancellation.
            with contextlib.suppress(Exception):
                reader.join(timeout=2.0)


__all__ = ["ProcessPublisher", "ProcessSubscriber", "Runtime"]
