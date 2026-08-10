"""Child process worker entry points: gevent and asyncio modes."""

import contextlib
import dataclasses
import time
import traceback
from typing import Any

from . import _internal


@dataclasses.dataclass(frozen=True, slots=True)
class WorkerConfig:
    """Configuration passed from ProcessProxy to child worker."""

    ipc_addr: str
    factory_bytes: bytes
    max_concurrency: int | None = None


# ZMQ message markers (shared with proxy.py)
OK = b"\x01"
ERR = b"\x00"
SHUTDOWN = b""

# Request-id wraparound mask (64-bit, packed as 8 bytes on the wire)
ID_MASK = 0xFFFFFFFFFFFFFFFF


def safe_dumps(data: Any, ok: bool) -> tuple[bytes, bool]:
    """Serialize data, falling back to wrapped error on failure."""
    try:
        return _internal.SmartPickle.dumps(data), ok
    except Exception as exc:
        err = _internal.wrap_exception(exc, traceback.format_exc())
        return _internal.SmartPickle.dumps(err), False


def safe_close(client: Any) -> None:
    """Safely call client.close() if it exists."""
    if close := getattr(client, "close", None):
        with contextlib.suppress(Exception):
            close()


def _malformed(exc: Exception) -> Exception:
    """Wrap a request-parse failure as a serializable error response."""
    return _internal.wrap_exception(
        ValueError(f"malformed request: {exc!r}"), traceback.format_exc()
    )


def gevent_worker(cfg: WorkerConfig, patch_kwargs: dict):
    """Gevent-based worker with greenlet concurrency."""
    import gevent.monkey

    gevent.monkey.patch_all(**patch_kwargs)

    # imported after patch_all so they pick up patched stdlib
    import dill
    import gevent
    import gevent.lock
    import gevent.pool
    import zmq
    import zmq.green as zmq_green

    gevent.get_hub()

    ctx = zmq_green.Context()
    sock = ctx.socket(zmq_green.ROUTER)
    sock.setsockopt(zmq.LINGER, 0)
    sock.bind(cfg.ipc_addr)

    factory = dill.loads(cfg.factory_bytes)
    client = None
    client_lock = gevent.lock.RLock()
    send_lock = gevent.lock.Semaphore()
    # Unbounded group + explicit slot semaphore: admission happens inside
    # handle() with a deadline, so saturation can never block _drain.
    handlers = gevent.pool.Group()
    slots = gevent.lock.Semaphore(cfg.max_concurrency) if cfg.max_concurrency else None

    def send(identity: bytes, req_id: bytes, ok: bool, data: Any):
        resp, ok = safe_dumps(data, ok)
        with send_lock:
            with contextlib.suppress(zmq.ZMQError):
                sock.send_multipart([identity, req_id, OK if ok else ERR, resp])

    def _invoke(method: str, args: tuple, kwargs: dict, deadline: float):
        # Returns (ok, payload) instead of raising: an exception escaping
        # this greenlet would make gevent print an unhandled-greenlet
        # traceback to stderr for every client error, and handle() would
        # never send a reply. GreenletExit (kill, or client-raised) must
        # still propagate as the greenlet's outcome.
        nonlocal client
        try:
            with client_lock:
                if client is None:
                    # An expired request must not trigger one-time client init.
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"{method} timed out")
                    client = factory()
            # Admission re-checked at the last yield-free instant before the
            # client call: hub-callback backlog, client_lock contention, or a
            # slow factory() can delay this greenlet past the deadline even
            # when the spawning handler saw budget remaining.
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{method} timed out")
            return True, getattr(client, method)(*args, **kwargs)
        except gevent.GreenletExit:
            raise
        except BaseException as e:
            # BaseException too: a client's own escaping gevent.Timeout is one,
            # and letting it kill this greenlet costs the caller a reply.
            return False, _internal.wrap_exception(e, traceback.format_exc())

    def handle(
        identity: bytes,
        req_id: bytes,
        method: str,
        args: tuple,
        kwargs: dict,
        deadline: float,
    ):
        try:
            # Budget from request-accept time so time spent waiting for a
            # slot counts against the timeout (else a queued call can run
            # after the caller already gave up).
            remaining = deadline - time.monotonic()
            if remaining <= 0 or (slots is not None and not slots.acquire(timeout=remaining)):
                raise TimeoutError(f"{method} timed out")
            # The client call runs on its own greenlet: a deadline raised
            # into client code can be swallowed by retry-on-Exception loops,
            # but kill()'s GreenletExit is a BaseException no such loop can
            # catch (a client catching BaseException leaks its slot until
            # restart — unbeatable). The slot is released only when the
            # greenlet is truly dead — kill(block=False) leaves the call
            # unwinding, and freeing the slot before that breaks
            # max_concurrency. Spawned into handlers so shutdown's join
            # waits for unwinding calls, not just their handle greenlets.
            # (rawlink runs in the hub; Semaphore.release never blocks.)
            g = handlers.spawn(_invoke, method, args, kwargs, deadline)
            if slots is not None:
                release = slots.release  # bind: pyright can't narrow `slots` inside the lambda
                g.rawlink(lambda _g: release())
            g.join(max(deadline - time.monotonic(), 0.0))
            if not g.ready():
                g.kill(block=False)
                raise TimeoutError(f"{method} timed out")
            result = g.get(block=False)
            if isinstance(result, gevent.GreenletExit):
                # gevent counts GreenletExit as success; a client raising
                # it must not surface as an OK result.
                raise RuntimeError(f"{method} killed: {result}")
            ok, payload = result
            send(identity, req_id, ok, payload)
        except Exception as e:
            send(identity, req_id, False, _internal.wrap_exception(e, traceback.format_exc()))

    def _drain() -> bool:
        """Drain all available messages. Returns False on shutdown."""
        while True:
            try:
                parts = sock.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                return True
            if len(parts) < 3:
                continue
            identity, req_id, payload = parts[:3]
            if payload == SHUTDOWN:
                return False
            try:
                method, args, kwargs, timeout = _internal.SmartPickle.loads(payload)
            except Exception as e:
                send(identity, req_id, False, _malformed(e))
                continue
            handlers.spawn(
                handle, identity, req_id, method, args, kwargs,
                time.monotonic() + timeout,
            )

    try:
        while True:
            if sock.poll(500) and not _drain():
                break
    except zmq.ZMQError:
        pass
    finally:
        handlers.join(timeout=6)
        safe_close(client)
        sock.close(linger=0)
        ctx.term()


def asyncio_worker(cfg: WorkerConfig):
    """Asyncio-based worker for async clients."""
    import asyncio
    import inspect

    import dill
    import zmq
    import zmq.asyncio

    factory = dill.loads(cfg.factory_bytes)
    client = None
    lock = asyncio.Lock()
    send_lock = asyncio.Lock()
    sem: asyncio.Semaphore | None = (
        asyncio.Semaphore(cfg.max_concurrency) if cfg.max_concurrency else None
    )
    tasks: set[asyncio.Task] = set()

    async def _dispose(c):
        """Best-effort client close; awaits an async close. Never raises."""
        with contextlib.suppress(Exception):
            if (close := getattr(c, "close", None)) is not None:
                r = close()
                if inspect.isawaitable(r):
                    await r

    async def get_client():
        nonlocal client
        async with lock:
            if client is None:
                # Publish only after connect succeeds: the per-call deadline
                # in handle() can cancel us mid-connect, and a half-connected
                # client stored here would be reused by every later call.
                c = factory()
                try:
                    if (connect := getattr(c, "connect", None)) is not None:
                        result = connect()
                        if inspect.isawaitable(result):
                            await result
                except BaseException:
                    # Dispose of the orphan (cancelled mid-connect or failed
                    # connect) so retry storms don't leak sockets. Detached:
                    # awaiting here, inside a task being cancelled, could
                    # hang or be re-cancelled; main() drains ``tasks``.
                    t = asyncio.get_running_loop().create_task(_dispose(c))
                    tasks.add(t)
                    t.add_done_callback(tasks.discard)
                    raise
                client = c
            return client

    async def send(sock, identity: bytes, req_id: bytes, ok: bool, data: Any):
        resp, ok = safe_dumps(data, ok)
        async with send_lock:
            with contextlib.suppress(zmq.ZMQError):
                await sock.send_multipart([identity, req_id, OK if ok else ERR, resp])

    async def _call(method: str, args: tuple, kwargs: dict):
        c = await get_client()
        fn = getattr(c, method)
        if inspect.iscoroutinefunction(fn):
            return await fn(*args, **kwargs)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    async def handle(
        sock,
        identity: bytes,
        req_id: bytes,
        method: str,
        args: tuple,
        kwargs: dict,
        deadline: float,
    ):
        ok, result = False, None
        tm = None
        try:
            async with sem if sem else contextlib.nullcontext():
                # Budget from accept time so time spent waiting on the
                # semaphore counts against the timeout.
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError
                async with asyncio.timeout(remaining) as tm:
                    result = await _call(method, args, kwargs)
                    ok = True
        except TimeoutError as e:
            # A client-raised TimeoutError is indistinguishable by type from
            # our deadline (asyncio.TimeoutError IS TimeoutError since 3.10);
            # tm.expired() tells them apart so the client's message survives.
            if tm is not None and not tm.expired():
                result = _internal.wrap_exception(e, traceback.format_exc())
            else:
                result = TimeoutError(f"{method} timed out")
        except Exception as e:
            result = _internal.wrap_exception(e, traceback.format_exc())
        except asyncio.CancelledError as e:
            # Cancellation aimed at *this* task (loop shutdown) must stay
            # cancelled and send nothing. A CancelledError merely leaked by
            # client code — an inner cancelled await escaping the method —
            # leaves no cancellation request here, so it is an ordinary
            # failed call; letting it kill this task would send no reply at
            # all and strand the caller until its own grace period.
            task = asyncio.current_task()
            if task is None or task.cancelling():
                raise
            result = _internal.wrap_exception(e, traceback.format_exc())
        except BaseException as e:
            result = _internal.wrap_exception(e, traceback.format_exc())
        await send(sock, identity, req_id, ok, result)

    async def main():
        ctx = zmq.asyncio.Context()
        sock = ctx.socket(zmq.ROUTER)
        sock.setsockopt(zmq.LINGER, 0)
        sock.bind(cfg.ipc_addr)
        poller = zmq.asyncio.Poller()
        poller.register(sock, zmq.POLLIN)

        try:
            while True:
                if not await poller.poll(1000):
                    continue
                parts = await sock.recv_multipart()
                if len(parts) < 3:
                    continue
                identity, req_id, payload = parts[:3]
                if payload == SHUTDOWN:
                    break
                try:
                    method, args, kwargs, timeout = _internal.SmartPickle.loads(payload)
                except Exception as e:
                    await send(sock, identity, req_id, False, _malformed(e))
                    continue
                task = asyncio.create_task(
                    handle(
                        sock, identity, req_id, method, args, kwargs,
                        time.monotonic() + timeout,
                    )
                )
                tasks.add(task)
                task.add_done_callback(tasks.discard)

            if tasks:
                await asyncio.wait(tasks, timeout=6)
        finally:
            await _dispose(client)
            sock.close(linger=0)
            ctx.term()

    try:
        asyncio.run(main())
    except zmq.ZMQError:
        pass
