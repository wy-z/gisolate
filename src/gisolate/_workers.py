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
    timeout: float
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


def _unpack(payload: bytes, default_timeout: float) -> tuple[str, tuple, dict, float]:
    """Unpack request payload (3-tuple legacy or 4-tuple with timeout)."""
    method, args, kwargs, *rest = _internal.SmartPickle.loads(payload)
    return method, args, kwargs, rest[0] if rest else default_timeout


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
    pool = (
        gevent.pool.Pool(cfg.max_concurrency)
        if cfg.max_concurrency
        else gevent.pool.Group()
    )

    def send(identity: bytes, req_id: bytes, ok: bool, data: Any):
        resp, ok = safe_dumps(data, ok)
        with send_lock:
            with contextlib.suppress(zmq.ZMQError):
                sock.send_multipart([identity, req_id, OK if ok else ERR, resp])

    def handle(
        identity: bytes,
        req_id: bytes,
        method: str,
        args: tuple,
        kwargs: dict,
        deadline: float,
    ):
        nonlocal client
        try:
            # Budget from request-accept time so time spent queued behind the
            # pool counts against the timeout (else a queued call can run after
            # the caller already gave up).
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"{method} timed out")
            with gevent.Timeout(remaining, TimeoutError(f"{method} timed out")):
                with client_lock:
                    if client is None:
                        client = factory()
                result = getattr(client, method)(*args, **kwargs)
            send(identity, req_id, True, result)
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
                method, args, kwargs, timeout = _unpack(payload, cfg.timeout)
            except Exception as e:
                send(identity, req_id, False, _malformed(e))
                continue
            pool.spawn(
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
        pool.join(timeout=6)
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

    async def get_client():
        nonlocal client
        async with lock:
            if client is None:
                client = factory()
                if (connect := getattr(client, "connect", None)) is not None:
                    result = connect()
                    if inspect.isawaitable(result):
                        await result
            return client

    async def send(sock, identity: bytes, req_id: bytes, ok: bool, data: Any):
        resp, ok = safe_dumps(data, ok)
        async with send_lock:
            with contextlib.suppress(zmq.ZMQError):
                await sock.send_multipart([identity, req_id, OK if ok else ERR, resp])

    async def _call(method: str, args: tuple, kwargs: dict, timeout: float):
        c = await get_client()
        fn = getattr(c, method)
        if inspect.iscoroutinefunction(fn):
            return await asyncio.wait_for(fn(*args, **kwargs), timeout=timeout)
        loop = asyncio.get_running_loop()
        return await asyncio.wait_for(
            loop.run_in_executor(None, lambda: fn(*args, **kwargs)), timeout
        )

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
        try:
            async with sem if sem else contextlib.nullcontext():
                # Budget from accept time so time spent waiting on the
                # semaphore counts against the timeout.
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise asyncio.TimeoutError
                result = await _call(method, args, kwargs, remaining)
                ok = True
        except asyncio.TimeoutError:
            result = TimeoutError(f"{method} timed out")
        except Exception as e:
            result = _internal.wrap_exception(e, traceback.format_exc())
        await send(sock, identity, req_id, ok, result)

    async def close_client():
        if (close := getattr(client, "close", None)) is not None:
            with contextlib.suppress(Exception):
                result = close()
                if inspect.isawaitable(result):
                    await result

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
                    method, args, kwargs, timeout = _unpack(payload, cfg.timeout)
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
            await close_client()
            sock.close(linger=0)
            ctx.term()

    try:
        asyncio.run(main())
    except zmq.ZMQError:
        pass
