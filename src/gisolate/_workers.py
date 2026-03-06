"""Child process worker entry points: gevent and asyncio modes."""

import contextlib
from typing import Any

import dill
import zmq

from ._internal import SmartPickle, wrap_exception

# ZMQ message markers (shared with proxy.py)
_OK = b"\x01"
_ERR = b"\x00"
_SHUTDOWN = b""


def safe_close(client: Any) -> None:
    """Safely call client.close() if it exists."""
    if close := getattr(client, "close", None):
        with contextlib.suppress(Exception):
            close()


def gevent_worker(
    ipc_addr: str, factory_bytes: bytes, timeout: float, patch_kwargs: dict
):
    """Gevent-based worker with greenlet concurrency."""
    import traceback

    import gevent
    import gevent.lock
    import gevent.monkey
    import gevent.pool
    import zmq.green as zmq_green

    gevent.monkey.patch_all(**patch_kwargs)
    gevent.get_hub()

    ctx = zmq_green.Context()
    sock = ctx.socket(zmq_green.ROUTER)
    sock.setsockopt(zmq.LINGER, 0)
    sock.bind(ipc_addr)

    factory = dill.loads(factory_bytes)
    client = None
    client_lock = gevent.lock.RLock()
    send_lock = gevent.lock.Semaphore()
    group = gevent.pool.Group()

    def send(identity: bytes, req_id: bytes, ok: bool, data: Any):
        with send_lock:
            with contextlib.suppress(zmq.ZMQError):
                sock.send_multipart(
                    [identity, req_id, _OK if ok else _ERR, SmartPickle.dumps(data)]
                )

    def handle(identity: bytes, req_id: bytes, method: str, args: tuple, kwargs: dict):
        nonlocal client
        try:
            with gevent.Timeout(timeout, TimeoutError(f"{method} timed out")):
                with client_lock:
                    client = client or factory()
                result = getattr(client, method)(*args, **kwargs)
            send(identity, req_id, True, result)
        except Exception as e:
            send(identity, req_id, False, wrap_exception(e, traceback.format_exc()))

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
            if payload == _SHUTDOWN:
                return False
            try:
                method, args, kwargs = SmartPickle.loads(payload)
            except Exception:
                send(identity, req_id, False, ValueError("malformed request"))
                continue
            group.spawn(handle, identity, req_id, method, args, kwargs)

    try:
        while True:
            if sock.poll(500) and not _drain():
                break
    except zmq.ZMQError:
        pass
    finally:
        group.join(timeout=6)
        safe_close(client)
        sock.close(linger=0)
        ctx.term()


def asyncio_worker(ipc_addr: str, factory_bytes: bytes, timeout: float):
    """Asyncio-based worker for async clients."""
    import asyncio
    import inspect
    import traceback

    import zmq.asyncio

    factory = dill.loads(factory_bytes)
    client = None
    lock = asyncio.Lock()
    tasks: set[asyncio.Task] = set()

    async def get_client():
        nonlocal client
        async with lock:
            if client is None:
                client = factory()
                if (connect := getattr(client, "connect", None)) is not None:
                    await connect() if inspect.iscoroutinefunction(
                        connect
                    ) else connect()
            return client

    async def send(sock, identity: bytes, req_id: bytes, ok: bool, data: Any):
        with contextlib.suppress(zmq.ZMQError):
            await sock.send_multipart(
                [identity, req_id, _OK if ok else _ERR, SmartPickle.dumps(data)]
            )

    async def handle(
        sock,
        identity: bytes,
        req_id: bytes,
        method: str,
        args: tuple,
        kwargs: dict,
    ):
        ok, result = False, None
        try:
            c = await get_client()
            fn = getattr(c, method)
            if inspect.iscoroutinefunction(fn):
                result = await asyncio.wait_for(fn(*args, **kwargs), timeout=timeout)
            else:
                loop = asyncio.get_running_loop()
                result = await asyncio.wait_for(
                    loop.run_in_executor(None, lambda: fn(*args, **kwargs)), timeout
                )
            ok = True
        except asyncio.TimeoutError:
            result = TimeoutError(f"{method} timed out")
        except Exception as e:
            result = wrap_exception(e, traceback.format_exc())
        await send(sock, identity, req_id, ok, result)

    async def close_client():
        if (close := getattr(client, "close", None)) is not None:
            with contextlib.suppress(Exception):
                await close() if inspect.iscoroutinefunction(close) else close()

    async def main():
        ctx = zmq.asyncio.Context()
        sock = ctx.socket(zmq.ROUTER)
        sock.setsockopt(zmq.LINGER, 0)
        sock.bind(ipc_addr)
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
                if payload == _SHUTDOWN:
                    break
                try:
                    method, args, kwargs = SmartPickle.loads(payload)
                except Exception:
                    await send(sock, identity, req_id, False, ValueError("malformed request"))
                    continue
                task = asyncio.create_task(
                    handle(sock, identity, req_id, method, args, kwargs)
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
