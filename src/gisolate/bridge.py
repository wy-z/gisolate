"""ProcessBridge: ZMQ-based RPC bridge for cross-process function calls."""

import contextlib
import enum
import itertools
import logging
import os
from typing import Any, Callable

import gevent

from ._internal import SmartPickle, wait_zmq_readable, wrap_exception
from ._workers import _ERR, _OK, _SHUTDOWN, _safe_dumps

log = logging.getLogger(__name__)


class ProcessBridge:
    """ZMQ-based RPC bridge for cross-process function calls.

    Server mode: Listens for requests, executes functions locally (gevent).
    Client mode: Sends requests to server, awaits results (asyncio).

    Args:
        address: IPC address (e.g., "ipc:///tmp/rpc.sock").
        mode: ProcessBridge.Mode.SERVER or ProcessBridge.Mode.CLIENT.
    """

    class Mode(enum.StrEnum):
        SERVER = "server"
        CLIENT = "client"

    def __init__(self, address: str, mode: "ProcessBridge.Mode"):
        self._addr = address
        self._init_mode = mode
        self._mode: ProcessBridge.Mode | None = None
        self._req_id = itertools.count()
        self._pending: dict[bytes, Any] = {}
        self._reader_task: Any = None
        self._server_greenlet: gevent.Greenlet | None = None

    def __del__(self):
        with contextlib.suppress(Exception):
            if getattr(self, "_mode", None) is not None:
                self.close()

    @property
    def address(self) -> str:
        """IPC address."""
        return self._addr

    def start(self) -> "ProcessBridge":
        """Start the bridge. Idempotent. Returns self for chaining."""
        if self._mode:
            return self
        if self._init_mode is ProcessBridge.Mode.CLIENT:
            self._start_client()
        else:
            self._start_server()
        return self

    async def call(self, func: Callable, *args, timeout: float = 60.0, **kwargs) -> Any:
        """Execute func on server. Starts client connection if needed.

        Safe for concurrent calls from multiple coroutines.
        """
        import asyncio

        if self._init_mode is ProcessBridge.Mode.SERVER:
            raise RuntimeError("Cannot call() in server mode")
        if not self._mode:
            self._start_client()
        elif self._reader_task is None or self._reader_task.done():
            self._reader_task = asyncio.ensure_future(self._read_responses())

        req_id = (next(self._req_id) & 0xFFFFFFFF).to_bytes(4)
        fut: asyncio.Future[tuple[bytes, bytes]] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending[req_id] = fut

        try:
            await self._sock.send_multipart(  # type: ignore[misc]
                [req_id, SmartPickle.dumps((func, args, kwargs))]
            )
            status, payload = await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Timed out after {timeout}s") from None
        finally:
            self._pending.pop(req_id, None)

        result = SmartPickle.loads(payload)
        if status != _OK:
            if tb := getattr(result, "__remote_traceback__", None):
                log.error(f"Remote traceback:\n{tb}")
            raise result
        return result

    async def _read_responses(self) -> None:
        """Single reader task: dispatch responses to pending futures."""
        import asyncio

        try:
            while True:
                parts = await self._sock.recv_multipart()  # type: ignore[misc]
                if len(parts) < 3:
                    continue
                resp_id, status, payload = parts[:3]
                if fut := self._pending.get(resp_id):
                    if not fut.done():
                        fut.set_result((status, payload))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            # Fail all pending on reader death
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(e)
            self._pending.clear()

    def _start_server(self):
        """Initialize server (raw zmq ROUTER socket — NOT zmq.green).

        Using raw zmq avoids zmq.green's busy-spin bug: its _Poller.poll()
        spins when the ZMQ FD is stuck readable, and its _Socket's persistent
        IO watcher interferes with select.select() via cached readiness state.
        The _serve loop uses a fresh gevent hub IO watcher for each wait.
        """
        import zmq

        self._mode = ProcessBridge.Mode.SERVER
        self._shutdown = False
        self._ctx = zmq.Context()
        self._sock = self._ctx.socket(zmq.ROUTER)
        self._sock.setsockopt(zmq.LINGER, 0)
        self._sock.setsockopt(zmq.ROUTER_MANDATORY, 1)
        self._sock.bind(self._addr)
        self._server_greenlet = gevent.spawn(self._serve)

    def _start_client(self):
        """Initialize client (asyncio DEALER socket + reader task)."""
        import asyncio

        import zmq.asyncio

        self._mode = ProcessBridge.Mode.CLIENT
        self._ctx = zmq.asyncio.Context()
        self._sock = self._ctx.socket(zmq.DEALER)
        self._sock.setsockopt(zmq.LINGER, 0)
        self._sock.connect(self._addr)
        self._reader_task = asyncio.ensure_future(self._read_responses())

    def _serve(self):
        """Server loop: dispatch each request to a greenlet for concurrency."""
        import gevent.lock
        import gevent.pool
        import zmq

        group = gevent.pool.Group()
        send_lock = gevent.lock.Semaphore()

        def _handle(identity: bytes, req_id: bytes, payload: bytes) -> None:
            import traceback

            try:
                func, args, kwargs = SmartPickle.loads(payload)
                data = func(*args, **kwargs)
                ok = True
            except Exception as exc:
                data = wrap_exception(exc, traceback.format_exc())
                ok = False
            resp, ok = _safe_dumps(data, ok)
            with send_lock:
                try:
                    self._sock.send_multipart(
                        [identity, req_id, _OK if ok else _ERR, resp],
                        flags=zmq.NOBLOCK,
                    )
                except zmq.ZMQError as exc:
                    log.warning("reply dropped: %s", exc)

        try:
            while not self._shutdown:
                while self._sock.getsockopt(zmq.EVENTS) & zmq.POLLIN:  # type: ignore[operator]
                    try:
                        parts: list[bytes] = self._sock.recv_multipart(zmq.NOBLOCK)  # type: ignore[assignment]
                    except zmq.Again:
                        break
                    if len(parts) < 3:
                        continue
                    identity, req_id, payload = parts[:3]
                    if payload == _SHUTDOWN:
                        self._shutdown = True
                        break
                    group.spawn(_handle, identity, req_id, payload)

                if self._shutdown:
                    break

                wait_zmq_readable(self._sock)
        except (gevent.GreenletExit, zmq.ZMQError):
            pass
        finally:
            group.join(timeout=6)

    def close(self):
        """Cleanup resources. Idempotent."""
        if not self._mode:
            return

        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
            self._reader_task = None

        err = ConnectionError("Bridge closed")
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(err)
        self._pending.clear()

        if self._mode is ProcessBridge.Mode.SERVER:
            self._shutdown = True
            # Wait for _serve greenlet to exit before closing socket
            if self._server_greenlet is not None:
                self._server_greenlet.join(timeout=2)
                if not self._server_greenlet.dead:
                    self._server_greenlet.kill(block=True, timeout=1)
                self._server_greenlet = None
            with contextlib.suppress(OSError):
                os.unlink(self._addr[6:])

        self._mode = None
        self._sock.close(linger=0)
        self._ctx.term()
