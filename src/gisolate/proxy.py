"""ProcessProxy: transparent method proxy to an isolated child process."""

import abc
import contextlib
import functools
import itertools
import logging
import multiprocessing
import os
import signal
import tempfile
import time
import uuid
from typing import Any, Callable, TypeVar

import dill
import gevent
import gevent.event
import zmq

from . import _internal, hub
from ._internal import ProcessError, SmartPickle
from ._workers import _OK, _SHUTDOWN, asyncio_worker, gevent_worker

log = logging.getLogger(__name__)
T = TypeVar("T")


def _get_ipc_dir() -> str:
    """Get a private directory for IPC sockets (mode 0o700)."""
    d = os.path.join(tempfile.gettempdir(), f"gi-{os.getuid()}")
    os.makedirs(d, mode=0o700, exist_ok=True)
    return d


_ZMQ_TMPDIR = _get_ipc_dir()
_default_mp_context: Any = None


def set_default_mp_context(ctx: Any) -> None:
    """Set the default multiprocessing context for all proxies."""
    global _default_mp_context
    _default_mp_context = ctx


def get_default_mp_context() -> Any:
    """Get the default multiprocessing context (spawn if not configured)."""
    return _default_mp_context or multiprocessing.get_context("spawn")


def _pack_id(n: int) -> bytes:
    return (n & 0xFFFFFFFF).to_bytes(4)


def _unpack_id(data: bytes) -> int:
    return int.from_bytes(data)


class ProcessProxy(abc.ABC):
    """Proxy executing operations in an isolated child process via ZMQ IPC.

    The child process is spawned without gevent monkey-patching (unless
    patch_kwargs is set), providing a clean environment for libraries
    that are incompatible with gevent.

    Subclass must implement: client_factory()
    Optional class attrs: mp_context, timeout, patch_kwargs

    Thread safety:
        - execute(): thread-safe (greenlets and native threads)
        - restart_process(): thread-safe (marshals to main hub if needed)
        - shutdown(): thread-safe (marshals to main hub if needed)
    """

    AUTO_RESTART_THRESHOLD = 6
    RESTART_COOLDOWN = 6.0
    ALIVE_CHECK_INTERVAL = 10
    mp_context: Any = None
    patch_kwargs: dict | None = None
    timeout: float = 24

    @staticmethod
    @abc.abstractmethod
    def client_factory() -> Any: ...

    def __init__(self):
        if getattr(multiprocessing.current_process(), "_inheriting", False):
            raise RuntimeError(
                "Cannot create ProcessProxy during subprocess bootstrapping. "
                "Wrap your code with: if __name__ == '__main__':"
            )

        self._pending: dict[int, Any] = {}
        self._cache: dict[str, Any] = {}
        self._lock = _internal.RLock()
        self._counter = itertools.count()
        self._last_restart = 0.0
        self._error_count = 0
        self._shutdown = False
        self._alive = False
        self._process: Any = None
        self._reader: gevent.Greenlet | None = None
        self._ctx: Any = None
        self._sock: Any = None
        self._addr: str | None = None
        self._owner = _internal.current_thread()
        hub.ensure_started()
        self._start()

    # --- Lifecycle ---

    def _get_mp_context(self) -> Any:
        return type(self).mp_context or get_default_mp_context()

    def _start(self):
        """Start child process if not running."""
        if not gevent.get_hub().loop.default:
            return hub.run_on_main_hub(self._start)

        import zmq.green as zmq_green

        self._owner = _internal.current_thread()
        with self._lock:
            if self._shutdown or self._process is not None:
                return

            cls = type(self)
            self._addr = f"ipc://{_ZMQ_TMPDIR}/gi-{uuid.uuid4().hex[:16]}.sock"
            self._ctx = zmq_green.Context()
            self._sock = self._ctx.socket(zmq_green.DEALER)
            self._sock.setsockopt(zmq.LINGER, 0)
            self._sock.connect(self._addr)

            factory_bytes = dill.dumps(cls.client_factory)
            base_args = (self._addr, factory_bytes, cls.timeout)
            worker, args = (
                (gevent_worker, (*base_args, cls.patch_kwargs))
                if cls.patch_kwargs is not None
                else (asyncio_worker, base_args)
            )

            mp_ctx = self._get_mp_context()
            self._process = mp_ctx.Process(target=worker, daemon=True, args=args)
            self._process.start()
            log.info(f"ProcessProxy started: pid={self._process.pid}, ctx={mp_ctx}")
            self._alive = True
            self._reader = gevent.spawn(self._read_loop)
            self._error_count = 0

    def _stop(self, error: Exception | None = None, timeout: float = 2.0):
        """Stop child process. Idempotent."""
        with self._lock:
            if self._process is None:
                return
            self._alive = False
            reader, self._reader = self._reader, None
            process, self._process = self._process, None
            sock, self._sock = self._sock, None
            ctx, self._ctx = self._ctx, None
            addr, self._addr = self._addr, None
            pending = list(self._pending.values())
            self._pending.clear()

        if reader and reader is not gevent.getcurrent():
            with contextlib.suppress(gevent.Timeout):
                reader.kill(block=True, timeout=timeout)

        self._cleanup_zmq(sock, ctx, addr)
        self._cleanup_process(process)

        err = error or ProcessError("Process stopped")
        for ar in pending:
            ar.set_exception(err)

    def _cleanup_zmq(self, sock, ctx, addr: str | None) -> None:
        if not sock:
            return
        with contextlib.suppress(zmq.ZMQError):
            sock.send_multipart([b"0", _SHUTDOWN], zmq.NOBLOCK)
        with contextlib.suppress(zmq.ZMQError):
            sock.close(linger=0)
        if ctx:
            with contextlib.suppress(zmq.ZMQError):
                ctx.term()
        if addr:
            with contextlib.suppress(OSError):
                os.unlink(addr[6:])

    def _cleanup_process(self, process) -> None:
        process.join(timeout=0.3)
        if not process.is_alive():
            return
        process.terminate()
        process.join(timeout=0.5)
        if process.is_alive() and process.pid:
            log.warning("Process did not terminate, sending SIGKILL")
            with contextlib.suppress(OSError):
                os.kill(process.pid, signal.SIGKILL)
            process.join(timeout=0.5)

    def _is_alive(self) -> bool:
        return self._process is not None and self._process.is_alive()

    def restart_process(self) -> None:
        """Kill and restart child process. Thread-safe (marshals to main hub)."""
        if not gevent.get_hub().loop.default:
            return hub.run_on_main_hub(self.restart_process)
        with self._lock:
            now = time.monotonic()
            if self._is_alive() and now - self._last_restart < self.RESTART_COOLDOWN:
                log.warning("Restart skipped (cooldown)")
                return
            self._last_restart = now
        self._stop(timeout=0.5)
        self._start()

    def shutdown(self, timeout: float = 10.0):
        """Gracefully shutdown child process. Thread-safe."""
        if not gevent.get_hub().loop.default:
            return hub.run_on_main_hub(functools.partial(self.shutdown, timeout))
        self._shutdown = True
        self._stop(timeout=timeout)

    def __del__(self):
        # Minimal cleanup only — avoid full shutdown() which marshals
        # to main hub and may fail during GC or interpreter shutdown.
        with contextlib.suppress(Exception):
            process = getattr(self, "_process", None)
            if process is not None:
                process.terminate()
            sock = getattr(self, "_sock", None)
            if sock is not None:
                sock.close(linger=0)
            ctx = getattr(self, "_ctx", None)
            if ctx is not None:
                ctx.term()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.shutdown()

    # --- Response Handling ---

    def _read_loop(self):
        """Dispatch responses from child process to waiting callers."""
        try:
            idle_cycles = 0
            while not self._shutdown:
                if self._sock.poll(50):
                    self._drain()
                    idle_cycles = 0
                else:
                    idle_cycles += 1
                    if (
                        idle_cycles >= self.ALIVE_CHECK_INTERVAL
                        and not self._is_alive()
                    ):
                        break
            if not self._shutdown:
                log.warning("Child process died, stopping reader")
        except (gevent.GreenletExit, zmq.ZMQError):
            pass
        except Exception as e:
            log.warning(f"Reader greenlet error: {e}")
        finally:
            self._alive = False
            self._stop(ProcessError("Child process disconnected"))

    def _drain(self) -> None:
        """Receive and dispatch all available responses."""
        while True:
            try:
                parts = self._sock.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                return
            if len(parts) < 3:
                continue
            req_id_bytes, ok_flag, payload = parts[:3]
            req_id = _unpack_id(req_id_bytes)
            try:
                result = SmartPickle.loads(payload)
                ok = ok_flag == _OK
            except Exception as e:
                log.warning(f"Failed to deserialize response: {e}")
                result, ok = ProcessError(f"Bad response: {e}"), False
            if not ok and (tb := getattr(result, "__remote_traceback__", None)):
                log.error(f"Remote traceback:\n{tb}")
            with self._lock:
                ar = self._pending.pop(req_id, None)
            if ar:
                (ar.set if ok else ar.set_exception)(result)

    # --- Execute ---

    def execute(self, method: str, *args, **kwargs) -> Any:
        """Send method call to child process and wait for response. Thread-safe."""
        self._ensure_running()

        req_id = next(self._counter) & 0xFFFFFFFF
        ar = (
            gevent.event.AsyncResult()
            if _internal.current_thread() is self._owner
            else hub.AsyncResult()
        )
        with self._lock:
            self._pending[req_id] = ar

        try:
            if err := self._send(req_id, method, args, kwargs):
                if isinstance(err, ProcessError):
                    raise err
                self.restart_process()
                raise ProcessError("Failed to send request to child") from err

            result = ar.get(timeout=self.timeout + max(2.0, self.timeout * 0.1))
            with self._lock:
                self._error_count = 0
            return result
        except gevent.Timeout:
            raise TimeoutError(f"{method} timed out after {self.timeout}s") from None
        except ProcessError:
            with self._lock:
                self._error_count += 1
                should_restart = self._error_count >= self.AUTO_RESTART_THRESHOLD
            if should_restart:
                log.warning(f"{self._error_count} consecutive errors, restarting")
                self.restart_process()
            raise
        finally:
            with self._lock:
                self._pending.pop(req_id, None)

    def _send(
        self, req_id: int, method: str, args: tuple, kwargs: dict
    ) -> Exception | None:
        try:
            payload = SmartPickle.dumps((method, args, kwargs))
            with self._lock:
                self._sock.send_multipart([_pack_id(req_id), payload], zmq.NOBLOCK)
            return None
        except Exception as e:
            return e

    def _ensure_running(self) -> None:
        if self._shutdown:
            raise RuntimeError("Proxy is shutdown")
        if not self._alive or not self._is_alive():
            self._alive = False
            self.restart_process()
            if self._sock is None:
                raise ProcessError("Process not running")

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")
        return self._cache.setdefault(name, functools.partial(self.execute, name))

    @classmethod
    def create(
        cls,
        factory: Callable[[], T],
        *,
        timeout: float = 24,
        mp_context: Any = None,
        patch_kwargs: dict | None = None,
    ) -> T:  # type: ignore[misc]
        """Create a proxy without subclassing.

        Args:
            factory: Callable that returns the client object in the child process.
            timeout: Per-call timeout in seconds.
            mp_context: Multiprocessing context (default: spawn or configured default).
            patch_kwargs: If dict, child uses gevent. If None, child uses asyncio.
        """
        ns: dict[str, Any] = {
            "client_factory": staticmethod(factory),
            "timeout": timeout,
            "patch_kwargs": patch_kwargs,
        }
        if mp_context is not None:
            ns["mp_context"] = mp_context
        klass = type(f"ProcessProxy<{factory.__qualname__}>", (cls,), ns)
        return klass()  # type: ignore[return-value]
