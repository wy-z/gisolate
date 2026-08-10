"""ProcessProxy: transparent method proxy to an isolated child process."""

import abc
import contextlib
import functools
import itertools
import logging
import multiprocessing
import multiprocessing.connection
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

from . import _internal, _workers, hub

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
    return (n & _workers.ID_MASK).to_bytes(8)


def _unpack_id(data: bytes) -> int:
    return int.from_bytes(data)


def _proc_exited(process: Any) -> bool:
    """True iff the child has exited — even if something else reaped it.

    ``Process.is_alive()`` trusts ``waitpid``: under a gevent parent, libev's
    default-loop SIGCHLD handler reaps children first, ``waitpid`` then fails
    with ECHILD and ``poll()`` answers "still running" forever — a segfaulted
    child passed every liveness check for hours (q-trade #435). The sentinel
    fd turns readable exactly when the child exits, no reaping involved.
    """
    try:
        return bool(multiprocessing.connection.wait([process.sentinel], timeout=0))
    except (OSError, ValueError):
        return True  # sentinel closed/invalid: no live child behind it


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

    mp_context: Any = None
    patch_kwargs: dict | None = None
    timeout: float = 24
    max_concurrency: int | None = None
    daemon: bool = True
    auto_restart_threshold: int = 6
    restart_cooldown: float = 6.0
    alive_check_idle_cycles: int = 10

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
        self._process: Any = None
        self._reader: gevent.Greenlet | None = None
        self._ctx: Any = None
        self._sock: Any = None
        self._addr: str | None = None
        self._owner = _internal.current_thread()
        hub.ensure_hub_started()
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

            config = _workers.WorkerConfig(
                ipc_addr=self._addr,
                factory_bytes=dill.dumps(cls.client_factory),
                timeout=cls.timeout,
                max_concurrency=cls.max_concurrency,
            )
            worker, args = (
                (_workers.gevent_worker, (config, cls.patch_kwargs))
                if cls.patch_kwargs is not None
                else (_workers.asyncio_worker, (config,))
            )

            mp_ctx = self._get_mp_context()
            self._process = mp_ctx.Process(target=worker, daemon=cls.daemon, args=args)
            with _internal.suppress_main_reimport():
                self._process.start()
            log.info(f"ProcessProxy started: pid={self._process.pid}, ctx={mp_ctx}")
            self._reader = gevent.spawn(self._read_loop)
            self._error_count = 0

    def _stop(
        self,
        error: Exception | None = None,
        timeout: float = 2.0,
        graceful: bool = False,
    ):
        """Stop child process. Idempotent.

        ``graceful=True`` waits up to ``timeout`` for the worker to drain
        in-flight calls + close its client before we tear sockets down and
        fall back to terminate/kill.
        """
        with self._lock:
            if self._process is None:
                return
            reader, self._reader = self._reader, None
            process, self._process = self._process, None
            sock, self._sock = self._sock, None
            ctx, self._ctx = self._ctx, None
            addr, self._addr = self._addr, None
            pending = list(self._pending.values())
            self._pending.clear()

        try:
            if reader and reader is not gevent.getcurrent():
                # kill(block=True, timeout=...) never raises its own timeout
                # (gevent's join identity-checks its internal timer), so a
                # gevent.Timeout here can only be a caller's enclosing
                # deadline — it must propagate, not be suppressed.
                reader.kill(block=True, timeout=timeout)

            if sock is not None:
                # Best-effort SHUTDOWN, sent before the socket closes so the
                # worker can drain + close its client (close(linger=0) would
                # drop an unsent frame). NOBLOCK: a blocking send would hang
                # the main hub if the DEALER is backpressured; a full send
                # queue means the child is already stuck and terminate/kill
                # below handles it.
                with contextlib.suppress(zmq.ZMQError):
                    sock.send_multipart([b"0", _workers.SHUTDOWN], zmq.NOBLOCK)
            if graceful:
                # Give the child up to ``timeout`` to drain in-flight calls
                # and exit on the SHUTDOWN frame, before terminate/kill cuts
                # it short.
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline and not _proc_exited(process):
                    gevent.sleep(0.05)
        finally:
            # State is already detached, so a caller's enclosing gevent.Timeout
            # must not leave teardown half-done: the child, socket and ipc file
            # would leak with no handle left to reach them, and pending waiters
            # would never learn the process is gone. The waits above aren't the
            # only interruption points — _cleanup_process's process.join()s are
            # gevent switch points too, so run teardown on its own greenlet: it
            # completes even if our join() below is interrupted mid-reap.
            def _teardown():
                self._cleanup_zmq(sock, ctx, addr)
                self._cleanup_process(process)
                err = error or _internal.ProcessError("Process stopped")
                for ar in pending:
                    ar.set_exception(err)

            gevent.spawn(_teardown).join()

    def _cleanup_zmq(self, sock, ctx, addr: str | None) -> None:
        if not sock:
            return
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
        # Gate signaling on the sentinel, not is_alive(): after a stolen reap
        # (see _proc_exited) is_alive() stays True and the pid may already be
        # recycled — terminate/SIGKILL would hit an innocent process.
        if _proc_exited(process):
            return
        process.terminate()
        process.join(timeout=0.5)
        if not _proc_exited(process) and process.pid:
            log.warning("Process did not terminate, sending SIGKILL")
            with contextlib.suppress(OSError):
                os.kill(process.pid, signal.SIGKILL)
            process.join(timeout=0.5)

    def _is_alive(self) -> bool:
        # Single read: a concurrent _stop may null _process between checks.
        p = self._process
        return p is not None and not _proc_exited(p)

    def restart_process(self) -> None:
        """Kill and restart child process. Thread-safe (marshals to main hub)."""
        if not gevent.get_hub().loop.default:
            return hub.run_on_main_hub(self.restart_process)
        with self._lock:
            now = time.monotonic()
            # Uniform cooldown, dead child included: a client that crashes
            # during startup would otherwise respawn on every execute.
            # Callers aren't stranded — _ensure_running fails fast on a
            # skipped restart, and _stop flushes pending waiters.
            if now - self._last_restart < self.restart_cooldown:
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
        self._stop(timeout=timeout, graceful=True)

    def __del__(self):
        # Minimal cleanup only — avoid full shutdown() which marshals
        # to main hub and may fail during GC or interpreter shutdown.
        # Two suppressions, not one: _proc_exited polls the sentinel, which is
        # a gevent switch point, so a caller's Timeout (a BaseException) can
        # land inside the liveness check — it must not take the socket
        # teardown down with it.
        with contextlib.suppress(BaseException):
            process = getattr(self, "_process", None)
            # Same sentinel gate as _cleanup_process: terminate() signals the
            # recorded pid whenever mp thinks the child is unreaped, and after
            # a stolen reap that pid may belong to someone else.
            if process is not None and not _proc_exited(process):
                process.terminate()
        with contextlib.suppress(Exception):
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
                if self._sock.poll(50) and self._drain():
                    idle_cycles = 0
                else:
                    idle_cycles += 1
                if idle_cycles >= self.alive_check_idle_cycles and not self._is_alive():
                    break
            if not self._shutdown:
                log.warning("Child process died, stopping reader")
        except (gevent.GreenletExit, zmq.ZMQError):
            pass
        except Exception as e:
            log.warning(f"Reader greenlet error: {e}", exc_info=True)
        finally:
            self._stop(_internal.ProcessError("Child process disconnected"))

    def _drain(self) -> bool:
        """Receive and dispatch all available responses; return True if any received."""
        received = False
        while True:
            try:
                parts = self._sock.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                return received
            received = True
            if len(parts) < 3:
                continue
            req_id_bytes, ok_flag, payload = parts[:3]
            req_id = _unpack_id(req_id_bytes)
            try:
                result = _internal.SmartPickle.loads(payload)
                ok = ok_flag == _workers.OK
            except Exception as e:
                log.warning(f"Failed to deserialize response: {e}")
                result, ok = _internal.ProcessError(f"Bad response: {e}"), False
            if not ok and (tb := getattr(result, "__remote_traceback__", None)):
                log.error(f"Remote traceback:\n{tb}")
            with self._lock:
                ar = self._pending.pop(req_id, None)
            if ar:
                (ar.set if ok else ar.set_exception)(result)

    # --- Execute ---

    def execute(self, method: str, *args, **kwargs) -> Any:
        """Send method call to child process and wait for response. Thread-safe."""
        return self._execute(method, args, kwargs, self.timeout)

    def _execute(
        self, method: str, args: tuple, kwargs: dict, rpc_timeout: float
    ) -> Any:
        self._ensure_running()

        req_id = next(self._counter) & _workers.ID_MASK
        is_owner = _internal.current_thread() is self._owner
        ar = gevent.event.AsyncResult() if is_owner else hub.AsyncResult()
        with self._lock:
            self._pending[req_id] = ar

        try:
            if err := self._send(req_id, method, args, kwargs, rpc_timeout, is_owner):
                if isinstance(err, _internal.ProcessError):
                    raise err
                self.restart_process()
                raise _internal.ProcessError("Failed to send request to child") from err

            wait_timeout = rpc_timeout + max(2.0, rpc_timeout * 0.1)
            if is_owner:
                # Custom-exception form: on expiry gevent raises OUR labeled
                # TimeoutError instead of the Timeout instance, so a caller's
                # enclosing gevent.Timeout propagates as itself and a child-
                # sent TimeoutError re-raised by ar.get() keeps its message.
                with gevent.Timeout(
                    wait_timeout,
                    TimeoutError(f"{method} timed out after {rpc_timeout}s"),
                ):
                    result = ar.get()
            else:
                try:
                    # hub.WaitTimeout is raised only by hub.AsyncResult's own
                    # wait deadline; a child-sent TimeoutError re-raised by
                    # ar.get() passes through un-relabeled.
                    result = ar.get(timeout=wait_timeout)
                except hub.WaitTimeout:
                    raise TimeoutError(
                        f"{method} timed out after {rpc_timeout}s"
                    ) from None
            with self._lock:
                self._error_count = 0
            return result
        except _internal.ProcessError:
            with self._lock:
                self._error_count += 1
                should_restart = self._error_count >= self.auto_restart_threshold
            if should_restart:
                log.warning(f"{self._error_count} consecutive errors, restarting")
                self.restart_process()
            raise
        finally:
            with self._lock:
                self._pending.pop(req_id, None)

    def _send(
        self,
        req_id: int,
        method: str,
        args: tuple,
        kwargs: dict,
        rpc_timeout: float,
        is_owner: bool,
    ) -> Exception | None:
        try:
            payload = _internal.SmartPickle.dumps((method, args, kwargs, rpc_timeout))
            frames = [_pack_id(req_id), payload]
            # The zmq.green socket is owned by the main hub (where the reader
            # greenlet recv()s). Sending from the owner (main) thread shares
            # that single OS thread — safe. A non-owner native thread must
            # marshal the send to the main hub; touching the socket from two
            # OS threads races libzmq's non-thread-safe socket.
            if is_owner:
                self._raw_send(req_id, frames)
            else:
                try:
                    hub.run_on_main_hub(
                        functools.partial(self._raw_send, req_id, frames),
                        timeout=rpc_timeout,
                    )
                except hub.WaitTimeout as e:
                    # Only a marshal timeout proves the main hub is unresponsive
                    # (the send never reached it). Restarting needs the hub too,
                    # so fail fast rather than block on another marshal.
                    raise TimeoutError(f"{method}: main hub unresponsive") from e
            return None
        except TimeoutError:
            raise  # propagate to caller; never feed the restart path
        except Exception as e:
            return e

    def _raw_send(self, req_id: int, frames: list[bytes]) -> None:
        with self._lock:
            # A marshaled send may run late (e.g. after the call timed out and
            # the socket was restarted). Skip unless the request is still live,
            # so a stale frame is never sent to a replacement child/socket.
            if req_id not in self._pending:
                return
            # Still pending but the socket is gone: _stop landed between the
            # liveness check and registration (_is_alive is a switch point —
            # mp.connection.wait polls a patched select). Dropping silently
            # here would leave the caller waiting out its full rpc timeout.
            if self._sock is None:
                raise _internal.ProcessError("Process not running")
            self._sock.send_multipart(frames, zmq.NOBLOCK)

    def _ensure_running(self) -> None:
        if self._shutdown:
            raise RuntimeError("Proxy is shutdown")
        if not self._is_alive():
            self.restart_process()
            # Still dead (restart in cooldown, or start failed): fail fast.
            # Proceeding would send into the void and burn the full timeout.
            if not self._is_alive():
                raise _internal.ProcessError("Process not running")

    def with_timeout(self, timeout: float) -> "_TimeoutView":
        """Return a view that uses the given timeout for all calls.

        Usage: proxy.with_timeout(60).slow_method(args)
        """
        return _TimeoutView(self, timeout)

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
        max_concurrency: int | None = None,
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
            "max_concurrency": max_concurrency,
        }
        if mp_context is not None:
            ns["mp_context"] = mp_context
        klass = type(f"ProcessProxy<{factory.__qualname__}>", (cls,), ns)
        return klass()  # type: ignore[return-value]


class _TimeoutView:
    """Lightweight view that forwards calls with a custom timeout."""

    __slots__ = ("_proxy", "_timeout")

    def __init__(self, proxy: ProcessProxy, timeout: float):
        self._proxy = proxy
        self._timeout = timeout

    def __getattr__(self, name: str):
        def proxy_method(*args, **kwargs):
            return self._proxy._execute(name, args, kwargs, self._timeout)

        return proxy_method
