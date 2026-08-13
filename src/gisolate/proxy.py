"""ProcessProxy: transparent method proxy to an isolated child process."""

import abc
import contextlib
import functools
import itertools
import logging
import multiprocessing
import multiprocessing.connection
import multiprocessing.context
import multiprocessing.process
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


try:  # POSIX only: a non-POSIX or custom launcher keeps the limitation
    from multiprocessing.popen_fork import Popen as _StdlibForkPopen
    from multiprocessing.popen_spawn_posix import Popen as _StdlibSpawnPopen
except ImportError:  # pragma: no cover - Windows
    _StdlibForkPopen = _StdlibSpawnPopen = object


def _publish_failed_launch(popen: Any, process_obj: Any) -> None:
    """Hand back a child whose launch raised after creating it.

    ``Process.start()`` is not atomic. Both stdlib POSIX launchers record the
    pid and then the sentinel, and ``BaseProcess.start()`` publishes ``_popen``
    and registers ``_children`` only once the launcher has RETURNED — so a
    failure in between (the child OOM-killed during bootstrap) strands a child
    nothing can reach: not multiprocessing, not the Process, not us.

    Measured, with a child multiprocessing never learned about: nobody reaps
    it, so it is a zombie until this process exits. And if the payload did
    arrive it may be RUNNING, holding the address whose cleanup this path used
    to skip.

    With a sentinel, the child is published and registered — registered because
    our own kill grace can give up, and in ``_children`` the interpreter's exit
    handler finishes what it could not. Without one it is killed and reaped on
    the spot instead: the pid is recorded first and is all that takes, while
    every cleanup path here joins, and join reads the sentinel that is missing.

    ``waitpid(-1)`` is not an alternative: it would consume the exit status of
    children belonging to the host application.
    """
    pid = getattr(popen, "pid", None)
    if pid is None:
        return
    # try/except rather than contextlib.suppress throughout: suppress is an
    # allocation, and a MemoryError building the guard would have skipped the
    # kill and reap the guard was for — leaving a child known to nothing.
    if getattr(popen, "sentinel", None) is None:
        # Finished here rather than handed on: every cleanup path in this
        # package joins, and join reads the sentinel this launcher never got to
        # set. The pid is all a kill and a reap need, and the child cannot be
        # doing anything worth keeping — it is still waiting on the bootstrap
        # it will never receive.
        killed = False
        try:
            os.kill(pid, signal.SIGKILL)
            killed = True
        except Exception:
            pass
        # Reaped only when the kill took: waitpid(pid, 0) blocks, and blocking
        # on a child the kill did NOT reach waits on a bootstrap that will
        # never finish. A child whose kill failed leaks instead — the bounded
        # loss, and the README's unbounded-reap limit covers the rest.
        if killed:
            try:
                os.waitpid(pid, 0)
            except Exception:
                pass
        return
    try:
        process_obj._popen = popen
        process_obj._sentinel = popen.sentinel
        multiprocessing.process._children.add(process_obj)  # type: ignore[attr-defined]
    except Exception:
        pass


class _RecoverLaunch:
    """Mixin publishing the child when the launch fails after creating it.

    Module level, not a closure, because spawn pickles the Process object and
    everything on it — including this launcher, which travels by reference.
    """

    def __init__(self, process_obj):
        try:
            super().__init__(process_obj)  # type: ignore[misc]
        except BaseException:
            _publish_failed_launch(self, process_obj)
            raise


class _RecoverableSpawnPopen(_RecoverLaunch, _StdlibSpawnPopen):  # type: ignore[misc,valid-type]
    pass


class _RecoverableForkPopen(_RecoverLaunch, _StdlibForkPopen):  # type: ignore[misc,valid-type]
    pass


def _recoverable_launchers() -> dict:
    """Stdlib launcher -> ours, for the ones whose ordering makes recovery work.

    forkserver is absent on purpose: it reads the pid LAST, after the request
    that already created the child, so a failure before that leaves a child we
    can neither name nor signal. The README says so.
    """
    if _StdlibSpawnPopen is object:
        return {}
    mapping = {}
    with contextlib.suppress(Exception):
        mapping[multiprocessing.context.SpawnProcess._Popen] = _RecoverableSpawnPopen
        mapping[multiprocessing.context.ForkProcess._Popen] = _RecoverableForkPopen
    return mapping


_RECOVERABLE_LAUNCHERS = _recoverable_launchers()


def _launch_recoverably(process: Any) -> None:
    """Install a recovering launcher when this Process uses a stdlib one.

    Keyed on the launcher itself, not on a reported start method: a custom
    context may answer "spawn" while launching its own way — passing
    descriptors, or doing setup of its own — and replacing that would start a
    child without it.
    """
    ours = _RECOVERABLE_LAUNCHERS.get(getattr(type(process), "_Popen", None))
    if ours is None:
        return
    with contextlib.suppress(Exception):
        # An instance attribute: BaseProcess.start() calls ``self._Popen(self)``.
        process._Popen = ours


def _detached(fn: Callable[[], Any]) -> None:
    """Run *fn* where a caller's enclosing ``gevent.Timeout`` cannot interrupt
    it — inline if the hub refuses a greenlet.

    The greenlet is protection, not a requirement: teardown reaches switch
    points, and a timeout landing in one strands whatever the caller had already
    detached from ``self``. A spawn that fails is worse than an interruptible
    cleanup, since nothing else will ever come back for it.
    """
    try:
        runner = gevent.spawn(fn)
    except KeyboardInterrupt:
        # The operator's, landing in the spawn. It still goes on — after the
        # cleanup, which is what nothing else would come back for.
        fn()
        raise
    except BaseException:  # noqa: BLE001
        fn()
        return
    runner.join()


def _forget_reaped(process: Any) -> None:
    """Release a child multiprocessing can no longer reap.

    ``join()`` drops a process from multiprocessing's own ``_children`` set only
    when ``wait()`` returns a status, and after libev has stolen the reap
    ``waitpid`` fails with ECHILD so ``poll()`` answers None for ever — the same
    lie :func:`_proc_exited` exists to see through. Measured over three killed
    children: each left its Process in that set and leaked two descriptors, and
    at interpreter exit multiprocessing's own atexit hook terminates the pid it
    still believes is running, which by then may belong to somebody else.

    Private attributes, knowingly: multiprocessing offers no public way to say
    "this one is gone, somebody else reaped it". Every step is suppressed, since
    this runs in teardown paths whose real diagnostic is elsewhere.

    Gated on the sentinel, because ``returncode is None`` says both "somebody
    else reaped it" and "it is still running" — and a child that outlived even
    the SIGKILL grace, in uninterruptible I/O or simply descheduled, would
    otherwise have its descriptors closed and its tracking dropped while alive,
    with nothing left to reap it.
    """
    if not _proc_exited(process):
        # It outlived even the kill — uninterruptible I/O, or simply
        # descheduled. Wait a moment rather than deciding on the spot: a child
        # that exits just after this check, with libev stealing its reap, would
        # otherwise stay in _children with its descriptors open and no later
        # path to release it. Still there afterwards, it keeps its tracking,
        # which is what a live child needs.
        try:
            multiprocessing.connection.wait([process.sentinel], timeout=1.0)
        except Exception:
            pass
        if not _proc_exited(process):
            return
    # Reaped here if nobody else has: the sentinel says the child is gone, and
    # "gone" is not "reaped" — with no waitpid called it is a zombie, and
    # discarding it below would leave it one until this process exits. join()
    # removes it from _children itself when it succeeds, so whatever is still
    # tracked afterwards really was somebody else's reap.
    try:
        process.join(timeout=0)
    except Exception:
        pass
    try:
        popen = process._popen
        if popen is not None and popen.returncode is None:
            # Closes the sentinel pipe; util.Finalize makes it idempotent.
            finalizer = getattr(popen, "finalizer", None)
            if finalizer is not None:
                finalizer()
    except Exception:
        pass
    try:
        multiprocessing.process._children.discard(process)  # type: ignore[attr-defined]
    except Exception:
        pass


def _attached_client_factory() -> Any:
    raise RuntimeError("an attached proxy does not build a client; the host does")


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
    # Set by attach(): the address of a worker this proxy talks to but does not
    # own. None means the usual thing — spawn a child at a private address.
    _attach_address: str | None = None

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
        self._starting = False
        self._process: Any = None
        self._reader: gevent.Greenlet | None = None
        self._transport: _internal.ZmqTransport | None = None
        # Held apart from the transport, because it is a different obligation:
        # this proxy CONNECTS, its child binds. The address is private to that
        # child, so there is no inode of ours to match — and the file may only
        # go once the child is dead, or it binds one we have already removed.
        self._lease = _internal.IpcLease.none()
        # Ownership is fixed at construction, never inferred from live state:
        # `_process is None` is also true for an owner mid-restart, and the two
        # cases differ in what teardown is allowed to touch.
        self._owns_worker = type(self)._attach_address is None
        hub.ensure_hub_started()
        # Recorded here and never again: this is the thread that owns the hub
        # the socket lives on, which does not change for the life of the proxy.
        # _start used to re-record it, and _start does not always run where it
        # thinks — under patch_all its marshal guard does not fire for a raw OS
        # thread, so a restart from one made that thread the owner, and every
        # later call from it then sent on the socket directly, against the
        # reader greenlet that owns it.
        self._owner = _internal.current_thread()
        self._start()

    # --- Lifecycle ---

    def _start(self):
        """Start the child process if not running. Marshals to the owning thread.

        The predicate is thread identity, and the body is a separate method,
        for reasons that are all measurable under ``patch_all()``:

        - ``gevent.get_hub()`` hands a raw OS thread the MAIN hub, and its
          ``loop.default`` is True there, so neither can tell a foreign thread
          from the owner. The guard this replaces used ``loop.default`` and
          therefore never fired: ``_stop``/``_start`` ran inline on whatever
          thread called them, spawning greenlets and sending on the socket the
          reader owns.
        - ``get_original("threading", "get_ident")`` returns the same value on
          every thread, so it cannot stand in either.
        - ``current_thread()`` does tell a foreign thread apart — but it is per
          GREENLET, not per thread: a greenlet spawned on the owning thread
          reports a ``_DummyThread``. It is therefore conservative, marshalling
          more often than strictly needed, which is safe. What it cannot do is
          answer twice: the greenlet a marshal spawns would test again, decide
          it is foreign, and marshal itself forever. Hence the split — the
          marshal target is the body, which never re-tests.

        Unbounded here on purpose: ``__init__`` calls this on the owner, and a
        foreign-thread restart has already crossed a bounded marshal to reach it.
        """
        if _internal.current_thread() is not self._owner:
            return hub.run_on_main_hub(self._start_on_owner)
        self._start_on_owner()

    def _start_on_owner(self):
        """``_start``'s body. Runs only on the owning thread — see ``_start``."""
        import zmq.green as zmq_green

        with self._lock:
            # The transport, not the process, says whether we are up: an
            # attached proxy has no process and still has everything it owns.
            # ``_starting`` covers the gap the transport alone no longer does:
            # nothing is published until the whole start succeeds, and _lock is
            # a native RLock, so a second greenlet on this thread re-enters it
            # freely and would spawn a second child while the first is in
            # start().
            if self._shutdown or self._transport is not None or self._starting:
                return

            cls = type(self)
            transport = process = None
            lease = _internal.IpcLease.none()

            # Everything acquired so far, released. Defined before the try
            # because both failure paths below use it — the one where nothing
            # was published, and the one where the reader's spawn refused after
            # publishing — and it reads the names as they stand when it runs.
            #
            # Not gated on the socket: a start that fails IN ctx.socket() has a
            # live context and no socket, and skipping the close there left the
            # context — with its IO thread — to whoever holds the traceback.
            def _undo():
                # Nested finallys, because transport.close() can raise out of
                # term()'s switch point — and sequential, what it skipped was
                # the child cleanup and the last handle anyone had on both.
                try:
                    try:
                        if transport is not None:
                            transport.close()
                    finally:
                        # On the handle, not on a flag set after start()
                        # returned: start() can fail AFTER creating the child,
                        # and _launch_recoverably is what leaves that handle
                        # behind. A Process that never launched has none, which
                        # is the join()-asserts case the flag used to stand in
                        # for.
                        if getattr(process, "_popen", None) is not None:
                            self._cleanup_process(process)
                finally:
                    # Released whether or not a handle survived. The child is
                    # dead by now either way — cleaned up above, or killed and
                    # reaped where the launcher had no sentinel to publish — and
                    # it may have bound the address before dying, which a fork
                    # child does immediately. The address is ours alone, a uuid
                    # in a per-uid directory, so this is the last chance anyone
                    # has to remove it.
                    lease.release()

            try:
                # Inside, so the finally below really does cover every path that
                # sets it: an allocation failing between the guard and here left
                # the flag claimed for good, and every later _start() returned
                # at the guard.
                self._starting = True
                addr = (
                    f"ipc://{_ZMQ_TMPDIR}/gi-{uuid.uuid4().hex[:16]}.sock"
                    if cls._attach_address is None
                    else cls._attach_address
                )
                transport = _internal.ZmqTransport.open(
                    zmq_green.Context, zmq.DEALER, addr, bind=False
                )
                if self._owns_worker:
                    # The address is this proxy's to clear, but only once the
                    # child that binds it is dead — hence a lease of its own
                    # rather than the transport's, which connects.
                    lease = _internal.IpcLease.private(addr)
                    config = _workers.WorkerConfig(
                        ipc_addr=addr,
                        factory_bytes=dill.dumps(cls.client_factory),
                        max_concurrency=cls.max_concurrency,
                    )
                    worker, args = (
                        (_workers.gevent_worker, (config, cls.patch_kwargs))
                        if cls.patch_kwargs is not None
                        else (_workers.asyncio_worker, (config,))
                    )
                    mp_ctx = cls.mp_context or get_default_mp_context()
                    process = mp_ctx.Process(
                        target=worker, daemon=cls.daemon, args=args
                    )
                    _launch_recoverably(process)
                    with _internal.suppress_main_reimport():
                        process.start()
                    log.info(f"ProcessProxy started: pid={process.pid}, ctx={mp_ctx}")
                else:
                    log.info(f"ProcessProxy attached: {addr}")
                if self._shutdown:
                    # shutdown() ran while we were inside process.start(). _lock
                    # is reentrant on this thread, so its _stop() got in — and
                    # found nothing published to tear down. Publishing now would
                    # hand a shut-down proxy a live child and transport.
                    raise _internal.ProcessError("Proxy was shut down while starting")
            except BaseException:
                # Everything fallible is inside, because the transport exists
                # before it: dill'ing the factory, spawning the child, even
                # formatting the log line above. An __init__ that raises returns
                # no proxy to call cleanup on, and __del__ gets there only once
                # the caller drops the traceback holding this frame — and
                # `self` with it, and the child it started.
                #
                # On its own greenlet, for _stop's reason: _cleanup_process's
                # join()s are switch points, so a caller's enclosing
                # gevent.Timeout can land inside them — and here nothing was
                # published, so an interrupted cleanup strands a live child and
                # its socket file with no handle left to reach either.
                _detached(_undo)
                raise
            finally:
                self._starting = False
            # One boundary: a proxy is either fully up or untouched. Publishing
            # the child earlier left a failed start with a live _process and no
            # transport — _is_alive() true, every call failing in _raw_send.
            self._transport, self._lease = transport, lease
            self._process = process
            try:
                self._reader = gevent.spawn(self._read_loop)
            except BaseException:
                # A spawn can refuse — allocation, or a hub in teardown — and by
                # here the child and the transport ARE published, so the
                # rollback has to unpublish before it releases: __init__ raises,
                # the caller gets no proxy, and a live child with no reader is
                # what would be left. The asyncio starts elsewhere in the
                # package have had this; the gevent ones had not.
                # The empty lease is built BEFORE the stores: it is the one
                # allocation here, and raising between detaching the fields and
                # _undo left everything reachable from nothing.
                cleared = _internal.IpcLease.none()
                self._transport = self._process = self._reader = None
                self._lease = cleared
                _detached(_undo)
                raise
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
        # Defined before anything is detached, not in the finally below: the
        # function object and its closure cells are allocations, and building
        # them after the swaps meant a refusal left the child, socket and
        # lease detached from the proxy, reachable from nothing. The names it
        # closes over are read when it RUNS, after the swaps bind them.
        def _teardown():
            # In a finally, because this runs detached: an exception from
            # the child cleanup — terminate() racing the child's own exit is
            # the ordinary one — took the rest of the teardown with it, and
            # join() on a spawned greenlet does not even report it. What was
            # skipped is the socket file and every caller still waiting.
            try:
                try:
                    if transport is not None:
                        transport.close()
                finally:
                    # Nested, not sequential: a BaseException out of the
                    # transport close — term() is a switch point — skipped the
                    # child cleanup while the finally below released its
                    # address anyway.
                    if process is not None:
                        self._cleanup_process(process)
            finally:
                # After the child is gone, and only for a worker we own: an
                # attached proxy holds no lease, so a host's socket — which
                # other clients are still using — is never ours to remove.
                # Nested: release re-raises the operator's interrupt, and the
                # waiters below must still learn the process is gone. Drained
                # destructively — pop allocates nothing, where an iterator is
                # a refusal that left every waiter running out its timeout.
                try:
                    lease.release()
                finally:
                    while pending:
                        pending.pop().set_exception(stop_error)

        with self._lock:
            if self._process is None and self._transport is None:
                return
            # Every allocation first — the list, the empty lease, and the
            # error the waiters get. Taking any of them after the swaps meant
            # a failure left a live child and a bound socket detached from the
            # proxy, reachable from nothing.
            pending = list(self._pending.values())
            cleared = _internal.IpcLease.none()
            stop_error = error or _internal.ProcessError("Process stopped")
            self._pending.clear()
            reader, self._reader = self._reader, None
            process, self._process = self._process, None
            transport, self._transport = self._transport, None
            lease, self._lease = self._lease, cleared

        try:
            if reader and reader is not gevent.getcurrent():
                # kill(block=True, timeout=...) never raises its own timeout
                # (gevent's join identity-checks its internal timer), so a
                # gevent.Timeout here can only be a caller's enclosing
                # deadline — it must propagate, not be suppressed.
                reader.kill(block=True, timeout=timeout)

            # Only the owner may end the worker: the frame makes it exit, and an
            # attached proxy shares that worker with every other process attached
            # to it — one client leaving would take it down for all of them.
            if transport is not None and self._owns_worker:
                # Best-effort SHUTDOWN, sent before the socket closes so the
                # worker can drain + close its client (close(linger=0) would
                # drop an unsent frame). NOBLOCK: a blocking send would hang
                # the main hub if the DEALER is backpressured; a full send
                # queue means the child is already stuck and terminate/kill
                # below handles it.
                try:
                    transport.sock.send_multipart(
                        [b"0", _workers.SHUTDOWN], zmq.NOBLOCK
                    )
                except zmq.ZMQError:
                    pass
            if graceful and process is not None:
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
            _detached(_teardown)

    def _cleanup_process(self, process) -> None:
        # Every step guarded separately, and against Exception, not just
        # OSError: terminate() racing the child's exit raises OSError, but a
        # custom mp_context — which is supported — can raise anything, and
        # whichever step fails, the escalation after it is what actually ends
        # a live child. The bookkeeping runs regardless: skipping
        # _forget_reaped leaked the Process and its descriptors once per
        # restart, with nothing left that could retry.
        try:
            try:
                process.join(timeout=0.3)
            except Exception:
                pass
            # Gate signaling on the sentinel, not is_alive(): after a stolen
            # reap (see _proc_exited) is_alive() stays True and the pid may
            # already be recycled — terminate/SIGKILL would hit an innocent
            # process.
            if not _proc_exited(process):
                try:
                    process.terminate()
                except Exception:
                    pass
                try:
                    process.join(timeout=0.5)
                except Exception:
                    pass
                if not _proc_exited(process) and process.pid:
                    log.warning("Process did not terminate, sending SIGKILL")
                    try:
                        os.kill(process.pid, signal.SIGKILL)
                    except OSError:
                        pass
                    try:
                        process.join(timeout=0.5)
                    except Exception:
                        pass
        finally:
            _forget_reaped(process)

    def _is_alive(self) -> bool:
        if not self._owns_worker:
            # Not remote liveness — that is not ours to read, and a host may be
            # restarted under us without our help (the DEALER reconnects on its
            # own; a host that is simply gone surfaces as an RPC timeout). What
            # this answers is whether our LOCAL transport can still complete a
            # call: an open socket whose reader greenlet died never will.
            reader = self._reader
            return (
                self._transport is not None
                and reader is not None
                and not reader.dead
            )
        # Single read: a concurrent _stop may null _process between checks.
        p = self._process
        return p is not None and not _proc_exited(p)

    def restart_process(self) -> None:
        """Kill and restart the child process. Thread-safe (marshals to main hub).

        An attached proxy owns no process: this rebuilds its socket and reader
        and leaves the host serving.
        """
        # Bounded, unlike _start's: a wedged hub cannot be fixed by waiting on
        # it, and an unbounded marshal here outlives every deadline the caller
        # set — _ensure_running reaches this from inside a call that has one.
        # WaitTimeout is a TimeoutError, and it escapes before _execute
        # registers a pending request, so it strands no waiter.
        if _internal.current_thread() is not self._owner:
            return hub.run_on_main_hub(self._restart_on_owner, timeout=self.timeout)
        self._restart_on_owner()

    def _restart_on_owner(self) -> None:
        """``restart_process``'s body — see ``_start`` for why it is separate."""
        with self._lock:
            now = time.monotonic()
            # Uniform cooldown, dead child included: a client that crashes
            # during startup would otherwise respawn on every execute.
            # Callers aren't stranded — _ensure_running fails fast on a
            # skipped restart, and _stop flushes pending waiters. Owners only:
            # an attached restart spawns nothing, it rebuilds a socket, so
            # throttling it only strands the client for the cooldown.
            if self._owns_worker and now - self._last_restart < self.restart_cooldown:
                log.warning("Restart skipped (cooldown)")
                return
            self._last_restart = now
        self._stop(timeout=0.5)
        # The body directly: we are already on the owner, and _start's guard is
        # conservative enough to marshal a greenlet that is standing on it.
        self._start_on_owner()

    def shutdown(self, timeout: float = 10.0):
        """Gracefully shutdown child process. Thread-safe."""
        # Same bound as restart_process, and this one names its own: a shutdown
        # that cannot reach the hub must report that rather than hang the thread
        # that asked for it.
        if _internal.current_thread() is not self._owner:
            return hub.run_on_main_hub(
                functools.partial(self._shutdown_on_owner, timeout), timeout=timeout
            )
        self._shutdown_on_owner(timeout)

    def _shutdown_on_owner(self, timeout: float) -> None:
        """``shutdown``'s body — see ``_start`` for why it is separate."""
        self._shutdown = True
        self._stop(timeout=timeout, graceful=True)

    def __del__(self):
        # Minimal cleanup only — avoid full shutdown() which marshals
        # to main hub and may fail during GC or interpreter shutdown.
        # Two suppressions, not one: _proc_exited polls the sentinel, which is
        # a gevent switch point, so a caller's Timeout (a BaseException) can
        # land inside the liveness check — it must not take the socket
        # teardown down with it.
        try:
            process = getattr(self, "_process", None)
            # Same sentinel gate as _cleanup_process: terminate() signals the
            # recorded pid whenever mp thinks the child is unreaped, and after
            # a stolen reap that pid may belong to someone else.
            if process is not None and not _proc_exited(process):
                process.terminate()
        except BaseException:  # noqa: BLE001 — GC context; nothing propagates from __del__
            pass
        # Nested: transport.close() lets a BaseException out of term()'s
        # switch point, and __del__ exiting there — the interpreter swallows
        # it — skipped the lease below, orphaning the child's socket file.
        try:
            if (transport := getattr(self, "_transport", None)) is not None:
                transport.close()
        finally:
            # The lease too, or every proxy that is merely dropped leaves its
            # socket file behind — __del__ is the whole cleanup such a proxy
            # ever gets, and a start that failed after publishing (a reader
            # that could not spawn) reaches nothing else either. release() is
            # idempotent and never raises, so the shutdown path having done it
            # already costs nothing.
            if (lease := getattr(self, "_lease", None)) is not None:
                lease.release()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.shutdown()

    # --- Response Handling ---

    def _read_loop(self):
        # Bound to THIS generation's socket, as pubsub's readers are: _stop's
        # kill is bounded, so a reader unwinding through slow client code can
        # outlive it — and everything below reached for whatever ``self`` held
        # by then, which after the _start that follows is somebody else's.
        transport = self._transport
        if transport is None:
            return
        sock = transport.sock
        try:
            idle_cycles = 0
            while not self._shutdown and transport is self._transport:
                if sock.poll(50) and self._drain(transport):
                    idle_cycles = 0
                else:
                    idle_cycles += 1
                if idle_cycles >= self.alive_check_idle_cycles and not self._is_alive():
                    break
            if not self._shutdown and transport is self._transport:
                log.warning("Worker unreachable, stopping reader")
        except (gevent.GreenletExit, zmq.ZMQError):
            pass
        except Exception as e:
            log.warning(f"Reader greenlet error: {e}", exc_info=True)
        finally:
            # Ours to stop only while it is still ours: a stale reader calling
            # _stop here disconnected the child a later _start had published and
            # failed every call waiting on it.
            if transport is self._transport:
                self._stop(_internal.ProcessError("Child process disconnected"))

    def _drain(self, transport) -> bool:
        """Receive and dispatch all available responses; return True if any received."""
        sock = transport.sock
        received = False
        while True:
            try:
                parts = sock.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                return received
            received = True
            if len(parts) < 3:
                continue
            req_id_bytes, ok_flag, payload = parts[:3]
            req_id = int.from_bytes(req_id_bytes)
            decoded = False
            try:
                result = _internal.SmartPickle.loads(payload)
                ok = ok_flag == _workers.OK
                decoded = True
            except KeyboardInterrupt:
                # The operator's, arriving while a legitimate response is being
                # reconstructed. Turning it into a ProcessError for one caller
                # absorbs Ctrl-C for the whole application.
                raise
            except gevent.GreenletExit:
                # A kill aimed at this reader, not a bad response. _stop injects
                # it and waits (kill blocks by default), and unpickling is a
                # switch point whenever it imports a class this process has not
                # seen — so the kill can land right here. Answering it with an
                # error and carrying on leaves the stopper waiting on a reader
                # that no longer means to exit, and the _start that follows then
                # publishes a second reader over a socket this one still drains.
                raise
            # BaseException otherwise: reconstruction runs client code
            # (__setstate__, a reduce callable), and one reply whose unpickling
            # raises SystemExit would kill this reader — the one shared by every
            # pending call — and _stop would fail all of them.
            except BaseException as e:
                # Never interpolate the exception itself: it came out of that
                # same client code, and a __str__ raising here would escape into
                # the reader we just protected. logging swallows its own
                # formatting failures, so exc_info is safe where an f-string is
                # not.
                log.warning("Failed to deserialize response", exc_info=True)
                result = _internal.ProcessError(
                    f"Bad response: {_internal.type_name(e)}"
                )
                ok = False
            if not ok and (tb := _internal.remote_traceback(result)):
                log.error(f"Remote traceback:\n{tb}")
            with self._lock:
                # A reply that deserialized cleanly is transport evidence,
                # whatever outcome it carries: error replies used to leave the
                # failure streak standing, so send failures spread across weeks
                # of honest errors still restarted a healthy worker. A reply
                # that would NOT deserialize stays counted — that recovery is
                # what the threshold is for. Generation-guarded so a stale
                # reader cannot clear the streak its successor is building.
                if decoded and self._transport is transport:
                    self._error_count = 0
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
        # Stamped first, and shared by everything this call does: the revival
        # below, the marshal, the worker, and the wait. It used to be stamped
        # after _ensure_running, so a revival could spend the proxy's whole
        # timeout before a with_timeout(0.1) had started counting; and the wait
        # used to start its own full clock after _send returned, so a
        # cross-thread call whose marshal took most of the budget then waited
        # that long again.
        deadline = time.monotonic() + rpc_timeout
        self._ensure_running(deadline)

        req_id = next(self._counter) & _workers.ID_MASK
        is_owner = _internal.current_thread() is self._owner
        ar = gevent.event.AsyncResult() if is_owner else hub.AsyncResult()
        with self._lock:
            self._pending[req_id] = ar
            # The generation this call belongs to. Failures are counted against
            # it, and a restart is aimed at it, so neither can act on whatever
            # replaced it in the meantime.
            generation = self._transport

        try:
            if err := self._send(
                req_id, method, args, kwargs, deadline, is_owner
            ):
                if isinstance(err, _internal.ProcessError):
                    raise err
                self._revive(generation, deadline)
                raise _internal.ProcessError("Failed to send request to child") from err

            # What is left of the budget, plus the grace the worker's own reply
            # needs to reach us.
            wait_timeout = max(deadline - time.monotonic(), 0.0) + max(
                2.0, rpc_timeout * 0.1
            )
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
            # The streak reset lives in _drain, at delivery: the reader is what
            # KNOWS a reply arrived, and it resets for error replies too.
            return result
        except _internal.ProcessError as e:
            # A ProcessError the WORKER raised — a nested proxy's, say — is the
            # call's answer, not evidence about our transport. Only the worker
            # attaches a remote traceback, so that is what tells them apart; six
            # honest ones in a row used to restart a perfectly healthy worker.
            if _internal.remote_traceback(e) is not None:
                raise
            with self._lock:
                # Only against the generation this call actually used. A
                # restart fails every pending call at once, and counting those
                # crossed the threshold — so the callers of a healthy new
                # generation restarted it again, and for an attached proxy
                # nothing throttled that.
                if self._transport is not generation:
                    raise
                self._error_count += 1
                should_restart = self._error_count >= self.auto_restart_threshold
            if should_restart:
                log.warning(f"{self._error_count} consecutive errors, restarting")
                self._revive(generation, deadline)
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
        deadline: float,
        is_owner: bool,
    ) -> Exception | None:
        # An absolute deadline, not a relative budget: the request may sit in
        # the DEALER's queue arbitrarily long before any worker receives it
        # (attach() connects to an address nobody has bound yet), and a worker
        # that re-based the budget at receipt would run a call whose caller gave
        # up long ago. Same-host monotonic clock, which ipc:// guarantees.
        #
        # Serialized OUTSIDE the try, because a payload we cannot even build
        # never reached the socket and says nothing about the child's health.
        # Reported as a send failure, one caller's unpicklable argument
        # restarted a healthy worker — losing its client's in-memory state and
        # failing every unrelated call in flight.
        payload = _internal.SmartPickle.dumps((method, args, kwargs, deadline))
        frames = [req_id.to_bytes(8), payload]
        try:
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
                        timeout=max(deadline - time.monotonic(), 0.0),
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
            if self._transport is None:
                raise _internal.ProcessError("Process not running")
            try:
                self._transport.sock.send_multipart(frames, zmq.NOBLOCK)
            except zmq.Again as e:
                # A full outgoing queue is this call's failure, not the
                # worker's. Reported as a transport failure it took the restart
                # path, which fails every OTHER pending call too — so a peer
                # that is merely slow, or an attached host that is restarting,
                # cost every caller its request once the HWM was reached.
                # ProcessError instead: _execute counts it, and
                # auto_restart_threshold decides when enough of them mean the
                # worker is really gone.
                raise _internal.ProcessError(
                    "Send queue full; worker is not consuming"
                ) from e

    def _revive(self, stale: Any, deadline: float) -> None:
        """Restart, unless somebody already replaced what we found dead.

        Two callers that saw the same dead transport both arrive here, and the
        second would otherwise tear down the generation the first just built —
        failing that caller's request, which may already have gone out. The
        owner's cooldown does not cover this: an attached proxy skips it on
        purpose, because rebuilding a socket strands nobody.

        Bounded by the CALL's deadline, not the proxy's: this runs inside
        somebody's ``with_timeout(0.1)``, and the marshal's own default would
        let a wedged hub spend the proxy's whole timeout before that 0.1s had
        even started.
        """
        if _internal.current_thread() is not self._owner:
            # The BODY is the marshal target, never this method: current_thread
            # is per greenlet, so the greenlet a marshal spawns tests again,
            # decides it is foreign, and marshals itself forever. Measured at 21
            # hops and climbing. Same split as _start's, and for the same
            # reason.
            return hub.run_on_main_hub(
                functools.partial(self._revive_on_owner, stale),
                timeout=max(deadline - time.monotonic(), 0.0),
            )
        self._revive_on_owner(stale)

    def _revive_on_owner(self, stale: Any) -> None:
        """``_revive``'s body — see ``_start`` for why it is separate."""
        if self._transport is not stale:
            return
        # The deadline bounds the marshal above — a queue we do not control —
        # and not this. Rebuilding the worker is shared work: interrupting it
        # because one caller is impatient would leave the proxy down for
        # everybody, so a call that triggers a revival pays for the spawn.
        # ``restart_cooldown`` is what bounds how often an owner pays it.
        self._restart_on_owner()

    def _ensure_running(self, deadline: float) -> None:
        if self._shutdown:
            raise RuntimeError("Proxy is shutdown")
        # Read before the liveness check, so the revival below can tell whether
        # it is still the generation we found dead.
        stale = self._transport
        if not self._is_alive():
            self._revive(stale, deadline)
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
        # Any callable, per the signature: functools.partial is the obvious way
        # to bind a factory's arguments and has no __qualname__ at all.
        name = getattr(factory, "__qualname__", None) or type(factory).__name__
        klass = type(f"ProcessProxy<{name}>", (cls,), ns)
        return klass()  # type: ignore[return-value]

    @classmethod
    def attach(cls, address: str, *, timeout: float = 24) -> "ProcessProxy":
        """Proxy a worker this process does not own — see :func:`gisolate.serve`.

        Nothing is spawned: the proxy connects to *address* and shares that
        worker with every other process attached to it, so an isolated library is
        resident once per host instead of once per client. The host owns the
        lifecycle — ``shutdown()`` here closes this client's sockets and leaves
        the worker serving.

        Attaching is asynchronous and proves nothing about the far end: ZMQ
        connects to an address nobody has bound yet just as happily, a host that
        restarts is picked up again by the DEALER's own reconnect, and a host
        that is simply absent surfaces as an RPC timeout rather than as a failure
        here. Like any ZMQ socket the proxy is process-local, so a forking server
        must attach AFTER the fork, once per worker.

        Args:
            address: the ``ipc://`` address the host bound — same host only, for
                the reasons :func:`gisolate.serve` documents.
            timeout: Per-call timeout in seconds.
        """
        _internal.require_ipc(address, "ProcessProxy.attach()")
        klass = type(
            "ProcessProxy<attached>",
            (cls,),
            {
                # Never called: the host built the client. Present only because
                # the base class declares it abstract.
                "client_factory": staticmethod(_attached_client_factory),
                "timeout": timeout,
                "_attach_address": address,
            },
        )
        return klass()


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
