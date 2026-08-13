"""Run a function in an isolated subprocess with gevent-safe polling."""

import logging
import os
import time
from typing import Any, Callable

import gevent

from . import _internal, proxy

log = logging.getLogger(__name__)

_EMPTY = object()

# The reply that cannot fail to be one — see _worker.
_LAST_RESORT = _internal.SmartPickle.dumps(
    ("error", _internal.RemoteError("UnknownError: <unserializable>", "UnknownError"))
)


def _make_pipe(mp: Any) -> tuple[Any, Any]:
    """Create a Pipe with blocking fd semantics.

    gevent's socket monkey patch can leave ``multiprocessing.Pipe()`` backed by
    non-blocking ``socketpair()`` fds. ``Connection.send_bytes()`` expects a
    blocking fd and may raise ``BlockingIOError`` under load if ``O_NONBLOCK``
    leaks through.
    """
    parent_conn, child_conn = mp.Pipe()
    try:
        for conn in (parent_conn, child_conn):
            os.set_blocking(conn.fileno(), True)
        return parent_conn, child_conn
    except BaseException:
        # set_blocking can refuse — and a caller keeping the traceback keeps
        # both descriptors with it. Nested, so the first close failing cannot
        # skip the second. Same rollback the Process construction below has.
        try:
            parent_conn.close()
        finally:
            child_conn.close()
        raise


def _worker(conn: Any, fn: Callable, fn_args: tuple, fn_kwargs: dict) -> None:
    """Worker entry point for run_in_subprocess."""
    try:
        conn.send_bytes(_internal.SmartPickle.dumps(("ok", fn(*fn_args, **fn_kwargs))))
    # BaseException, because a target's own expiring gevent.Timeout is one and
    # the caller is owed it: uncaught, the child dies with nothing sent and the
    # error becomes "Subprocess exited with code 1". Nothing kills this
    # greenlet — the process is one-shot and a terminate arrives as a signal —
    # so there is no cancellation here to swallow.
    except BaseException as e:
        import traceback

        err = _internal.wrap_exception(e, traceback.format_exc())
        try:
            reply = _internal.SmartPickle.dumps(("error", err))
        except BaseException:  # noqa: BLE001
            # wrap_exception proves the error pickles ONCE — its probe — and
            # this is a second call. Escaping here, the child died with nothing
            # sent and the caller was told a process had exited, which is not
            # the failure it was owed. Serialized at import, so the answer to a
            # failed serialization needs none of its own.
            reply = _LAST_RESORT
        conn.send_bytes(reply)
    finally:
        conn.close()


def run_in_subprocess(
    target: Callable,
    args: tuple = (),
    kwargs: dict | None = None,
    *,
    timeout: float = 3600,
    mp_context: Any = None,
    poll_interval: float = 0.1,
    daemon: bool = True,
) -> Any:
    """Run target function in subprocess with non-blocking polling (gevent-safe).

    Args:
        target: Function to run in subprocess (must be picklable).
        args: Positional arguments for target.
        kwargs: Keyword arguments for target.
        timeout: Maximum time to wait for result in seconds.
        mp_context: Multiprocessing context (default: configured or spawn).
        poll_interval: Seconds between status polls.

    Returns:
        Result from target function.

    Raises:
        TimeoutError: If process doesn't complete within timeout.
        RuntimeError: If process exits without producing a result.
        Exception: Any exception raised by target function.
    """
    mp = mp_context or proxy.get_default_mp_context()
    # Stamped before the spawn, not after: under spawn, Process.start() pickles
    # the target and its arguments inline, which for a large argument is real
    # time the caller's budget was not being charged for. It does not BOUND
    # start() — that is synchronous and has no interruption point — but a call
    # that overran while starting now reports the timeout instead of returning
    # late as though nothing had happened.
    deadline = time.monotonic() + timeout

    # Both function objects are built before anything they clean up or read
    # exists: a def is an allocation, and building cleanup after the pipe and
    # the Process meant the refusal stranded both outside every cleanup path.
    # The names they close over are read when they RUN, which is after the
    # acquisitions below bind them.
    def cleanup():
        # Every step guarded separately, the shape _cleanup_process has: this
        # runs detached, and any one step raising — terminate() racing the
        # child's own exit, a close on a broken pipe — used to skip the
        # escalation after it and the bookkeeping below.
        try:
            try:
                parent_conn.close()
            except Exception:
                pass
            try:
                child_conn.close()
            except Exception:
                pass
            # Exception, not just OSError: a custom mp_context — which is
            # supported — can raise anything from these, and each failure's
            # answer is the escalation after it, not the skip.
            if not proxy._proc_exited(proc):
                try:
                    proc.terminate()
                except Exception:
                    pass
            try:
                proc.join(timeout=2)
            except Exception:
                pass
            if not proxy._proc_exited(proc):
                try:
                    proc.kill()
                except Exception:
                    pass
                try:
                    proc.join(timeout=1)
                except Exception:
                    pass
        finally:
            # join() cannot drop a child whose reap was stolen, so without this
            # each call leaves its Process and two descriptors behind — see the
            # helper.
            proxy._forget_reaped(proc)

    def try_recv() -> Any:
        if not parent_conn.poll(0):
            return _EMPTY
        try:
            # On the hub's threadpool, because poll(0) promises a byte and not
            # a whole frame: recv_bytes then blocks until the last one arrives,
            # on fds _make_pipe deliberately leaves blocking. Measured at
            # roughly 1.5ms per MB — a 512MB result stopped every greenlet in
            # this process for 773ms, and took the timeout below with it, which
            # is the one thing a module named for gevent-safe polling must not
            # do. The deserialize that follows is CPU and has no such excuse.
            #
            # Under our own deadline, because apply() has none of its own: it
            # waits on an untimed semaphore for a pool slot, so a threadpool
            # saturated by somebody else's blocking work would hold the promise
            # below open forever. Raised as the TimeoutError this function
            # documents, so the caller sees one answer either way. ``deadline``
            # is read at call time on purpose: the final grace read rebinds it.
            with gevent.Timeout(
                max(deadline - time.monotonic(), 0.0),
                TimeoutError(f"Subprocess timed out after {timeout}s"),
            ):
                raw = gevent.get_hub().threadpool.apply(parent_conn.recv_bytes)
        except EOFError:
            return _EMPTY
        try:
            msg = _internal.SmartPickle.loads(raw)
        except KeyboardInterrupt:
            # The operator's, landing while a legitimate result is rebuilt.
            raise
        except BaseException as e:
            # The child serialized it; THIS process reconstructs it, running the
            # target's own code — a __setstate__, a reduce callable. Unguarded,
            # a SystemExit from there came out of run_in_subprocess raw, past
            # every ordinary `except Exception` the caller has. `from None`
            # because the cause is that same object, and rendering it would run
            # its __str__ in whatever prints the traceback.
            log.warning("Failed to deserialize subprocess result", exc_info=True)
            raise _internal.ProcessError(
                f"Bad result: {_internal.type_name(e)}"
            ) from None
        match msg:
            case ("ok", result):
                return result
            case ("error", exc):
                if tb := _internal.remote_traceback(exc):
                    log.error(f"Subprocess traceback:\n{tb}")
                raise exc
            case _:
                raise RuntimeError(f"Malformed subprocess message: {msg!r}")

    parent_conn, child_conn = _make_pipe(mp)
    try:
        # Inside, because construction can fail too — a context refusing the
        # configuration, or resource exhaustion — and both ends of the pipe are
        # already open by now. Left to the caller's traceback they stay that way.
        proc = mp.Process(
            target=_worker,
            args=(child_conn, target, args, kwargs or {}),
            daemon=daemon,
        )
    except BaseException:
        try:
            parent_conn.close()
        finally:
            child_conn.close()
        raise

    proxy._launch_recoverably(proc)
    try:
        with _internal.suppress_main_reimport():
            proc.start()
    except BaseException:
        # start() raises on its own — an unpicklable target under spawn is the
        # usual way — and the cleanup below never runs, so both ends of the pipe
        # stay open for as long as the caller keeps the traceback. Repeat that
        # and the process runs out of descriptors.
        #
        # start() can also fail AFTER creating the child: _launch_recoverably
        # leaves the handle that makes it reachable, and nothing else ever
        # would. That branch goes through the same cleanup the successful path
        # uses — it closes both pipe ends itself, each step guarded, so a
        # close that raises cannot skip the kill after it, which is exactly
        # what the sequential closes here used to do.
        if getattr(proc, "_popen", None) is not None:
            proxy._detached(cleanup)
        else:
            # No child: cleanup would trip over a Process that never started
            # (join asserts on it), so only the pipes need closing.
            try:
                parent_conn.close()
            finally:
                child_conn.close()
        raise
    try:
        # Inside the try: this close failing must still reach the cleanup —
        # which closes it again, idempotently, alongside everything else.
        child_conn.close()
        while time.monotonic() < deadline:
            if (result := try_recv()) is not _EMPTY:
                return result
            # Sentinel, not is_alive(): under a stolen reap (proxy._proc_exited)
            # a crashed target burns the whole timeout — an hour by default —
            # and surfaces as TimeoutError instead of its real exit.
            if proxy._proc_exited(proc):
                if (result := try_recv()) is not _EMPTY:
                    return result
                raise RuntimeError(f"Subprocess exited with code {proc.exitcode}")
            # Capped by what is left: a poll_interval larger than the timeout
            # would sleep past the deadline and then report a timeout for a
            # result that had already arrived.
            gevent.sleep(min(poll_interval, max(deadline - time.monotonic(), 0.0)))
        # One last look before giving up: the result may have landed during the
        # final sleep, and reporting a timeout for a value already in the pipe
        # is a lie. A second of grace for reading it, which is what bounds the
        # overshoot — the read itself is what the deadline could not cover.
        deadline = time.monotonic() + 1.0
        if (result := try_recv()) is not _EMPTY:
            return result
        raise TimeoutError(f"Subprocess timed out after {timeout}s")
    finally:
        # On its own greenlet: proc.join() is a switch point, so an enclosing
        # gevent.Timeout landing between terminate() and kill() would leave a
        # child that ignored SIGTERM running, reachable only through the
        # multiprocessing state this frame is about to drop.
        proxy._detached(cleanup)
