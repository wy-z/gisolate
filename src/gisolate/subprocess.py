"""Run a function in an isolated subprocess with gevent-safe polling."""

import logging
import time
from typing import Any, Callable

import gevent

from .proxy import get_default_mp_context

log = logging.getLogger(__name__)

_EMPTY = object()


def _worker(conn: Any, fn: Callable, fn_args: tuple, fn_kwargs: dict) -> None:
    """Worker entry point for run_in_subprocess."""
    from ._internal import wrap_exception

    try:
        conn.send(("ok", fn(*fn_args, **fn_kwargs)))
    except Exception as e:
        import traceback

        conn.send(("error", wrap_exception(e, traceback.format_exc())))
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
    mp = mp_context or get_default_mp_context()
    parent_conn, child_conn = mp.Pipe()
    proc = mp.Process(
        target=_worker,
        args=(child_conn, target, args, kwargs or {}),
        daemon=False,
    )
    proc.start()
    child_conn.close()

    def try_recv() -> Any:
        if not parent_conn.poll(0):
            return _EMPTY
        try:
            msg = parent_conn.recv()
        except EOFError:
            return _EMPTY
        match msg:
            case ("ok", result):
                return result
            case ("error", exc):
                if tb := getattr(exc, "__remote_traceback__", None):
                    log.error(f"Subprocess traceback:\n{tb}")
                raise exc
            case _:
                raise RuntimeError(f"Malformed subprocess message: {msg!r}")

    def cleanup():
        parent_conn.close()
        if proc.is_alive():
            proc.terminate()
        proc.join(timeout=2)
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=1)

    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            if (result := try_recv()) is not _EMPTY:
                return result
            if not proc.is_alive():
                if (result := try_recv()) is not _EMPTY:
                    return result
                raise RuntimeError(f"Subprocess exited with code {proc.exitcode}")
            gevent.sleep(poll_interval)
        raise TimeoutError(f"Subprocess timed out after {timeout}s")
    finally:
        cleanup()
