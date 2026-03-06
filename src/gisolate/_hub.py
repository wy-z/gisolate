"""Main hub: marshal tasks to the gevent default event loop."""

import atexit
import contextlib
import functools
import logging
from typing import Any, Callable

import gevent

from . import _internal

log = logging.getLogger(__name__)


class AsyncResult:
    """Thread-safe result container compatible with gevent.event.AsyncResult.

    Uses unpatched threading.Event for true cross-thread signaling,
    even in monkey-patched environments.
    """

    __slots__ = ("_event", "_ok", "_value")

    def __init__(self):
        self._event = _internal.Event()
        self._ok = self._value = None

    def set(self, value):
        self._ok, self._value = True, value
        self._event.set()

    def set_exception(self, exc):
        self._ok, self._value = False, exc
        self._event.set()

    def get(self, timeout=None):
        if not self._event.wait(timeout):
            raise TimeoutError(f"Timed out after {timeout}s")
        if self._ok:
            return self._value
        raise self._value  # type: ignore[misc]


_queue = _internal.Queue()
_started = False
_stopping = False
_greenlet: gevent.Greenlet | None = None


def _task(func: Callable) -> None:
    try:
        func()
    except Exception as e:
        log.error(f"main_hub task failed: {e}", exc_info=True)


def _loop():
    while not _stopping:
        try:
            func, result = _queue.get_nowait()
            log.debug(f"Main hub picked up task: {func}")
        except _internal.QueueEmpty:
            gevent.sleep(0.005)
            continue
        if result is None:
            gevent.spawn(_task, func)
        else:
            try:
                func()
                result.set(None)
            except Exception as e:
                result.set_exception(e)


def ensure_started() -> None:
    """Lazily start the main hub loop. Safe to call multiple times."""
    global _started, _stopping, _greenlet
    if _started and not _stopping:
        return
    _stopping = False
    _started = True
    _greenlet = gevent.spawn(_loop)


def shutdown() -> None:
    """Stop the main hub loop. Safe to call multiple times."""
    global _started, _stopping, _greenlet
    if not _started:
        return
    _stopping = True
    if _greenlet is not None:
        _greenlet.join(timeout=2)
        _greenlet.kill(block=False)
        _greenlet = None
    _started = False


def _cleanup_resource_tracker() -> None:
    """Close multiprocessing resource tracker fd to prevent hang on exit.

    Under gevent monkey-patching, the resource tracker's pipe fd becomes
    non-blocking, causing its _stop() to fail during interpreter shutdown.
    Closing the fd directly lets the tracker process exit on its own.
    """
    with contextlib.suppress(Exception):
        import multiprocessing.resource_tracker as rt

        tracker = rt._resource_tracker
        fd = getattr(tracker, "_fd", None)
        if fd is not None:
            import os

            os.close(fd)
            tracker._fd = None  # type: ignore[attr-defined]


# atexit handlers run in LIFO order: clean resource tracker last (registered first)
atexit.register(shutdown)
atexit.register(_cleanup_resource_tracker)


def run_on_main_hub(func: Callable) -> Any:
    """Run function on main hub and wait for result. Thread-safe."""
    ensure_started()
    ar = AsyncResult()
    _queue.put((func, ar))
    return ar.get()


def spawn_on_main_hub(func: Callable, *args, **kwargs) -> None:
    """Schedule function on main hub without waiting. Thread-safe, fire-and-forget."""
    ensure_started()
    _queue.put((functools.partial(func, *args, **kwargs), None))
