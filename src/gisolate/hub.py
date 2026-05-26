"""Main hub: marshal tasks to the gevent default event loop."""

import atexit
import contextlib
import functools
from typing import Any, Callable

import gevent

from . import _internal


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


_lock = _internal.RLock()
_started = False
_stopping = False
_main_hub: Any = None


def _schedule(func: Callable) -> None:
    """Spawn ``func`` as a greenlet on the main hub from any thread.

    ``run_callback_threadsafe`` wakes the main hub's loop via its async
    watcher (thread-safe). The callback only spawns a greenlet — ``func``
    itself may block / switch, which a raw loop callback cannot.
    """
    _main_hub.loop.run_callback_threadsafe(lambda: gevent.spawn(func))


def ensure_hub_started() -> None:
    """Capture the main hub for cross-thread marshaling. Thread-safe.

    Must be called from the main thread on first invocation so that the
    captured hub is the default (main) event loop. ProcessProxy.__init__
    calls this, ensuring correct ownership when the proxy is created on
    the main thread.
    """
    global _started, _stopping, _main_hub
    if _started and not _stopping:
        return
    with _lock:
        if _started and not _stopping:
            return
        if not gevent.get_hub().loop.default:
            raise RuntimeError(
                "Hub must be started from the main thread. "
                "Create your first ProcessProxy on the main thread."
            )
        _stopping = False
        _main_hub = gevent.get_hub()
        _started = True


def shutdown() -> None:
    """Stop accepting marshaled tasks. Safe to call multiple times."""
    global _started, _stopping
    with _lock:
        if not _started:
            return
        _stopping = True
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


# atexit runs in LIFO order: shutdown hub first, then clean resource tracker
atexit.register(_cleanup_resource_tracker)
atexit.register(shutdown)


def run_on_main_hub(func: Callable, timeout: float | None = None) -> Any:
    """Run function on main hub and wait for result. Thread-safe.

    ``timeout`` bounds the wait so a wedged main hub (unable to run the
    scheduled callback) raises TimeoutError instead of blocking forever.
    """
    ensure_hub_started()
    with _lock:
        if _stopping:
            raise RuntimeError("Hub is shutting down")
        ar = AsyncResult()

    def runner():
        try:
            ar.set(func())
        except Exception as e:
            ar.set_exception(e)

    _schedule(runner)
    return ar.get(timeout)


def spawn_on_main_hub(func: Callable, *args, **kwargs) -> None:
    """Schedule function on main hub without waiting. Thread-safe, fire-and-forget."""
    ensure_hub_started()
    with _lock:
        if _stopping:
            return
    _schedule(functools.partial(func, *args, **kwargs))
