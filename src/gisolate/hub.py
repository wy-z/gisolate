"""Main hub: marshal tasks to the gevent default event loop."""

import atexit
import contextlib
import logging
import time
from typing import Any, Callable

import gevent
import gevent.exceptions

from . import _internal

log = logging.getLogger(__name__)


class WaitTimeout(TimeoutError):
    """get() timed out waiting for a result.

    Distinct type so callers can tell a wait-timeout apart from a stored
    exception that happens to be a TimeoutError (gevent's AsyncResult makes
    the same distinction via gevent.Timeout).
    """


class AsyncResult:
    """Thread-safe result container compatible with gevent.event.AsyncResult.

    The FIELDS are the truth; the event is only the fast path. get_original
    returns the original Event class, but its instances still build their
    Condition and Lock from the patched threading module — so its cross-thread
    wake is reliable only when the setter is a main-hub greenlet. A setter
    with no greenlet — _schedule's refusal path runs in a loop callback — gets
    its store in but may lose the wake: measured, a threadpool setter
    completed ``set()`` and the waiter still timed out. The sliced wait in
    :meth:`get` is what reads a completion whose wake was lost.
    """

    __slots__ = ("_event", "_ok", "_value")

    def __init__(self):
        self._event = _internal.Event()
        self._ok = self._value = None

    def set(self, value):
        # Value before flag: the waiter breaks on the flag, and the tuple
        # assignment stores left to right.
        self._value, self._ok = value, True
        self._event.set()

    def set_exception(self, exc):
        self._value, self._ok = exc, False
        self._event.set()

    def get(self, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        while not self._event.wait(0.05):
            if self._ok is not None:
                break  # completed by a setter whose wake was lost
            if deadline is not None and time.monotonic() >= deadline:
                raise WaitTimeout(f"Timed out after {timeout}s")
        if self._ok:
            return self._value
        raise self._value  # type: ignore[misc]


_lock = _internal.RLock()
_closed = False  # shutdown() ran: nothing is marshaled after it, started or not
_main_hub: Any = None


def _schedule(func: Callable, fail: Callable[[BaseException], None]) -> None:
    """Spawn ``func`` as a greenlet on the main hub from any thread.

    ``run_callback_threadsafe`` wakes the main hub's loop via its async
    watcher (thread-safe). The callback only spawns a greenlet — ``func``
    itself may block / switch, which a raw loop callback cannot.

    The spawn itself can refuse — allocation, a hub in teardown — and the
    refusal surfaces inside the loop callback, where the hub's error handler
    is the only audience. *fail* carries it back to whoever scheduled this:
    for :func:`run_on_main_hub` that waiter is usually on an unbounded
    ``get()``, and a spawn lost silently wedged it for good.

    *fail* is called right here in the callback, where its event wake may be
    forbidden: completing the result acquires an event lock a foreign-thread
    waiter leaves contended, and a loop callback may not block. The STORE
    lands before the wake is attempted, and :meth:`AsyncResult.get`'s sliced
    wait reads it — so the wake is best-effort, and only its own failure is
    suppressed.
    """

    def wake_lost(e: BaseException) -> None:
        try:
            fail(e)
        except gevent.exceptions.BlockingSwitchOutError:
            pass  # the store landed; the waiter's sliced wait sees it

    def scheduled():
        try:
            gevent.spawn(func)
        except KeyboardInterrupt as e:
            # The operator's, landing in the callback itself. Told to the
            # waiter AND passed on, for runner's reason below.
            wake_lost(e)
            raise
        except BaseException as e:  # noqa: BLE001
            wake_lost(e)

    _main_hub.loop.run_callback_threadsafe(scheduled)


def ensure_hub_started() -> None:
    """Capture the main hub for cross-thread marshaling. Thread-safe.

    Must be called from the main thread on first invocation so that the
    captured hub is the default (main) event loop. ProcessProxy.__init__
    calls this, ensuring correct ownership when the proxy is created on
    the main thread.
    """
    global _main_hub
    if _main_hub is not None or _closed:
        return
    with _lock:
        if _main_hub is not None or _closed:
            return
        if not gevent.get_hub().loop.default:
            raise RuntimeError(
                "Hub must be started from the main thread. "
                "Create your first ProcessProxy on the main thread."
            )
        _main_hub = gevent.get_hub()


def shutdown() -> None:
    """Stop accepting marshaled tasks. Safe to call multiple times."""
    global _closed
    with _lock:
        _closed = True


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
        if _closed:
            raise RuntimeError("Hub is shutting down")
        ar = AsyncResult()

    def runner():
        try:
            ar.set(func())
        except KeyboardInterrupt as e:
            # Told to the waiter AND passed on. Relaying alone is not enough:
            # the waiter is usually a native thread, and an uncaught
            # KeyboardInterrupt there kills only that thread while the host
            # carries on. Raised here it reaches the hub, and gevent forwards it
            # to the main greenlet, which is what the operator asked for.
            ar.set_exception(e)
            raise
        except BaseException as e:
            # BaseException too: a GreenletExit (the hub killing this greenlet)
            # or a SystemExit would otherwise complete the result neither way,
            # and the caller — usually on the default unbounded wait — would
            # block forever. Re-raised in the calling thread, it is at least
            # visible where someone can act on it.
            ar.set_exception(e)

    _schedule(runner, ar.set_exception)
    return ar.get(timeout)


def spawn_on_main_hub(func: Callable, *args, **kwargs) -> None:
    """Schedule function on main hub without waiting. Thread-safe, fire-and-forget."""
    ensure_hub_started()
    with _lock:
        if _closed:
            return

    def runner():
        try:
            func(*args, **kwargs)
        except gevent.GreenletExit:
            raise
        except KeyboardInterrupt:
            # The operator's. Measured: a real SIGINT is raised in whatever
            # greenlet is running on the main OS thread, so it lands here rather
            # than in the main greenlet.
            raise
        except BaseException:
            # The boundary run_on_main_hub gets from its AsyncResult, which
            # fire-and-forget work has nowhere to record. gevent forwards a
            # greenlet's SystemExit or KeyboardInterrupt to the main one, which
            # ends the process — and the caller that scheduled this is on
            # another thread entirely, with nothing waiting on the outcome.
            log.exception("spawn_on_main_hub task failed")

    # Fire-and-forget has no waiter to tell, so a refused spawn is only logged
    # — but logged HERE, not left to the hub's stderr handler.
    _schedule(runner, lambda e: log.error(f"task was never spawned: {e!r}"))
