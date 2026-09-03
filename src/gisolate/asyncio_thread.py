"""AsyncioThread: one asyncio event loop on one native thread, in a gevent process.

Why a thread and not a greenlet: CPython's ``_asyncio`` keeps the running loop
in *thread* state, so two greenlets each running a loop collide
(``asyncio.run() cannot be called from a running event loop``). A native thread
has its own thread state, and so its own loop.

Why nothing gevent inside that thread: a gevent selector or socket binds to the
hub of the thread that created it, and a loop built on one waits on a hub that
never runs (``LoopExit``). The loop here polls with the interpreter's own
``poll()`` and wakes itself through the interpreter's own ``socketpair()``,
both reached through ``get_original``. That pipe is the only descriptor the
loop thread owns; application and network I/O stay on the gevent thread.

Two crossings, both bounded and cancellable from either side:

- :meth:`AsyncioThread.call` — from a greenlet: run a coroutine on the loop,
  blocking only the calling greenlet.
- :meth:`AsyncioThread.to_gevent` — from the loop: run a function in a fresh
  greenlet on the calling greenlet's thread and await its result.
"""

import asyncio
import contextvars
import logging
import select
import time
from typing import Any, Callable, Coroutine

import _socket
import gevent
import gevent.event
import gevent.monkey

from . import _internal
from .hub import WaitTimeout

log = logging.getLogger(__name__)

_start_new_thread = gevent.monkey.get_original("_thread", "start_new_thread")

NEW, STARTING, RUNNING, STOPPING, DEAD = (
    "new",
    "starting",
    "running",
    "stopping",
    "dead",
)

# How long a cancelled coroutine is given to unwind — by call() leaving early,
# and by the teardown. Only a coroutine that ignores its cancellation runs it
# out; the teardown abandons such a task rather than wait on it.
_UNWIND_GRACE = 6.0


class LoopStopped(RuntimeError):
    """The loop is not running — stopped, dead, or never started."""


if hasattr(select, "poll"):

    class _RawSelector(gevent.monkey.get_original("selectors", "PollSelector")):  # type: ignore[misc]
        """stdlib PollSelector on the unpatched poll(): no hub involvement."""

        _selector_cls = gevent.monkey.get_original("select", "poll")

else:  # Windows: select() only, and the loop watches one fd anyway
    _select = gevent.monkey.get_original("select", "select")

    class _RawSelector(gevent.monkey.get_original("selectors", "SelectSelector")):  # type: ignore[misc,no-redef]
        def _select(self, r, w, x, timeout=None):
            return _select(r, w, x, timeout)


class _Loop(asyncio.SelectorEventLoop):
    """A stdlib-only loop: its selector, its wake-up pipe, and no executor."""

    def __init__(self):
        super().__init__(_RawSelector())

    def _make_self_pipe(self):
        # asyncio calls the (patched) socket.socketpair here, which would hand
        # the loop gevent sockets bound to this thread's hub. Not the original
        # socket.socketpair either: it wraps its pair in socket.py's `socket`
        # global, which is patched too. The C-level pair has all the loop uses.
        self._ssock, self._csock = _socket.socketpair()
        self._ssock.setblocking(False)
        self._csock.setblocking(False)
        self._internal_fds += 1  # type: ignore[attr-defined]
        self._add_reader(self._ssock.fileno(), self._read_from_self)  # type: ignore[attr-defined]

    def run_in_executor(self, executor, func, *args):
        # Only where it would wedge: an unpatched process gets real threads.
        if gevent.monkey.is_module_patched("threading"):
            raise RuntimeError(
                "AsyncioThread refuses run_in_executor/to_thread: under monkey-patching "
                "the worker thread is a greenlet on the loop's own thread, which cannot "
                "run while the loop blocks in poll(). Use AsyncioThread.to_gevent."
            )
        return super().run_in_executor(executor, func, *args)


# The hub of the greenlet that called in, seen by every task the coroutine
# spawns: to_gevent lands its work back there, not on whichever thread
# happened to start the loop.
_caller_hub: contextvars.ContextVar = contextvars.ContextVar("gisolate_caller_hub")


def _schedule_on_hub(hub, fn: Callable, *args) -> None:
    """Run ``fn`` on ``hub``'s thread, from any thread."""
    try:
        hub.loop.run_callback_threadsafe(fn, *args)
    except Exception:  # noqa: BLE001
        log.warning("AsyncioThread: hub wake failed", exc_info=True)


def _settle_once(result: gevent.event.AsyncResult, setter: Callable, value) -> None:
    """The first answer stands: a call failed by the loop's exit is not then
    re-answered by the task the exit cancelled."""
    if not result.ready():
        setter(value)


def _close(loop, torn: set) -> None:
    """asyncio.run()'s teardown, bounded: a task that will not yield to its
    cancellation is abandoned after the grace, not waited on forever."""
    loop.set_task_factory(None)  # the teardown's own tasks, not a caller's factory
    deadline = time.monotonic() + _UNWIND_GRACE
    try:
        _drain(loop, deadline, torn)
        # Open async generators, closed the same way: given the grace, then
        # abandoned. (Not wait_for: a close that swallows its cancellation
        # would hold that up too.)
        closing = loop.create_task(loop.shutdown_asyncgens())
        loop.run_until_complete(
            asyncio.wait({closing}, timeout=max(0, deadline - time.monotonic()))
        )
        closing.cancel()
        _drain(loop, deadline, torn)  # a close may have spawned tasks of its own
        # An unpatched process may have used the default executor; its
        # threads must not outlive stop() — within reason.
        loop.run_until_complete(loop.shutdown_default_executor(_UNWIND_GRACE))
        _drain(loop, deadline, torn)  # so may a future of theirs, completing
    except BaseException:  # noqa: BLE001 — a raw native thread: logged is all there is
        log.exception("AsyncioThread: loop teardown failed")
    finally:
        loop.close()


def _drain(loop, deadline: float, torn: set) -> None:
    """Cancel every task and wait for it — in short slices, looking again
    each time, so a task that holds out cannot hide one a cleanup spawned
    meanwhile — until none is left or the deadline is."""
    while (tasks := asyncio.all_tasks(loop)) and (
        left := deadline - time.monotonic()
    ) > 0:
        for task in tasks:
            # One already being cancelled keeps its own reason and its
            # cleanup; a to_gevent it awaits is ended by the crossing's kill.
            if not task.cancelling() and task not in torn:
                task.cancel()
                torn.add(task)
        loop.run_until_complete(asyncio.wait(tasks, timeout=min(left, 0.05)))


def _wait(result: gevent.event.AsyncResult, timeout: float | None) -> Any:
    """Wait on a result another thread will set, without the hub giving up.

    A hub with no watchers left raises ``LoopExit`` rather than wait for a
    wake it cannot see coming; the ref'd async watcher is what tells it one
    is. (A server's listening socket does the same, which is why this never
    shows up under gunicorn.)
    """
    keep = gevent.get_hub().loop.async_()
    keep.start(lambda: None)
    try:
        result.wait(timeout)
        if not result.ready():
            raise WaitTimeout(f"Timed out after {timeout}s")
        return result.get()
    finally:
        keep.stop()
        keep.close()


class AsyncioThread:
    """One asyncio loop on one native thread. See the module docstring."""

    def __init__(self, *, start_timeout: float = 10.0):
        self._start_timeout = start_timeout
        self._lock = _internal.RLock()
        self._state = NEW
        # Where the loop's own residents land — captured in start().
        self._hub: Any = None
        self._loop: Any = None
        self._closing = False  # loop thread only: teardown has begun
        self._crossings: dict = {}  # loop thread only: active to_gevent kills, by future
        self._torn: set = set()  # loop thread only: the tasks the teardown cancelled
        self._pending: set = set()  # (hub, result) of every in-flight call()
        self._stopped: list = []  # (hub, result) of every stop() waiting on exit

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()

    # -- lifecycle ----------------------------------------------------------

    def start(self):
        with self._lock:
            if self._state is not NEW:
                raise RuntimeError(f"AsyncioThread already {self._state}")
            if not hasattr(_socket, "socketpair"):  # Windows emulates the pair over TCP
                self._state = DEAD
                raise RuntimeError(
                    "AsyncioThread needs a native socketpair: POSIX only"
                )
            self._state = STARTING
        self._hub = gevent.get_hub()
        started = gevent.event.AsyncResult()
        try:
            _start_new_thread(self._run, (started,))
        except BaseException:
            with self._lock:
                self._state = DEAD  # no thread: nothing to stop, ever
                stopped, self._stopped = self._stopped, []
                self._hub = self._loop = None
            for hub, exited in stopped:  # a stop() that got in meanwhile
                _schedule_on_hub(hub, exited.set)
            raise
        try:
            _wait(started, self._start_timeout)
        except BaseException:
            # Leaving — timed out, or killed. Close the door: still coming
            # up, the loop arrives, finds STOPPING and leaves; already
            # RUNNING with its word not yet through to this hub, it is told
            # to stop, or it runs unowned.
            with self._lock:
                if self._state is RUNNING:
                    self._loop.call_soon_threadsafe(self._stop_loop)
                if self._state is not DEAD:
                    self._state = STOPPING
            raise
        return self

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the loop and wait for the thread to exit.

        The teardown's contract, and all of it:

        - Every task is cancelled and every greenlet a ``to_gevent`` is
          awaiting is killed — including ones created during the teardown.
        - A call whose coroutine honours the cancellation raises
          :class:`LoopStopped`; one already handling a cancellation of its
          own keeps its cleanup and its own answer or reason; one whose
          coroutine finishes regardless gets its answer.
        - The whole teardown is bounded by ``_UNWIND_GRACE``: a task, async
          generator or executor thread that does not yield to it in that
          time is abandoned, and the loop is closed under it.

        Nothing beyond that is promised for a coroutine that resists its
        cancellation; ``timeout`` bounds this call's own wait.
        """
        exited = gevent.event.AsyncResult()
        with self._lock:
            if self._state in (NEW, DEAD):
                self._state = DEAD
                return
            # RUNNING under the lock means the loop is open: DEAD is set here
            # before it closes. STARTING: the loop is not up to be stopped; its
            # arrival callback sees STOPPING and stops it.
            if self._state is RUNNING:
                self._loop.call_soon_threadsafe(self._stop_loop)
            self._state = STOPPING
            waiter = (gevent.get_hub(), exited)
            self._stopped.append(waiter)
        try:
            _wait(exited, timeout)
        finally:
            with self._lock:
                if waiter in self._stopped:  # left early: the teardown owes it nothing
                    self._stopped.remove(waiter)

    def _stop_loop(self):  # loop thread
        # A stop() may have queued this while the loop was already on its way
        # out; landing inside the teardown's own run_until_complete, it would
        # end that early.
        if not self._closing:
            self._loop.stop()

    def _run(self, started):
        try:
            loop = self._loop = _Loop()
            asyncio.set_event_loop(loop)

            def running():
                with self._lock:
                    if self._state is not STARTING:
                        loop.stop()  # start() gave up, or stop() got here first
                        return
                    self._state = RUNNING
                _schedule_on_hub(self._hub, started.set)

            loop.call_soon(running)
            loop.run_forever()
        except BaseException as e:  # noqa: BLE001 — relayed to start(), then logged
            # A raw native thread: no operator interrupt lands here, and the
            # gevent-side results are the only way out.
            log.exception("AsyncioThread loop died")
            _schedule_on_hub(self._hub, started.set_exception, e)
        finally:
            self._closing = True
            with self._lock:
                self._state = STOPPING  # no new calls; done() answers LoopStopped
            # Every greenlet a to_gevent is awaiting is killed now, on its
            # hub — the teardown's cancellations do not reach a task already
            # cancelling, and its cleanup may be exactly such an await.
            for hub, kill in list(self._crossings.values()):
                _schedule_on_hub(hub, kill)
            # Teardown BEFORE anyone is told the loop is gone: cancelling a task
            # queues the kill of the greenlet it awaited on its caller's hub,
            # and the caller — a one-shot native thread, say — takes that hub
            # with it the moment it has its answer. Each caller's answer
            # follows its kill, in the same queue.
            if self._loop is not None:
                _close(self._loop, self._torn)
            self._crossings.clear()  # what a crossing's own end did not pop: abandoned
            self._torn.clear()  # dead tasks, and through them callers' hubs: not ours to keep
            with self._lock:
                self._state = DEAD
                pending, self._pending = self._pending, set()
                stopped, self._stopped = self._stopped, []
            if not started.ready():  # a start() still waiting: the loop left first
                exc = LoopStopped("stopped before start completed")
                _schedule_on_hub(
                    self._hub, _settle_once, started, started.set_exception, exc
                )
            for hub, result in pending:  # calls the loop never got to
                exc = LoopStopped("loop stopped before the call completed")
                _schedule_on_hub(hub, _settle_once, result, result.set_exception, exc)
            for hub, exited in stopped:  # each stop() on its own hub
                _schedule_on_hub(hub, exited.set)
            self._hub = self._loop = (
                None  # nothing keeps the starter's hub, or the loop
            )

    # -- greenlet -> loop ---------------------------------------------------

    def call(self, coro: Coroutine, timeout: float | None = None) -> Any:
        """Run ``coro`` on the loop; block only the calling greenlet.

        ``timeout`` raises :class:`WaitTimeout`. Leaving early for any reason
        — timeout, ``gevent.Timeout``, the greenlet being killed — cancels the
        coroutine on the loop, and returns only once the loop has unwound it.
        """
        hub = gevent.get_hub()
        result = gevent.event.AsyncResult()
        context = contextvars.copy_context()
        context.run(_caller_hub.set, hub)
        task: list = []  # the Task, once the loop has made it

        def done(t):  # loop thread: once, whether or not the coroutine ever ran
            if t.cancelled():
                if t in self._torn:  # ours: the loop went down under it
                    exc = LoopStopped("loop stopped before the call completed")
                else:  # the coroutine's own doing — with its reason, if it gave one
                    exc = asyncio.CancelledError()
                    try:
                        t.result()
                    except asyncio.CancelledError as e:
                        exc = e
                _schedule_on_hub(hub, _settle_once, result, result.set_exception, exc)
            elif (exc := t.exception()) is not None:
                if self._closing and isinstance(exc, gevent.GreenletExit):
                    # The teardown killed the greenlet it was awaiting.
                    exc = LoopStopped("loop stopped before the call completed")
                _schedule_on_hub(hub, _settle_once, result, result.set_exception, exc)
            else:
                _schedule_on_hub(hub, _settle_once, result, result.set, t.result())

        def create():  # loop thread
            if self._state is not RUNNING:
                coro.close()  # on its way out: the sweep after teardown answers
                return
            try:
                t = loop.create_task(coro, context=context)
            except BaseException as e:  # noqa: BLE001 — a task factory that raises, say
                coro.close()
                _schedule_on_hub(hub, _settle_once, result, result.set_exception, e)
                if isinstance(e, (KeyboardInterrupt, SystemExit)):
                    raise  # the operator's: told to the caller AND passed on
                return
            t.add_done_callback(done)
            task.append(t)

        def cancel():  # loop thread, queued after create: it finds the task
            if task:
                task[0].cancel()

        entry = (hub, result)
        with self._lock:
            if self._state is not RUNNING:
                coro.close()
                raise LoopStopped(f"AsyncioThread is {self._state}")
            loop = self._loop  # ours from here: teardown clears the attribute
            loop.call_soon_threadsafe(create)
            self._pending.add(entry)
        try:
            return _wait(result, timeout)
        finally:
            with self._lock:
                self._pending.discard(entry)
            if not result.ready():
                # Leaving early: cancel, then stay until the loop has delivered
                # the cancellation — the kill of any greenlet the coroutine
                # was awaiting is queued on THIS hub, which may stop spinning
                # the moment we return.
                try:
                    loop.call_soon_threadsafe(cancel)
                except RuntimeError:
                    pass  # closed: the exit's answer is on its way to this hub
                # A further interruption meanwhile — an outer gevent.Timeout, a
                # kill — changes nothing about that; it is raised once the wait
                # is over, and the grace bounds the wait.
                deadline = time.monotonic() + _UNWIND_GRACE
                interruption = None
                while not result.ready() and (left := deadline - time.monotonic()) > 0:
                    try:
                        result.wait(left)
                    except KeyboardInterrupt:
                        raise
                    except BaseException as e:  # noqa: BLE001 — raised below
                        interruption = e
                if not result.ready():
                    log.warning(
                        "AsyncioThread: a cancelled coroutine did not unwind within %ss",
                        _UNWIND_GRACE,
                    )
                if interruption is not None:
                    raise interruption

    # -- loop -> greenlet ---------------------------------------------------

    async def to_gevent(self, fn: Callable, *args, **kwargs) -> Any:
        """Run ``fn(*args, **kwargs)`` in a fresh greenlet on the caller's thread.

        The caller is whoever called in through :meth:`call`; the loop's own
        residents, which nobody called in, land on the thread that started it.
        A task that outlives the call it descends from must not come here:
        its caller's hub may be gone with the caller's thread.
        A killed greenlet raises ``GreenletExit`` here rather than returning it
        (gevent counts a kill as success; the awaiting side must not).
        Cancelling this coroutine kills the greenlet.
        """
        loop = asyncio.get_running_loop()
        if loop is not self._loop:
            raise RuntimeError("to_gevent must be awaited on this AsyncioThread's loop")
        hub = _caller_hub.get(self._hub)
        fut = loop.create_future()  # the answer; cancelled with this task
        ended = loop.create_future()  # the greenlet's end, whatever became of fut
        greenlet = None
        killed = False

        def settle_on_loop(target, setter: Callable, value):  # from the gevent thread
            def settle():
                if not target.done():  # the awaiter may already have left
                    setter(value)

            try:
                loop.call_soon_threadsafe(settle)
            except RuntimeError:
                pass  # loop closed: the awaiter is gone with it

        def run():  # the greenlet: an exception is the awaiter's answer, not
            # a report to the hub — gevent.Timeout and CancelledError included.
            # SystemExit and KeyboardInterrupt pass: gevent forwards those to
            # the main greenlet, as the operator meant; GreenletExit is a kill.
            try:
                return None, fn(*args, **kwargs)
            except (KeyboardInterrupt, SystemExit, gevent.GreenletExit):
                raise
            except BaseException as e:  # noqa: BLE001 — relayed below
                return e, None

        def finish(g):  # link callback, gevent thread
            if (exc := g.exception) is not None:
                if not isinstance(exc, Exception):
                    # Forwarded to the main greenlet already; asyncio would
                    # let it take the loop down too.
                    exc = RuntimeError(f"{type(exc).__name__} escaped the greenlet")
                settle_on_loop(fut, fut.set_exception, exc)
            elif isinstance(g.value, gevent.GreenletExit):
                settle_on_loop(fut, fut.set_exception, g.value)
            elif (exc := g.value[0]) is not None:
                settle_on_loop(fut, fut.set_exception, exc)
            else:
                settle_on_loop(fut, fut.set_result, g.value[1])
            settle_on_loop(ended, ended.set_result, None)

        def spawn():  # hub callback, gevent thread
            nonlocal greenlet
            try:
                greenlet = gevent.spawn(run)
            except KeyboardInterrupt as e:
                settle_on_loop(fut, fut.set_exception, e)
                raise  # the operator's: told to the awaiter AND passed on
            except BaseException as e:  # noqa: BLE001 — relayed to the awaiter
                settle_on_loop(fut, fut.set_exception, e)
            else:
                greenlet.link(finish)

        def kill():  # hub callback, gevent thread — queued after spawn, so it
            # finds the greenlet; on one not yet switched to, kill() cancels the
            # start, and fn never runs. Once: a second kill would throw into
            # the cleanup the first one started.
            nonlocal killed
            if greenlet is not None and not killed:
                killed = True
                greenlet.kill(block=False)

        _schedule_on_hub(hub, spawn)
        self._crossings[fut] = (hub, kill)
        if (
            self._closing
        ):  # entered from a cleanup the teardown is running: ended at once
            _schedule_on_hub(hub, kill)
        try:
            return await fut
        except asyncio.CancelledError:
            _schedule_on_hub(hub, kill)
            # Stay for the greenlet's end — its cleanup may yield — so the
            # caller, whose hub it lives on, is still there for it: the caller
            # leaves only once this task has unwound. A further cancellation
            # (a teardown's) changes nothing about that; the grace bounds the
            # stay, for a hub that no longer spins.
            deadline = loop.time() + _UNWIND_GRACE
            while not ended.done():
                try:
                    await asyncio.wait_for(
                        asyncio.shield(ended), deadline - loop.time()
                    )
                except asyncio.CancelledError:
                    continue
                except TimeoutError:
                    break
            raise
        finally:
            self._crossings.pop(fut, None)
