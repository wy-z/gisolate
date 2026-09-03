"""Tests for gisolate.asyncio_thread — one asyncio loop on one native thread."""

import asyncio
import collections.abc
import subprocess
import sys
import textwrap
import time

import gevent
import gevent.monkey
import pytest

from gisolate import AsyncioThread, LoopStopped, WaitTimeout

real_ident = gevent.monkey.get_original("_thread", "get_ident")
start_native = gevent.monkey.get_original("_thread", "start_new_thread")
MAIN_IDENT = real_ident()


class Boom(Exception):
    pass


@pytest.fixture
def thread():
    with AsyncioThread() as t:
        yield t
    assert t._state == "dead"


def wait_for(flag, timeout=2.0):
    with gevent.Timeout(timeout):
        while not flag:
            gevent.sleep(0.01)


def outcome(fn, *args):
    try:
        return fn(*args)
    except BaseException as e:  # noqa: BLE001 — the test reads it (CancelledError is one)
        return e


async def add(a, b):
    return a + b


async def boom():
    raise Boom("from the loop")


def slow_until_killed(killed):
    """A greenlet that sleeps until killed, and says so — after a cleanup
    that yields, as a real one (closing a connection, say) would."""
    try:
        time.sleep(10)
    except gevent.GreenletExit:
        gevent.sleep(0.2)
        killed.append(True)
        raise


async def sleep_until_cancelled(cancelled):
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        cancelled.append(True)
        raise


class TestLoop:
    def test_runs_on_a_native_thread_with_unpatched_io(self, thread):
        async def ident():
            return real_ident()

        assert thread.call(ident()) != MAIN_IDENT
        # No gevent inside the loop thread: the selector and the wake-up pipe
        # are the interpreter's own, or the loop binds to a hub it never runs.
        assert "Gevent" not in type(thread._loop._selector).__name__
        assert type(thread._loop._ssock).__module__ == "_socket"

    def test_without_a_native_socketpair_start_says_so(self, monkeypatch):
        """Windows CPython has no _socket.socketpair (it emulates one over
        TCP): the loop is POSIX-only, and says so at start rather than
        failing somewhere inside asyncio."""
        import _socket

        monkeypatch.delattr(_socket, "socketpair")
        t = AsyncioThread()
        with pytest.raises(RuntimeError, match="POSIX"):
            t.start()
        assert t._state == "dead"

    def test_executor_is_refused(self, thread):
        """A thread the loop spawns is a greenlet on the loop's own thread —
        which cannot run while the raw selector blocks. Fail loudly instead."""
        with pytest.raises(RuntimeError, match="to_gevent"):
            thread.call(asyncio.to_thread(time.sleep, 0))

    def test_unpatched_default_executor_is_joined_on_stop(self):
        """Without monkey-patching the loop keeps its executor; stop() must not
        leave its threads behind. A subprocess, because this suite is patched."""
        script = textwrap.dedent(
            """
            import asyncio, threading, time
            from gisolate import AsyncioThread
            cleaned = []
            async def cleanup():
                try:
                    await asyncio.sleep(10)
                finally:  # cancelled by the teardown, not destroyed pending
                    cleaned.append(True)
            async def late_spawner():
                # An executor future completing during the executor's shutdown
                # creates a task: the teardown must end that one too.
                loop = asyncio.get_running_loop()
                fut = loop.run_in_executor(None, time.sleep, 0.2)
                fut.add_done_callback(lambda f: loop.create_task(cleanup()))
            async def immortal():
                while True:
                    try:
                        await asyncio.sleep(10)
                    except asyncio.CancelledError:
                        pass
            async def hold_executor():
                asyncio.get_running_loop().run_in_executor(None, time.sleep, 3)
            import gevent
            from gisolate import asyncio_thread
            asyncio_thread._UNWIND_GRACE = 0.5
            with AsyncioThread() as t:
                assert t.call(asyncio.to_thread(time.sleep, 0.05)) is None
                assert any("asyncio" in th.name for th in threading.enumerate())
                t.call(late_spawner())
                t.call(hold_executor())  # an executor thread the shutdown cannot join in time
                gevent.spawn(t.call, immortal())  # a task that eats the whole grace
                gevent.sleep(0.05)
                started = time.monotonic()
                t.stop(timeout=5)
                took = time.monotonic() - started
                assert took < 0.8, took  # ONE grace bounds the whole teardown, executor included
            assert cleaned == [True], cleaned
            print("ok")
            """
        )
        out = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
        )
        assert out.returncode == 0, out.stderr
        assert out.stdout.strip() == "ok"


class TestCall:
    def test_propagates_the_exception(self, thread):
        with pytest.raises(Boom, match="from the loop"):
            thread.call(boom())

    def test_many_greenlets_run_concurrently(self, thread):
        started = time.monotonic()
        gevent.joinall(
            [gevent.spawn(thread.call, asyncio.sleep(0.2)) for _ in range(50)],
            raise_error=True,
        )
        assert time.monotonic() - started < 1.0

    def test_timeout_cancels_the_coroutine(self, thread):
        cancelled = []
        with pytest.raises(WaitTimeout):
            thread.call(sleep_until_cancelled(cancelled), timeout=0.1)
        wait_for(cancelled)

    def test_killing_the_waiting_greenlet_cancels_the_coroutine(self, thread):
        cancelled = []
        g = gevent.spawn(thread.call, sleep_until_cancelled(cancelled))
        gevent.sleep(0.05)
        g.kill()
        wait_for(cancelled)

    def test_leaving_before_the_coroutine_started_is_not_a_wait_for_it(self, thread):
        """A call cancelled before the loop ever ran its coroutine must not
        sit out the unwind grace for a coroutine that has nothing to unwind."""

        async def hog():
            time.sleep(0.3)  # the loop thread is unpatched: this blocks it

        gevent.spawn(thread.call, hog())
        gevent.sleep(0.05)  # the hog is on the loop; nothing after it can start
        started = time.monotonic()
        with pytest.raises(WaitTimeout):
            thread.call(asyncio.sleep(10), timeout=0)
        assert time.monotonic() - started < 2.0

    @pytest.mark.parametrize("failure", [Boom, asyncio.CancelledError, SystemExit])
    def test_a_task_the_loop_cannot_create_is_the_callers_error(self, failure, caplog):
        """create_task can fail — a task factory that raises, say, and not
        only with an Exception. The caller must hear it, not wait for a task
        that never was; and the teardown, which makes tasks of its own, must
        not trip over the same factory."""

        def broken_factory(loop, coro, **kwargs):
            coro.close()
            raise failure("no tasks today")

        async def install():
            asyncio.get_running_loop().set_task_factory(broken_factory)

        t = AsyncioThread().start()
        t.call(install())
        with pytest.raises(failure, match="no tasks today"):
            t.call(add(1, 2))
        t.stop(timeout=2)
        assert t._state == "dead"
        assert "teardown failed" not in caplog.text

    def test_a_cancellation_keeps_its_reason(self, thread):
        async def quit_with_reason():
            raise asyncio.CancelledError("on purpose")

        with pytest.raises(asyncio.CancelledError, match="on purpose"):
            thread.call(quit_with_reason())

    def test_a_coroutine_cancelling_itself_is_not_a_dead_loop(self, thread):
        async def quit_early():
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            await asyncio.sleep(0)

        with pytest.raises(asyncio.CancelledError):
            thread.call(quit_early())
        assert thread._state == "running"
        assert thread.call(add(1, 1)) == 2


class TestToGevent:
    def test_runs_fn_in_a_greenlet_on_the_calling_thread(self, thread):
        def where():
            return real_ident(), type(gevent.getcurrent()).__name__

        ident, kind = thread.call(thread.to_gevent(where))
        assert ident == MAIN_IDENT
        assert kind == "Greenlet"

    def test_lands_on_the_calling_thread_not_the_starting_one(self, thread):
        """The greenlet runs on the hub of whoever called in, not the hub
        start() saw: a request served on another thread gets its work back
        on that thread."""
        seen = []

        def caller():
            seen.append((real_ident(), thread.call(thread.to_gevent(real_ident))))

        start_native(caller, ())
        wait_for(seen)
        caller_ident, landed = seen[0]
        assert caller_ident != MAIN_IDENT
        assert landed == caller_ident

    def test_gevent_blocking_calls_run_concurrently(self, thread):
        started = time.monotonic()
        gevent.joinall(
            [
                gevent.spawn(thread.call, thread.to_gevent(time.sleep, 0.2))
                for _ in range(20)
            ],
            raise_error=True,
        )
        assert time.monotonic() - started < 1.0

    def test_propagates_the_exception(self, thread):
        def fail():
            raise Boom("from the greenlet")

        with pytest.raises(Boom, match="from the greenlet"):
            thread.call(thread.to_gevent(fail))

    def test_a_gevent_timeout_in_fn_is_relayed_as_itself(self, thread):
        """gevent.Timeout is a BaseException; it is still fn's own answer."""

        def late():
            raise gevent.Timeout(1)

        with pytest.raises(gevent.Timeout):
            thread.call(thread.to_gevent(late))

    def test_a_system_exit_in_fn_does_not_take_the_loop_down(self, thread):
        """gevent forwards a SystemExit escaping a greenlet to the main
        greenlet — the operator's intent, kept. The loop must survive it:
        asyncio would otherwise re-raise it out of run_forever."""

        def quit():
            raise SystemExit(3)

        with pytest.raises(SystemExit):
            thread.call(thread.to_gevent(quit))
        assert thread.call(thread.to_gevent(real_ident)) == MAIN_IDENT

    def test_a_killed_greenlet_is_an_error_not_a_value(self, thread):
        """gevent counts a GreenletExit as success; the awaiting coroutine must not."""

        def die():
            raise gevent.GreenletExit("killed")

        with pytest.raises(gevent.GreenletExit):
            thread.call(thread.to_gevent(die))

    def test_one_shot_caller_leaves_only_after_its_greenlet_is_killed(self, thread):
        """A thread that calls once and exits takes its hub with it. The kill
        of the greenlet it awaited must land BEFORE call() returns, while that
        hub still spins — or the greenlet stays suspended and its finally
        never runs."""
        killed = []
        seen = []

        def caller():
            try:
                thread.call(thread.to_gevent(slow_until_killed, killed), timeout=0.1)
            except WaitTimeout:
                seen.append(list(killed))  # what call() left behind, as it returned

        start_native(caller, ())
        wait_for(seen)
        assert seen == [[True]]

    def test_one_shot_caller_interrupted_again_still_leaves_after_its_greenlet(
        self, thread
    ):
        """An outer gevent.Timeout (or a kill) arriving while call() waits for
        the cancellation to unwind must not cut that wait short either: the
        greenlet's cleanup still needs this hub. What interrupted is raised
        once the wait is over."""
        killed = []
        seen = []

        def caller():
            try:
                with gevent.Timeout(0.15):
                    thread.call(
                        thread.to_gevent(slow_until_killed, killed), timeout=0.1
                    )
            except gevent.Timeout:
                seen.append(list(killed))

        start_native(caller, ())
        wait_for(seen)
        assert seen == [[True]]

    def test_outside_the_loop_raises(self, thread):
        with pytest.raises(RuntimeError):
            asyncio.run(thread.to_gevent(lambda: 1))


class TestLifecycle:
    def test_start_twice_raises(self, thread):
        with pytest.raises(RuntimeError):
            thread.start()

    def test_a_start_timeout_never_becomes_running(self):
        """The thread is still coming up when start() gives up; when it does
        arrive it must find the door closed, not set RUNNING behind our back."""
        t = AsyncioThread(start_timeout=0)
        with pytest.raises(WaitTimeout):
            t.start()
        gevent.sleep(0.2)
        assert t._state in ("stopping", "dead")
        t.stop(timeout=2)
        assert t._state == "dead"

    @pytest.mark.parametrize(
        "leaving", [WaitTimeout, gevent.Timeout, gevent.GreenletExit]
    )
    def test_a_start_that_gives_up_after_the_loop_arrived_still_stops(
        self, monkeypatch, leaving
    ):
        """The loop can reach RUNNING with its word still in flight to the hub
        when start() gives up — by its own timeout, an outer one, or a kill.
        Closing the door is not enough then: the loop is up and nobody has
        told it to stop."""
        from gisolate import asyncio_thread

        real_wait = asyncio_thread._wait

        def wait_then_leave(result, timeout):
            real_wait(result, 2)  # the loop is RUNNING and its word delivered ...
            raise leaving()  # ... and start() leaves anyway

        t = AsyncioThread()
        monkeypatch.setattr(asyncio_thread, "_wait", wait_then_leave)
        with pytest.raises(leaving):
            t.start()
        monkeypatch.setattr(asyncio_thread, "_wait", real_wait)
        t.stop(timeout=2)
        assert t._state == "dead"

    def test_stop_during_start_fails_start_at_once(self, monkeypatch):
        """stop() before the loop is up: the loop arrives, finds STOPPING and
        leaves — and start() must hear that now, not sit out its timeout."""
        from gisolate import asyncio_thread

        native_sleep = gevent.monkey.get_original("time", "sleep")
        loop_init = asyncio_thread._Loop.__init__

        def slow_init(self):
            native_sleep(0.3)  # the loop thread, before it can arrive
            loop_init(self)

        monkeypatch.setattr(asyncio_thread._Loop, "__init__", slow_init)
        t = AsyncioThread()
        starter = gevent.spawn(outcome, t.start)
        gevent.sleep(0.05)
        assert t._state == "starting"
        t.stop(timeout=2)
        assert isinstance(starter.get(timeout=2), LoopStopped)
        assert t._state == "dead"

    def test_a_stop_waiting_on_a_launch_that_fails_is_answered(self, monkeypatch):
        from gisolate import asyncio_thread

        t = AsyncioThread()
        stopper = []

        def launch_fails(fn, args):
            stopper.append(gevent.spawn(t.stop, 2))
            gevent.sleep(0.01)  # it registers while the state is STARTING
            raise RuntimeError("can't start new thread")

        monkeypatch.setattr(asyncio_thread, "_start_new_thread", launch_fails)
        with pytest.raises(RuntimeError, match="can't start"):
            t.start()
        assert stopper[0].get(timeout=2) is None
        assert t._state == "dead"
        assert t._hub is None and t._loop is None

    def test_stop_during_a_killed_greenlets_cleanup_still_waits_for_it(self, thread):
        """A one-shot caller timed out and its greenlet is in its (yielding)
        cleanup when stop() arrives. The teardown's cancellation must not cut
        that wait short: the caller would leave, and its hub with it."""
        killed = []
        seen = []

        def caller():
            seen.append(
                outcome(thread.call, thread.to_gevent(slow_until_killed, killed), 0.1)
            )

        start_native(caller, ())
        gevent.sleep(0.2)  # timed out at 0.1: the kill is in, the cleanup is yielding
        thread.stop()
        wait_for(seen)
        assert killed == [True]

    def test_stop_from_elsewhere_after_the_starting_thread_left(self):
        """The thread that called start() is not owed anything after that: a
        stop() from any other thread must be answered on its own hub."""
        t = AsyncioThread()
        started = []
        start_native(lambda: started.append(t.start()), ())
        wait_for(started)
        t.stop(timeout=2)
        assert t._state == "dead"
        # Nothing is owed to the starter's dead hub, and nothing keeps it.
        assert t._hub is None and t._loop is None

    def test_a_stop_that_gives_up_leaves_no_waiter_behind(self, thread):
        async def stubborn():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                await asyncio.sleep(0.5)  # a teardown that takes its time

        gevent.spawn(outcome, thread.call, stubborn())
        gevent.sleep(0.05)
        with pytest.raises(WaitTimeout):
            thread.stop(timeout=0.1)
        assert thread._stopped == []
        thread.stop(timeout=3)
        assert thread._state == "dead"

    def test_a_coroutine_that_finishes_despite_cancellation_keeps_its_answer(
        self, thread
    ):
        """stop() cancels every task; a coroutine that catches that and returns
        has an answer, and it is delivered — LoopStopped is for calls the loop
        did not finish, not a verdict on ones it did."""

        async def stubborn():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                return 42

        g = gevent.spawn(outcome, thread.call, stubborn())
        gevent.sleep(0.05)
        thread.stop()
        assert g.get(timeout=2) == 42

    def test_a_coroutine_that_never_yields_to_cancellation_is_abandoned(
        self, monkeypatch
    ):
        """stop() is bounded: a coroutine that swallows its cancellation and
        waits forever is abandoned after the unwind grace, its caller gets
        LoopStopped, and the thread ends."""
        from gisolate import asyncio_thread

        monkeypatch.setattr(asyncio_thread, "_UNWIND_GRACE", 0.3)

        async def immortal():
            while True:
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    pass

        t = AsyncioThread().start()
        g = gevent.spawn(outcome, t.call, immortal())
        gevent.sleep(0.05)
        started = time.monotonic()
        t.stop(timeout=5)
        assert time.monotonic() - started < 3.0
        assert isinstance(g.get(timeout=2), LoopStopped)
        assert t._state == "dead"
        assert t._torn == set() and t._crossings == {}  # the abandoned task is not kept

    def test_an_async_generator_that_never_finishes_closing_is_abandoned(
        self, monkeypatch
    ):
        """The teardown also closes open async generators; one whose finally
        awaits forever is abandoned after the grace, like a task."""
        from gisolate import asyncio_thread

        monkeypatch.setattr(asyncio_thread, "_UNWIND_GRACE", 0.3)
        keep = []

        async def leave_one_open():
            async def forever():
                try:
                    yield 1
                finally:
                    while True:
                        try:
                            await asyncio.sleep(10)
                        except asyncio.CancelledError:
                            pass

            keep.append(forever())  # referenced, so only the teardown will close it
            await keep[0].__anext__()

        t = AsyncioThread().start()
        t.call(leave_one_open())
        started = time.monotonic()
        t.stop(timeout=5)
        assert time.monotonic() - started < 3.0
        assert t._state == "dead"

    def test_a_cancellation_already_under_way_keeps_its_reason_through_stop(
        self, thread
    ):
        """A task cancelled with its own reason before its first step, with
        the teardown arriving before that step: the teardown must not cancel
        it again — on a task not yet started, asyncio would replace the
        reason with the teardown's."""

        async def hog():
            time.sleep(0.3)  # the loop thread is unpatched: this blocks it

        async def never_started():
            await asyncio.sleep(10)

        def cancel_then_stop():  # runs right after create(), before the task's first step
            for task in asyncio.all_tasks(thread._loop):
                task.cancel("own reason")
            thread._loop.stop()

        gevent.spawn(thread.call, hog())
        gevent.sleep(0.05)  # the hog is on the loop; what follows queues behind it
        g = gevent.spawn(outcome, thread.call, never_started())
        gevent.sleep(0)  # create() is queued
        thread._loop.call_soon_threadsafe(cancel_then_stop)  # the loop stops itself
        got = g.get(timeout=2)
        assert isinstance(got, asyncio.CancelledError) and got.args == ("own reason",)
        thread.stop(timeout=2)

    def test_a_task_spawned_during_cleanup_is_finished_by_the_teardown_too(
        self, thread, monkeypatch
    ):
        """A cancelled coroutine's cleanup may spawn a task of its own. The
        teardown cannot stop at its first snapshot: that one must end
        before the loop closes under it — even while another task holds
        out against its cancellation for the whole grace."""
        from gisolate import asyncio_thread

        monkeypatch.setattr(asyncio_thread, "_UNWIND_GRACE", 0.5)
        finished = []

        async def immortal():
            while True:
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    pass

        gevent.spawn(outcome, thread.call, immortal())

        async def child():
            try:
                await asyncio.sleep(10)
            finally:
                await asyncio.sleep(0.05)
                finished.append(True)

        async def parent():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                asyncio.get_running_loop().create_task(child())
                raise

        gevent.spawn(outcome, thread.call, parent())
        gevent.sleep(0.05)
        thread.stop()
        assert finished == [True]

    def test_a_task_in_its_own_cancellations_cleanup_still_has_its_greenlet_killed(
        self, thread
    ):
        """Cancelled with its own reason, a task may catch that and clean up
        on gevent. The teardown must still cancel it — that is what kills
        the cleanup's greenlet — even though it is already 'cancelling'."""
        killed = []
        tasks = []

        async def cleanup_on_gevent():
            tasks.append(asyncio.current_task())
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                await thread.to_gevent(slow_until_killed, killed)

        gevent.spawn(outcome, thread.call, cleanup_on_gevent())
        wait_for(tasks)
        thread._loop.call_soon_threadsafe(tasks[0].cancel, "own reason")
        gevent.sleep(0.1)  # the cleanup greenlet is up
        thread.stop()
        assert killed == [True]

    def test_a_cancelled_coroutines_async_cleanup_completes_through_stop(self, thread):
        """Cancelled with its own reason, a coroutine may still be in an
        async cleanup when stop() arrives. The teardown must not cancel it
        again: the cleanup completes, and its answer is delivered."""
        tasks = []

        async def cleanup_then_answer():
            tasks.append(asyncio.current_task())
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                await asyncio.sleep(0.2)  # cleanup that yields
                return 42

        g = gevent.spawn(outcome, thread.call, cleanup_then_answer())
        wait_for(tasks)
        thread._loop.call_soon_threadsafe(tasks[0].cancel, "own reason")
        gevent.sleep(0.05)  # in the cleanup
        thread.stop()
        assert g.get(timeout=2) == 42

    def test_a_wrapped_coroutine_is_a_coroutine_too(self, thread):
        """call() takes any Coroutine, not only a native one — including
        through the teardown."""

        class Wrapped(collections.abc.Coroutine):
            def __init__(self, coro):
                self._coro = coro

            def send(self, value):
                return self._coro.send(value)

            def throw(self, *args):
                return self._coro.throw(*args)

            def close(self):
                return self._coro.close()

            def __await__(self):
                return self._coro.__await__()

        assert thread.call(Wrapped(add(2, 3))) == 5
        g = gevent.spawn(
            outcome, thread.call, Wrapped(thread.to_gevent(time.sleep, 10))
        )
        gevent.sleep(0.05)
        thread.stop()
        assert isinstance(g.get(timeout=2), LoopStopped)

    def test_a_crossing_entered_during_the_teardown_is_ended_by_it(
        self, thread, monkeypatch
    ):
        """A coroutine may answer the teardown's own cancellation by awaiting
        to_gevent in its cleanup — a crossing that did not exist when the
        teardown swept them. It is ended all the same, and nothing is kept."""
        from gisolate import asyncio_thread

        monkeypatch.setattr(asyncio_thread, "_UNWIND_GRACE", 0.5)
        seen = []

        def linger():
            seen.append(gevent.getcurrent())
            time.sleep(10)

        async def cleanup_on_gevent():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                await thread.to_gevent(linger)

        g = gevent.spawn(outcome, thread.call, cleanup_on_gevent())
        gevent.sleep(0.05)
        thread.stop()
        assert isinstance(g.get(timeout=2), LoopStopped)
        gevent.sleep(0.05)
        assert all(greenlet.dead for greenlet in seen)
        assert thread._crossings == {}

    def test_a_translated_teardown_cancellation_is_still_loop_stopped(self, thread):
        """A coroutine may catch the teardown's cancellation and raise a
        CancelledError of its own words. The teardown cancelled it; the
        answer is LoopStopped, not the words."""

        async def translate():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                raise asyncio.CancelledError("translated") from None

        g = gevent.spawn(outcome, thread.call, translate())
        gevent.sleep(0.05)
        thread.stop()
        assert isinstance(g.get(timeout=2), LoopStopped)

    def test_stop_fails_pending_calls_and_refuses_new_ones(self, thread):
        g = gevent.spawn(outcome, thread.call, asyncio.sleep(10))
        gevent.sleep(0.05)
        thread.stop()
        assert isinstance(g.get(timeout=2), LoopStopped)
        assert thread._state == "dead"
        with pytest.raises(LoopStopped):
            thread.call(asyncio.sleep(0))

    def test_loop_death_fails_pending_calls(self, thread):
        """A loop that stops on its own fails every other in-flight call; the
        one that stopped it finished, and its answer still arrives."""

        async def die():
            asyncio.get_running_loop().stop()

        g = gevent.spawn(outcome, thread.call, asyncio.sleep(10))
        gevent.sleep(0.05)
        assert thread.call(die()) is None
        assert isinstance(g.get(timeout=2), LoopStopped)
        thread.stop(timeout=2)  # answers come during the teardown; DEAD after it
        assert thread._state == "dead"
