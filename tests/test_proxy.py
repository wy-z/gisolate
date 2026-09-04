# pyright: reportAttributeAccessIssue=false, reportGeneralTypeIssues=false
"""Tests for gisolate.proxy module (ProcessProxy)."""

import contextlib
import multiprocessing
import os
import signal
import time
from unittest.mock import MagicMock

import gevent
import pytest

from gisolate import _internal, hub
from gisolate._internal import ProcessError
from gisolate.proxy import (
    ProcessProxy,
    _proc_exited,
    get_default_mp_context,
    set_default_mp_context,
)

from .helpers import (
    HostileTraceback,
    UnprintableError,
    Unserializable,
    adder_factory,
    blocking_connect_factory,
    cancel_leaker_factory,
    cancel_swallower_factory,
    exit_once_factory,
    nested_process_error_factory,
    slow_build_factory,
    slow_marking_build,
    slow_connect_factory,
    stateful_factory,
    swallower_factory,
    sync_wrapped_async_factory,
    tracker_factory,
    unprintable_factory,
    wait_bound,
)


class TestDefaultMpContext:
    def test_default_is_spawn(self):
        set_default_mp_context(None)
        ctx = get_default_mp_context()
        assert ctx.get_start_method() == "spawn"

    def test_set_and_get(self):
        custom = multiprocessing.get_context("fork")
        set_default_mp_context(custom)
        assert get_default_mp_context() is custom
        set_default_mp_context(None)


class TestProcessProxyCreate:
    def test_create_and_call(self):
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.add(2, 3) == 5

    def test_transparent_attr_access(self):
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.echo("hello") == "hello"

    def test_remote_exception(self):
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            with pytest.raises(ValueError, match="intentional error"):
                proxy.fail()

    def test_child_runs_in_different_process(self):
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.pid() != os.getpid()

    def test_context_manager(self):
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.add(1, 1) == 2

    def test_remote_timeout_error_not_masked_asyncio_worker(self):
        """A client-raised TimeoutError must keep its message — the asyncio
        worker's deadline handling must not rewrite it (asyncio.TimeoutError
        is builtin TimeoutError since 3.10)."""
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            with pytest.raises(TimeoutError, match="quota exceeded"):
                proxy.raise_timeout()

    def test_client_cancelled_error_still_replies(self):
        """A CancelledError leaked by client code is a BaseException, so it
        escapes the asyncio worker's `except Exception` and would kill the
        handler task with no reply, stranding the caller until its own grace
        period. Cancellation of the handler task itself must still propagate."""
        with ProcessProxy.create(cancel_leaker_factory, timeout=10) as proxy:
            with pytest.raises(Exception, match="CancelledError") as excinfo:
                proxy.with_timeout(3).leak_cancelled()
            # The child's error reply, not the parent giving up.
            assert "timed out after" not in str(excinfo.value)
            assert proxy.add(1, 2) == 3  # worker still serving

    def test_unprintable_client_error_still_replies(self):
        """Formatting the error reply must not itself raise: an exception whose
        __str__ blows up would kill the handler task before send(), stranding
        the caller until its own grace period."""
        with ProcessProxy.create(unprintable_factory, timeout=10) as proxy:
            with pytest.raises(Exception) as excinfo:
                proxy.with_timeout(3).boom()
            assert excinfo.type.__name__ == "UnprintableError"  # not a timeout
            assert proxy.add(1, 2) == 3  # worker still serving

    def test_a_cancelled_build_becomes_the_client(self):
        """Same rule one step earlier: a synchronous factory runs where
        cancellation does not reach, so its client is the one the next call
        wants — not an orphan to close and build again per retry."""
        with ProcessProxy.create(slow_build_factory, timeout=10) as proxy:
            wait_bound(proxy)  # else the 0.3s is spent on child startup
            with pytest.raises(TimeoutError):
                proxy.with_timeout(0.3).ping()
            assert proxy.ping() == "pong"
            assert proxy.build_count() == 1
            assert proxy.close_count() == 0

    def test_a_build_landing_during_shutdown_is_still_closed(self, tmp_path):
        """The drain fixes its task set when it starts, and finishing tasks
        make more: a shielded build completing inside the wait schedules the
        dispose of its orphan, which the loop shutdown then cancelled."""
        import functools

        marker = tmp_path / "closed.txt"
        proxy = ProcessProxy.create(
            functools.partial(slow_marking_build, str(marker)), timeout=10
        )
        wait_bound(proxy)  # else the 0.3s is spent on child startup
        with pytest.raises(TimeoutError):
            proxy.with_timeout(0.3).ping()
        proxy.shutdown(timeout=10)  # arrives while the build is still running

        assert marker.exists() and marker.read_text().count("closed") == 1

    def test_a_build_outlasting_the_drain_is_still_closed(self, tmp_path):
        """Past the six-second drain the teardown has to join the build itself:
        off_loop's executor thread does not stop when asyncio.run cancels the
        await, so the client it goes on to return would be left with nobody to
        close it."""
        import functools

        marker = tmp_path / "closed.txt"
        proxy = ProcessProxy.create(
            functools.partial(slow_marking_build, str(marker), 7.0), timeout=10
        )
        wait_bound(proxy)  # else the 0.3s is spent on child startup
        with pytest.raises(TimeoutError):
            proxy.with_timeout(0.3).ping()
        proxy.shutdown(timeout=20)  # drain expires with the build still running

        assert marker.exists() and marker.read_text().count("closed") == 1

    def test_a_factory_that_exits_does_not_take_the_worker_with_it(self):
        """SystemExit raised inside a task is re-raised into the event loop by
        Task.__step and unwinds asyncio.run. The build is a task of its own, so
        a client library calling sys.exit over its own bad configuration ended
        the worker — and under serve(), every attached client's service."""
        with ProcessProxy.create(exit_once_factory, timeout=10) as proxy:
            with pytest.raises(Exception) as excinfo:
                proxy.add(1, 2)
            assert "SystemExit" in str(excinfo.value)
            assert proxy.add(1, 2) == 3  # same worker, still serving

    def test_private_attr_raises(self):
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            with pytest.raises(AttributeError):
                proxy._secret

    def test_a_cancelled_connect_keeps_the_client_it_built(self):
        """The worker owns initialisation, not whichever call arrived first: a
        caller losing interest does not make the client invalid, and connect()
        cannot be stopped anyway — off_loop runs it where cancellation does not
        reach. So the build finishes and becomes the client every later call
        gets, instead of being closed and started again."""
        with ProcessProxy.create(slow_connect_factory, timeout=10) as proxy:
            wait_bound(proxy)  # else the 0.3s is spent on child startup
            with pytest.raises(TimeoutError):
                proxy.with_timeout(0.3).is_ready()
            assert proxy.is_ready() is True  # the same build, finished
            assert proxy.connect_count() == 1  # and only one ever ran
            assert proxy.close_count() == 0  # nothing was thrown away


class TestProcessProxySubclass:
    def test_subclass_pattern(self):
        class AdderProxy(ProcessProxy):
            client_factory = staticmethod(adder_factory)

        with AdderProxy() as proxy:
            assert proxy.add(10, 20) == 30


class TestProcessProxyRestart:
    def test_restart_process(self):
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            pid1 = proxy.pid()
            proxy.restart_process()
            pid2 = proxy.pid()
            assert pid1 != pid2
            assert pid2 != os.getpid()

    def test_shutdown_then_execute_raises(self):
        proxy = ProcessProxy.create(adder_factory, timeout=10)
        proxy.shutdown()
        with pytest.raises(RuntimeError, match="shutdown"):
            proxy.add(1, 2)

    def test_reaped_child_detected_and_restarted(self):
        """A gevent parent's libev loop steals child reaps via its SIGCHLD
        handler; multiprocessing's ``waitpid`` then fails with ECHILD and
        ``is_alive()`` answers True forever. The proxy must see through the
        lie (q-trade #435: a segfaulted child passed every liveness check
        for four hours)."""
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.echo("warm") == "warm"
            process = proxy._process
            pid = process.pid
            os.kill(pid, signal.SIGKILL)
            # Steal the reap exactly as libev's child watcher does. It may
            # legitimately have beaten us to it — same end state.
            with contextlib.suppress(ChildProcessError):
                os.waitpid(pid, 0)
            # mp bookkeeping is now permanently wrong…
            assert process.is_alive()
            # …but the sentinel is not.
            assert not proxy._is_alive()
            # The next call restarts the child and serves.
            assert proxy.echo("back") == "back"
            assert proxy.pid() != pid

    def test_dead_child_in_cooldown_fails_fast(self):
        """A child dying within the cooldown window must not respawn on
        every execute; the call fails fast instead of burning its timeout."""
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            proxy.restart_process()  # stamps _last_restart
            pid = proxy._process.pid
            os.kill(pid, signal.SIGKILL)
            with contextlib.suppress(ChildProcessError):
                os.waitpid(pid, 0)
            start = time.monotonic()
            # "not running" (restart skipped) or "disconnected" (reader won
            # the race and flushed) — both are the intended fast failure.
            with pytest.raises(ProcessError):
                proxy.add(1, 2)
            assert time.monotonic() - start < proxy.timeout  # no rpc-timeout burn

    def test_teardown_racing_registration_fails_fast(self):
        """_stop can land between the liveness check and request registration
        (_is_alive polls the sentinel, which yields). The request is then
        pending with no socket: it must raise, not sit out the rpc timeout."""
        with ProcessProxy.create(adder_factory, timeout=5) as proxy:
            assert proxy.add(1, 2) == 3
            alive, fired = proxy._is_alive, []

            def racing_is_alive():
                ok = alive()
                if ok and not fired:  # once, at the exact window
                    fired.append(1)
                    proxy._stop()
                return ok

            proxy._is_alive = racing_is_alive
            start = time.monotonic()
            with pytest.raises(ProcessError):
                proxy.add(1, 2)
            assert time.monotonic() - start < proxy.timeout


class TestProcessProxyGeventWorker:
    def test_with_patch_kwargs(self):
        with ProcessProxy.create(
            adder_factory,
            timeout=10,
            patch_kwargs={"thread": False, "os": False},
        ) as proxy:
            assert proxy.add(5, 6) == 11

    def test_remote_timeout_error_not_masked(self):
        """A TimeoutError raised BY the remote method must propagate with its
        original message, not be rewritten as a local wait-timeout."""
        with ProcessProxy.create(
            adder_factory,
            timeout=10,
            patch_kwargs={"thread": False, "os": False},
        ) as proxy:
            with pytest.raises(TimeoutError, match="quota exceeded"):
                proxy.raise_timeout()


class TestProcessProxyConcurrency:
    def test_concurrent_calls(self):
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            greenlets = [gevent.spawn(proxy.add, i, i) for i in range(10)]
            gevent.joinall(greenlets, timeout=15)
            results = sorted(g.value for g in greenlets if g.value is not None)
            assert results == [i * 2 for i in range(10)]


class TestMaxConcurrency:
    def test_asyncio_worker_limits_concurrency(self):
        with ProcessProxy.create(
            tracker_factory, timeout=10, max_concurrency=2
        ) as proxy:
            greenlets = [gevent.spawn(proxy.run, 0.3) for _ in range(6)]
            gevent.joinall(greenlets, timeout=15)
            peak = proxy.get_peak()
            assert peak <= 2

    def test_gevent_worker_limits_concurrency(self):
        with ProcessProxy.create(
            tracker_factory,
            timeout=10,
            max_concurrency=2,
            patch_kwargs={"thread": False, "os": False},
        ) as proxy:
            greenlets = [gevent.spawn(proxy.run, 0.3) for _ in range(6)]
            gevent.joinall(greenlets, timeout=15)
            peak = proxy.get_peak()
            assert peak <= 2

    def test_unlimited_concurrency_by_default(self):
        with ProcessProxy.create(tracker_factory, timeout=10) as proxy:
            greenlets = [gevent.spawn(proxy.run, 0.3) for _ in range(6)]
            gevent.joinall(greenlets, timeout=15)
            peak = proxy.get_peak()
            assert peak > 2

    def test_asyncio_saturated_pool_keeps_consuming_requests(self):
        """A client that swallows the cancellation its deadline raises keeps
        the only slot for good — unbeatable, as in the gevent worker. What must
        not happen is the queued call going unanswered: its slot was acquired
        outside any deadline, so its handler waited there indefinitely and sent
        no reply at all. See TestGeventWorkerDeadlineIsolation for the twin."""
        with ProcessProxy.create(
            cancel_swallower_factory, timeout=5, max_concurrency=1
        ) as proxy:
            assert proxy.add(0, 0) == 0  # warmup: child booted and responsive
            hog = gevent.spawn(proxy.with_timeout(30).swallow_and_hang)
            gevent.sleep(0.5)  # hog occupies the only slot
            try:
                # The child's own deadline reply ("add timed out"), not the
                # parent giving up after its grace period ("... after 1s").
                with pytest.raises(TimeoutError) as excinfo:
                    proxy.with_timeout(1).add(1, 2)
                assert "after" not in str(excinfo.value)
            finally:
                hog.kill(block=False)


def _swallower_proxy():
    return ProcessProxy.create(
        swallower_factory,
        timeout=5,
        max_concurrency=1,
        patch_kwargs={"thread": False, "os": False},
    )


class TestGeventWorkerDeadlineIsolation:
    """The gevent worker's per-call deadline must hold even against client
    code that swallows the injected TimeoutError, and saturated admission
    slots must never stop the worker from consuming requests."""

    def test_swallowed_timeout_still_replies_and_frees_slot(self):
        with _swallower_proxy() as proxy:
            with pytest.raises(TimeoutError) as excinfo:
                proxy.with_timeout(1).swallow_and_hang()
            # The child's own deadline reply ("... timed out"), not the
            # parent giving up after its grace period ("... after 1s").
            assert "after" not in str(excinfo.value)
            # The hung call's slot must be reclaimed: the next call runs.
            assert proxy.add(1, 2) == 3

    def test_saturated_pool_keeps_consuming_requests(self):
        with _swallower_proxy() as proxy:
            assert proxy.add(0, 0) == 0  # warmup: child booted and responsive
            hog = gevent.spawn(proxy.with_timeout(30).swallow_and_hang)
            gevent.sleep(0.5)  # hog occupies the only slot
            try:
                # This queued request must not block the receive loop: the
                # child must still read it and reply with its own deadline
                # ("add timed out"), not leave the parent to give up on an
                # unresponsive worker ("... after 1s").
                with pytest.raises(TimeoutError) as excinfo:
                    proxy.with_timeout(1).add(1, 2)
                assert "after" not in str(excinfo.value)
            finally:
                hog.kill(block=False)

    def test_slot_stays_held_until_killed_call_unwinds(self):
        """A timed-out call's slot must not be reusable while its GreenletExit
        unwind (e.g. a yielding finally) is still running — max_concurrency
        would silently be violated."""
        with _swallower_proxy() as proxy:
            assert proxy.add(0, 0) == 0  # warmup
            with pytest.raises(TimeoutError):
                proxy.with_timeout(1).hang_with_slow_cleanup()
            with pytest.raises(TimeoutError):
                proxy.with_timeout(1).hang_with_slow_cleanup()
            gevent.sleep(2.5)  # let cleanups finish
            assert proxy.get_peak() == 1

    def test_client_base_exception_still_replies(self):
        """A BaseException from client code (gevent.Timeout is one) must come
        back as an error; killing the invoke greenlet with no reply would
        strand the caller until its own grace period."""
        with _swallower_proxy() as proxy:
            with pytest.raises(Exception, match="Timeout") as excinfo:
                proxy.with_timeout(3).escaping_base_exception()
            # The child's error reply, not the parent giving up.
            assert "timed out after" not in str(excinfo.value)

    def test_unprintable_client_error_still_replies(self):
        """Same hazard on the gevent side: a raising __str__ must not escape
        the invoke greenlet and turn the client's error into a bogus one."""
        with ProcessProxy.create(
            unprintable_factory, timeout=5, patch_kwargs={"thread": False, "os": False}
        ) as proxy:
            with pytest.raises(Exception) as excinfo:
                proxy.with_timeout(3).boom()
            assert excinfo.type.__name__ == "UnprintableError"

    def test_client_raised_greenlet_exit_is_an_error(self):
        """gevent treats GreenletExit as a *successful* greenlet outcome; the
        worker must not forward it as an OK result."""
        with _swallower_proxy() as proxy:
            with pytest.raises(Exception, match="self_kill killed"):
                proxy.self_kill()


class TestPerCallTimeout:
    def test_execute_timeout_overrides_default(self):
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            with pytest.raises(TimeoutError):
                proxy._execute("slow", (5,), {}, 0.5)

    def test_with_timeout_overrides_default(self):
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            with pytest.raises(TimeoutError):
                proxy.with_timeout(0.5).slow(5)

    def test_with_timeout_allows_longer(self):
        with ProcessProxy.create(adder_factory, timeout=1) as proxy:
            assert proxy.with_timeout(10).slow(0.5) == "done"

    def test_timeout_kwarg_forwarded_to_remote(self):
        """Ensure 'timeout' kwarg is not consumed by execute()."""
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.echo_timeout(timeout=42) == 42

    def test_outer_gevent_timeout_not_relabeled(self):
        """A caller's enclosing gevent.Timeout firing while the call waits
        must surface as that caller's Timeout (their cancellation), not be
        rewritten into this call's TimeoutError."""
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            timer = gevent.Timeout.start_new(0.3)
            try:
                with pytest.raises(gevent.Timeout) as excinfo:
                    proxy.slow(5)
                assert excinfo.value is timer
            finally:
                timer.close()

    def test_outer_timeout_during_stop_not_swallowed(self):
        """A caller's enclosing gevent.Timeout firing while _stop waits on
        the reader must propagate as their cancellation — gevent's kill never
        raises its own timeout, so suppressing Timeout there could only ever
        swallow the caller's deadline. _stop's teardown must still complete:
        the child is reaped, not orphaned."""
        proxy = ProcessProxy.create(adder_factory, timeout=10)
        proc = proxy._process
        try:
            assert proxy.add(1, 1) == 2
            # Wedge the reader-kill step so the caller's timer fires inside it.
            proxy._reader.kill = lambda *_a, **_k: gevent.sleep(5)
            timer = gevent.Timeout.start_new(0.3)
            try:
                with pytest.raises(gevent.Timeout) as excinfo:
                    proxy.shutdown()
                assert excinfo.value is timer
            finally:
                timer.close()
            # The interrupt fired mid-teardown, after state was detached;
            # _stop's finally must still reap the child rather than leak it
            # with no handle left to reach it.
            assert _proc_exited(proc)
        finally:
            if not _proc_exited(proc):
                proc.terminate()
                proc.join(timeout=1)

    def test_outer_timeout_during_cleanup_fails_pending(self):
        """The finally's own teardown is interruptible too: _cleanup_process's
        process.join()s are gevent switch points. A caller's enclosing
        gevent.Timeout firing *inside* teardown (not just the try-block waits
        the sibling test covers) must not skip failing pending waiters — they
        would hang forever. Teardown runs on a detached greenlet that finishes
        regardless of our interrupted join."""
        proxy = ProcessProxy.create(adder_factory, timeout=10)
        proc = proxy._process
        waiter = gevent.event.AsyncResult()
        try:
            assert proxy.add(1, 1) == 2
            # A pending waiter teardown must fail. Wedge a finally-stage step
            # so the caller's timer fires inside teardown, before the
            # pending-fail loop that runs after _cleanup_process.
            proxy._pending[999] = waiter
            orig_cleanup = proxy._cleanup_process

            def slow_cleanup(p):
                gevent.sleep(0.5)
                orig_cleanup(p)

            proxy._cleanup_process = slow_cleanup
            timer = gevent.Timeout.start_new(0.2)
            try:
                with pytest.raises(gevent.Timeout) as excinfo:
                    proxy.shutdown()
                assert excinfo.value is timer
            finally:
                timer.close()
            # shutdown() raised mid-teardown, but the detached greenlet keeps
            # going: the pending waiter is failed rather than left hanging, and
            # the child is still reaped.
            for _ in range(60):
                if waiter.ready():
                    break
                gevent.sleep(0.05)
            assert isinstance(waiter.exception, ProcessError)
            assert _proc_exited(proc)
        finally:
            if not _proc_exited(proc):
                proc.terminate()
                proc.join(timeout=1)


class TestCrossThreadCalls:
    """Calls from a native (non-owner) OS thread must marshal the socket
    send to the main hub — the zmq.green socket is single-thread-owned."""

    def test_call_from_native_thread(self):
        Thread = gevent.monkey.get_original("threading", "Thread")
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            box = {}

            def worker():
                try:
                    box["result"] = proxy.add(7, 8)
                except Exception as e:  # noqa: BLE001
                    box["error"] = e

            t = Thread(target=worker)
            t.start()
            t.join(timeout=15)

        assert box.get("error") is None
        assert box["result"] == 15

    def test_concurrent_native_threads(self):
        Thread = gevent.monkey.get_original("threading", "Thread")
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            results: list[int] = []
            lock = gevent.monkey.get_original("threading", "Lock")()

            def worker(i):
                r = proxy.add(i, i)
                with lock:
                    results.append(r)

            threads = [Thread(target=worker, args=(i,)) for i in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=15)

        assert sorted(results) == [2 * i for i in range(8)]

    def test_send_fails_fast_when_hub_unresponsive(self, monkeypatch):
        """A wedged main hub (marshaled callbacks never run) must make a
        non-owner send time out and raise TimeoutError instead of blocking
        forever — and must NOT trigger a (futile) child restart."""
        Thread = gevent.monkey.get_original("threading", "Thread")
        with ProcessProxy.create(adder_factory, timeout=0.5) as proxy:
            # Simulate a wedged hub: scheduled callbacks are dropped.
            monkeypatch.setattr(hub, "_schedule", lambda *_: None)
            restarts: list[int] = []
            monkeypatch.setattr(proxy, "restart_process", lambda: restarts.append(1))

            box: dict = {}

            def worker():
                try:
                    proxy.add(1, 2)
                except Exception as e:  # noqa: BLE001
                    box["error"] = e

            t = Thread(target=worker)
            t.start()
            t.join(timeout=10)

        err = box.get("error")
        assert isinstance(err, TimeoutError)
        assert "main hub unresponsive" in str(err)
        assert restarts == []

    def test_a_restart_from_a_native_thread_keeps_the_owner(self):
        """Under patch_all the marshal guard does not fire for a raw OS thread,
        so _start runs inline there. Re-recording the owner made that thread the
        owner, and every later call from it then sent on the socket directly —
        against the reader greenlet on the main hub that owns it."""
        Thread = gevent.monkey.get_original("threading", "Thread")
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.add(1, 1) == 2
            owner = proxy._owner
            box: dict = {}

            def worker():
                try:
                    proxy.restart_process()
                    box["owner"] = proxy._owner
                except BaseException as e:  # noqa: BLE001
                    box["error"] = e

            t = Thread(target=worker, daemon=True)
            t.start()
            # Cooperatively, not Thread.join: the restart now marshals, and a
            # blocking join would stop the very hub it is waiting on.
            deadline = time.monotonic() + 30
            while t.is_alive() and time.monotonic() < deadline:
                gevent.sleep(0.05)
            assert not t.is_alive(), "the marshal never came back"
            assert "error" not in box, box["error"]
            assert box["owner"] is owner, "a restart took ownership of the socket"
            assert proxy.add(2, 3) == 5

    def test_lifecycle_from_a_native_thread_marshals(self, monkeypatch):
        """Under patch_all, gevent reports a raw OS thread's hub as the default
        one, so a hub-based guard could not tell it from the owner — and _start,
        restart_process and shutdown all ran inline on it, spawning greenlets
        and sending on the socket the main hub's reader owns."""
        Thread = gevent.monkey.get_original("threading", "Thread")
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.add(1, 1) == 2
            marshalled: list[str] = []
            real = hub.run_on_main_hub

            def spy(func, timeout=None):
                marshalled.append(getattr(func, "__name__", type(func).__name__))
                return real(func, timeout)

            monkeypatch.setattr(hub, "run_on_main_hub", spy)
            box: dict = {}

            def worker():
                try:
                    proxy._start()
                    proxy.restart_process()
                    proxy.shutdown()
                except BaseException as e:  # noqa: BLE001
                    box["error"] = e

            t = Thread(target=worker, daemon=True)
            t.start()
            deadline = time.monotonic() + 30
            while t.is_alive() and time.monotonic() < deadline:
                gevent.sleep(0.05)
            assert not t.is_alive(), "a marshal never came back"
            assert "error" not in box, box["error"]
            assert len(marshalled) == 3, marshalled

    def test_the_wait_spends_what_the_marshal_left(self):
        """The send marshal and the reply wait spend ONE budget. The wait used
        to start its own full clock after the marshal returned, so a call whose
        marshal took most of its timeout then waited that long again — twice
        the deadline the caller asked for, against a worker that had already
        given up on it."""
        import tempfile
        import uuid

        Thread = gevent.monkey.get_original("threading", "Thread")
        # Attached, with no host: nothing ever replies, so the wait is the only
        # thing that can end the call.
        address = f"ipc://{tempfile.gettempdir()}/gi-none-{uuid.uuid4().hex[:8]}.sock"
        proxy = ProcessProxy.attach(address, timeout=4)
        try:
            original = proxy._raw_send

            def slow_marshal(req_id, frames):
                gevent.sleep(3.5)  # a hub that is busy with something else
                original(req_id, frames)

            proxy._raw_send = slow_marshal
            box: dict = {}

            def worker():
                start = time.monotonic()
                try:
                    proxy.with_timeout(4).add(1, 2)
                except BaseException as e:  # noqa: BLE001
                    box["error"] = e
                box["elapsed"] = time.monotonic() - start

            t = Thread(target=worker, daemon=True)
            t.start()
            deadline = time.monotonic() + 30
            while t.is_alive() and time.monotonic() < deadline:
                gevent.sleep(0.05)
            assert not t.is_alive(), "the call never came back"
            assert isinstance(box.get("error"), TimeoutError), box.get("error")
            assert box["elapsed"] < 8.0, f"took {box['elapsed']:.1f}s of a 4s budget"
        finally:
            proxy.shutdown()

    def test_remote_timeout_error_not_masked_cross_thread(self):
        """Non-owner path (hub.AsyncResult): a remote TimeoutError must not be
        rewritten as a local wait-timeout — the two share a type here, unlike
        the owner path where wait-timeouts are gevent.Timeout."""
        Thread = gevent.monkey.get_original("threading", "Thread")
        with ProcessProxy.create(
            adder_factory,
            timeout=10,
            patch_kwargs={"thread": False, "os": False},
        ) as proxy:
            box: dict = {}

            def worker():
                try:
                    proxy.raise_timeout()
                except Exception as e:  # noqa: BLE001
                    box["error"] = e

            t = Thread(target=worker)
            t.start()
            t.join(timeout=15)

        err = box.get("error")
        assert isinstance(err, TimeoutError)
        assert "quota exceeded" in str(err)

    def test_raw_send_drops_stale_request(self):
        """The _raw_send guard: a late marshaled send must no-op once the
        request is gone (timed out / socket restarted), so a stale frame is
        never delivered to a replacement socket."""
        proxy = ProcessProxy.create(adder_factory, timeout=10)
        proxy.shutdown()  # tears down the live process: _transport=None, _pending cleared
        stub = MagicMock()
        proxy._transport = _internal.ZmqTransport(
            stub, None, "ipc:///stub", _internal.IpcLease.none()
        )

        # Stale: req_id not pending -> dropped.
        proxy._raw_send(123, [b"a", b"b"])
        stub.send_multipart.assert_not_called()

        # Live: req_id pending -> forwarded.
        proxy._pending[7] = hub.AsyncResult()
        proxy._raw_send(7, [b"c", b"d"])
        stub.send_multipart.assert_called_once()


class TestSendFailureIsolation:
    def test_an_unserializable_argument_spares_the_worker(self):
        """A payload that cannot even be built never reached the socket, so it
        says nothing about the child's health. Treated as a send failure, one
        caller's bad argument restarted a healthy worker — losing the client's
        in-memory state — and _stop() failed every unrelated call in flight."""
        with ProcessProxy.create(stateful_factory, timeout=10) as proxy:
            proxy.remember("alpha")
            pid_before = proxy.pid()

            with pytest.raises(Exception) as excinfo:
                proxy.remember(Unserializable())
            assert not isinstance(excinfo.value, ProcessError)

            assert proxy.pid() == pid_before  # same child
            assert proxy.recall() == "alpha"  # with its state intact


class TestStolenReapCleanup:
    def test_a_stolen_reap_does_not_retain_the_child(self):
        """join() drops a process from multiprocessing's _children only when
        wait() returns a status, and after libev steals the reap waitpid fails
        with ECHILD for ever. Measured over three killed children: each left its
        Process in that set and leaked two descriptors — and multiprocessing's
        own atexit hook then terminates a pid that may have been recycled."""
        import multiprocessing.process as mpp

        def open_fds():
            return len(os.listdir("/dev/fd"))

        before_children, before_fds = len(mpp._children), open_fds()
        for _ in range(3):
            proxy = ProcessProxy.create(adder_factory, timeout=5)
            assert proxy.add(1, 1) == 2
            pid = proxy._process.pid
            os.kill(pid, signal.SIGKILL)
            with contextlib.suppress(ChildProcessError):
                os.waitpid(pid, 0)  # steal the reap, exactly as libev does
            proxy.shutdown()

        assert len(mpp._children) == before_children
        assert open_fds() <= before_fds


class TestLaunchFailureAfterTheChildExists:
    def test_the_child_is_reaped(self, monkeypatch):
        """Process.start() is not atomic: spawn creates the child, records its
        pid and sentinel, and only then writes the bootstrap payload. A failure
        there strands a child multiprocessing never registered — measured,
        nobody reaps it, so it is a zombie until this process exits."""
        import multiprocessing.popen_spawn_posix as spawn_posix

        pids: list[int] = []
        real_launch = spawn_posix.Popen._launch

        def launch_then_fail(self, process_obj):
            real_launch(self, process_obj)  # the child exists from here on
            pids.append(self.pid)
            raise BrokenPipeError("bootstrap write failed")

        monkeypatch.setattr(spawn_posix.Popen, "_launch", launch_then_fail)
        with pytest.raises(BrokenPipeError):
            ProcessProxy.create(adder_factory, timeout=5)
        monkeypatch.undo()

        assert pids, "the child was never created; the test proves nothing"
        with pytest.raises(ChildProcessError):
            os.waitpid(pids[0], os.WNOHANG)  # reaped, not left a zombie

    def test_a_failure_before_the_sentinel_still_reaps(self):
        """The pid is recorded before the sentinel, so a failure between them
        leaves a child every cleanup path here would trip over: they all join,
        and join reads that sentinel."""
        import multiprocessing.popen_spawn_posix as spawn_posix

        pids: list[int] = []
        bound: list[str] = []
        real_launch = spawn_posix.Popen._launch

        def launch_then_fail(self, process_obj):
            real_launch(self, process_obj)
            pids.append(self.pid)
            # As a fork child would have: bound before the parent noticed.
            bound.append(process_obj._args[0].ipc_addr.removeprefix("ipc://"))
            open(bound[0], "wb").close()
            del self.sentinel  # as if it had raised one line earlier
            raise BrokenPipeError("interrupted before the sentinel")

        spawn_posix.Popen._launch = launch_then_fail
        try:
            with pytest.raises(BrokenPipeError):
                ProcessProxy.create(adder_factory, timeout=5)
        finally:
            spawn_posix.Popen._launch = real_launch

        assert pids, "the child was never created; the test proves nothing"
        with pytest.raises(ChildProcessError):
            os.waitpid(pids[0], os.WNOHANG)
        # And the socket such a child may already have bound goes with it: the
        # lease is the last handle anyone has on that file.
        assert bound and not os.path.exists(bound[0])

    def test_a_custom_launcher_is_left_alone(self):
        """A context may answer "spawn" while launching its own way — passing
        descriptors, or doing setup of its own. Replacing that with the stdlib
        launcher starts a child without it."""
        from gisolate import proxy as proxy_mod

        class OwnLauncher(multiprocessing.get_context("spawn").Process):
            @staticmethod
            def _Popen(process_obj):  # pragma: no cover - never launched here
                raise AssertionError("the custom launcher should be used")

        process = OwnLauncher(target=len, args=((),))
        proxy_mod._launch_recoverably(process)
        assert "_Popen" not in vars(process), "gisolate replaced a custom launcher"

    def test_a_recovered_child_is_tracked(self, monkeypatch):
        """Our own cleanup gives up on a child that outlives its kill grace.
        Registered with multiprocessing, the interpreter's exit handler finishes
        what that grace could not; unregistered, nothing ever looks at it."""
        import multiprocessing.popen_spawn_posix as spawn_posix

        seen: list = []
        real_launch = spawn_posix.Popen._launch

        def launch_then_fail(self, process_obj):
            real_launch(self, process_obj)
            seen.append(process_obj)
            raise BrokenPipeError("bootstrap write failed")

        monkeypatch.setattr(spawn_posix.Popen, "_launch", launch_then_fail)
        with pytest.raises(BrokenPipeError):
            ProcessProxy.create(adder_factory, timeout=5)
        monkeypatch.undo()
        assert seen and seen[0]._popen is not None


class TestReapBeforeForget:
    def test_an_exited_child_is_reaped_before_it_is_forgotten(self):
        """The sentinel says the child is gone, and "gone" is not "reaped":
        with nobody having called waitpid it is a zombie, and dropping the
        Process leaves it one until this process exits."""
        from gisolate import proxy as proxy_mod

        proxy = ProcessProxy.create(adder_factory, timeout=5)
        try:
            assert proxy.add(1, 1) == 2
            process = proxy._process
            pid = process.pid
            process.terminate()
            for _ in range(200):
                if proxy_mod._proc_exited(process):
                    break
                gevent.sleep(0.05)
            assert proxy_mod._proc_exited(process)

            proxy_mod._forget_reaped(process)  # with no join before it
            with pytest.raises(ChildProcessError):
                os.waitpid(pid, os.WNOHANG)  # already reaped, not a zombie
        finally:
            proxy.shutdown()


class TestLiveChildIsNotForgotten:
    def test_a_running_child_keeps_its_tracking(self, monkeypatch):
        """`returncode is None` says both "somebody else reaped it" and "it is
        still running". A child that outlived even the SIGKILL grace would
        otherwise have its descriptors closed and its tracking dropped while
        alive, with nothing left to reap it."""
        import multiprocessing.process as mpp

        from gisolate import proxy as proxy_mod

        with ProcessProxy.create(adder_factory, timeout=5) as proxy:
            assert proxy.add(1, 1) == 2
            process = proxy._process
            monkeypatch.setattr(proxy_mod, "_proc_exited", lambda _p: False)
            proxy_mod._forget_reaped(process)
            assert process in mpp._children


class TestRevivalMarshal:
    def test_a_revival_from_a_spawned_greenlet_terminates(self, monkeypatch):
        """current_thread is per GREENLET, so the greenlet a marshal spawns
        tests again, decides it is foreign, and marshals itself forever —
        measured at 21 hops and still climbing. The marshal target has to be the
        body, the same split _start already uses."""
        with ProcessProxy.create(adder_factory, timeout=5) as proxy:
            assert proxy.add(1, 1) == 2
            pid = proxy._process.pid
            os.kill(pid, signal.SIGKILL)
            with contextlib.suppress(ChildProcessError):
                os.waitpid(pid, 0)

            marshals: list[int] = []
            real = hub.run_on_main_hub

            def counting(func, timeout=None):
                marshals.append(1)
                if len(marshals) > 20:
                    raise RuntimeError("remarshal loop")
                return real(func, timeout)

            monkeypatch.setattr(hub, "run_on_main_hub", counting)
            call = gevent.spawn(proxy.add, 2, 3)  # a greenlet, not the main one
            call.join(timeout=30)
            assert call.successful(), call.exception
            assert call.value == 5
            assert len(marshals) < 5, f"{len(marshals)} marshals for one revival"


class TestRevivalCoalescing:
    def test_a_late_reviver_leaves_the_new_generation_alone(self):
        """Two callers that saw the same dead transport both reach the restart.
        The second would tear down the generation the first just built — and an
        attached proxy skips the cooldown that covers this for an owner, on
        purpose, because rebuilding a socket strands nobody."""
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.add(1, 1) == 2
            stale = proxy._transport

            proxy.restart_process()  # the first caller rebuilds
            fresh = proxy._transport
            assert fresh is not stale

            # The second arrives late, holding the generation it found dead.
            proxy._revive(stale, time.monotonic() + 10)
            assert proxy._transport is fresh, "the late reviver took it down"
            assert proxy.add(2, 3) == 5


class TestRemoteErrorsAreAnswers:
    def test_a_remote_process_error_does_not_count_as_a_transport_failure(self):
        """A ProcessError the WORKER raised — a nested proxy's — is this call's
        answer, not evidence about our transport. Counted as one, six honest
        ones in a row restarted a perfectly healthy worker."""
        with ProcessProxy.create(nested_process_error_factory, timeout=10) as proxy:
            pid_before = proxy.pid()
            for _ in range(proxy.auto_restart_threshold + 2):
                with pytest.raises(ProcessError, match="inner proxy is down"):
                    proxy.relay()
            assert proxy.pid() == pid_before, "a healthy worker was restarted"


class TestRestartStormIsolation:
    def test_a_restarts_own_failures_do_not_restart_it_again(self):
        """A restart fails every pending call at once. Counting those crossed
        auto_restart_threshold, so the callers of the healthy new generation
        restarted IT — and an attached proxy has no cooldown to throttle that."""
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.add(1, 1) == 2
            restarts = []
            original = proxy._restart_on_owner

            def counting():
                restarts.append(1)
                original()

            proxy._restart_on_owner = counting

            # More pending calls than the threshold, all failed by one restart.
            assert proxy.auto_restart_threshold < 12
            calls = [gevent.spawn(proxy.slow, 30) for _ in range(12)]
            gevent.sleep(0.5)  # let them register
            proxy.restart_process()
            gevent.joinall(calls, timeout=20)

            assert len(restarts) == 1, f"{len(restarts)} restarts for one death"
            assert proxy.add(2, 3) == 5


class TestBackpressureIsolation:
    def test_a_full_send_queue_spares_the_worker(self):
        """A DEALER whose peer is slow — or an attached host that is restarting
        — fills its outgoing queue and NOBLOCK raises. Reported as a transport
        failure it took the restart path, which fails every OTHER pending call
        and, for an owned worker, throws away its client's state."""
        import zmq

        with ProcessProxy.create(stateful_factory, timeout=10) as proxy:
            assert proxy.remember("alpha")
            pid_before = proxy.pid()
            assert proxy._transport is not None
            sock = proxy._transport.sock
            real_send = sock.send_multipart

            def queue_full(*_args, **_kwargs):
                raise zmq.Again()

            sock.send_multipart = queue_full
            with pytest.raises(ProcessError):
                proxy.recall()
            sock.send_multipart = real_send

            assert proxy.pid() == pid_before  # same child
            assert proxy.recall() == "alpha"  # with its state intact


class TestStartInProgress:
    def test_a_failed_start_can_be_retried(self, monkeypatch):
        """The in-progress flag is what keeps two greenlets from each spawning
        a child. Setting it before the fallible work meant a failure there left
        it set for good, and every later start returned at the guard."""
        import zmq.green

        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.add(1, 1) == 2
            proxy._stop()

            def no_context():
                raise zmq.ZMQError(zmq.EINVAL)

            monkeypatch.setattr(zmq.green, "Context", no_context)
            with pytest.raises(zmq.ZMQError):
                proxy._start()
            assert not proxy._starting  # not wedged
            monkeypatch.undo()
            assert proxy.add(2, 3) == 5  # and it recovers


class TestFailedStartCleansUp:
    def test_a_failure_after_spawn_leaves_no_socket_file(self, monkeypatch):
        """The child binds the address, and a start that fails after spawning it
        publishes nothing — so _stop has no address to unlink later and the
        failure path is the only thing that can. Left behind, the file sits in
        the per-uid ipc dir for good."""
        from gisolate import proxy as proxy_mod

        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.add(1, 1) == 2
            proxy._stop()
            before = set(os.listdir(proxy_mod._ZMQ_TMPDIR))
            bound = []

            def fail_once_bound(*_args, **_kwargs):
                # Fail only after the child is really listening: terminating it
                # mid-boot leaves no file, and the test would prove nothing.
                deadline = time.monotonic() + 10
                while not bound and time.monotonic() < deadline:
                    bound.extend(set(os.listdir(proxy_mod._ZMQ_TMPDIR)) - before)
                    gevent.sleep(0.05)
                raise RuntimeError("post-spawn failure")

            monkeypatch.setattr(proxy_mod.log, "info", fail_once_bound)
            with pytest.raises(RuntimeError):
                proxy._start()
            monkeypatch.undo()

            assert bound, "child never bound its socket"
            assert not set(os.listdir(proxy_mod._ZMQ_TMPDIR)) - before
            assert proxy.add(2, 3) == 5  # and the proxy still recovers

    def test_an_interrupted_failed_start_still_reaps_the_child(self, monkeypatch):
        """That cleanup is interruptible: _cleanup_process's join()s are gevent
        switch points, and nothing was published for a later _stop to find — so
        a caller's enclosing timeout landing inside it strands a live child and
        its socket file with no handle left to reach either."""
        from gisolate import proxy as proxy_mod

        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.add(1, 1) == 2
            proxy._stop()
            before = set(os.listdir(proxy_mod._ZMQ_TMPDIR))
            bound, timers, children = [], [], []

            def fail_once_bound(*_args, **_kwargs):
                deadline = time.monotonic() + 10
                while not bound and time.monotonic() < deadline:
                    bound.extend(set(os.listdir(proxy_mod._ZMQ_TMPDIR)) - before)
                    gevent.sleep(0.05)
                # Started here, so it fires inside the cleanup below rather
                # than during the spawn we just waited out.
                timers.append(gevent.Timeout.start_new(0.2))
                raise RuntimeError("post-spawn failure")

            original_cleanup = proxy._cleanup_process

            def slow_cleanup(process):
                children.append(process)
                gevent.sleep(0.5)  # wedged, so the timer lands in here
                original_cleanup(process)

            monkeypatch.setattr(proxy_mod.log, "info", fail_once_bound)
            proxy._cleanup_process = slow_cleanup
            try:
                with pytest.raises(gevent.Timeout) as excinfo:
                    proxy._start()
                assert excinfo.value is timers[0]
            finally:
                timers[0].close()
            monkeypatch.undo()

            assert bound, "child never bound its socket"
            # The detached cleanup unlinks the file a hub turn after the child
            # is seen dead, so wait for both: checking the directory the
            # instant the sentinel turned readable raced it (measured on
            # Linux, where this greenlet won).
            for _ in range(60):
                if (
                    children
                    and _proc_exited(children[0])
                    and not set(os.listdir(proxy_mod._ZMQ_TMPDIR)) - before
                ):
                    break
                gevent.sleep(0.05)
            assert children and _proc_exited(children[0]), "child left running"
            assert not set(os.listdir(proxy_mod._ZMQ_TMPDIR)) - before


class TestFinalizer:
    def test_it_removes_the_socket_file_too(self):
        """__del__ closed the transport and terminated the child but released no
        lease, so the paths that reach it — a start that failed after
        publishing, a proxy outliving its dead reader — left the socket file in
        the ipc directory. Called directly, because a proxy whose reader is
        still running is reachable from the hub and never collected at all (see
        README, Known limits)."""
        from gisolate import proxy as proxy_mod

        before = set(os.listdir(proxy_mod._ZMQ_TMPDIR))
        proxy = ProcessProxy.create(adder_factory, timeout=10)
        assert proxy.add(1, 1) == 2
        assert set(os.listdir(proxy_mod._ZMQ_TMPDIR)) - before, "child never bound"

        proxy.__del__()
        assert not set(os.listdir(proxy_mod._ZMQ_TMPDIR)) - before
        proxy._process.join(timeout=5)

    def test_a_teardown_step_that_raises_still_releases_everything(self):
        """The detached teardown does the work in one straight line: the child,
        then the lease, then the waiters. Anything raising in the middle took
        the rest with it — and it runs on its own greenlet, so join() does not
        even report it. Pending callers then waited out their full timeout for
        a worker that was already gone."""
        from gisolate import proxy as proxy_mod

        before = set(os.listdir(proxy_mod._ZMQ_TMPDIR))
        proxy = ProcessProxy.create(adder_factory, timeout=10)
        assert proxy.add(1, 1) == 2
        proxy._cleanup_process = lambda _process: (_ for _ in ()).throw(
            RuntimeError("the child would not go quietly")
        )
        waiter = hub.AsyncResult()
        proxy._pending[999] = waiter

        proxy._stop(_internal.ProcessError("stopped"))

        assert not set(os.listdir(proxy_mod._ZMQ_TMPDIR)) - before
        with pytest.raises(ProcessError):
            waiter.get(timeout=5)


class TestStaleReader:
    def test_a_reader_outliving_its_stop_leaves_the_next_one_alone(self):
        """_stop's kill is bounded, so a reader unwinding through slow client
        code can outlive it. Reading the proxy's fields afterwards, it drained
        the socket a later _start had published — alongside that generation's
        own reader — and then stopped the child underneath it."""
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.add(1, 1) == 2
            stale = proxy._reader
            stale.kill = lambda *_args, **_kwargs: None  # the kill that misses
            # And wedge it there, so it wakes only once the restart below has
            # published a replacement — the interleaving a bounded kill leaves
            # open, and the only one in which the stale reader does harm.
            assert proxy._transport is not None
            proxy._transport.sock.poll = lambda *_a, **_k: gevent.sleep(2.0)
            gevent.sleep(0.1)  # let it finish the poll it is in and enter that one

            proxy.restart_process()
            new_pid = proxy.pid()

            stale.join(timeout=10)
            assert stale.dead, "the stale reader kept draining the new socket"
            assert proxy.pid() == new_pid
            assert proxy.add(2, 3) == 5


class TestReaderIsolation:
    """The reader greenlet is shared by every pending call, so what happens to
    it when a REPLY cannot be deserialized decides what happens to all of them."""

    def test_a_kill_landing_in_deserialization_is_not_answered(self, monkeypatch):
        """_stop kills the reader and waits on it. Unpickling is a switch point
        whenever it imports a class this process has not seen, so the kill can
        land inside it; answering it as a bad response and carrying on leaves a
        reader that outlives the stop — and drains the socket the next start
        publishes."""
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.add(1, 1) == 2
            reader = proxy._reader

            def killed(_payload):
                raise gevent.GreenletExit("kill landed inside loads")

            monkeypatch.setattr(_internal.SmartPickle, "loads", staticmethod(killed))
            with pytest.raises(ProcessError):
                proxy.add(2, 2)
            reader.join(timeout=5)
            assert reader.dead, "the kill was answered as a bad response instead"

    def test_an_unprintable_deserialization_error_spares_the_reader(
        self, monkeypatch
    ):
        """The failure is raised by client code — a __setstate__, a reduce
        callable — so formatting it can raise too. Interpolating it into the
        error message let that escape the reader and stop a healthy worker."""
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.add(1, 1) == 2
            reader, pid_before = proxy._reader, proxy.pid()

            def unprintable(_payload):
                raise UnprintableError("payload")

            monkeypatch.setattr(
                _internal.SmartPickle, "loads", staticmethod(unprintable)
            )
            with pytest.raises(ProcessError):
                proxy.add(2, 2)
            monkeypatch.undo()

            reader.join(timeout=2)  # it must NOT end
            assert not reader.dead
            assert proxy.add(3, 4) == 7
            assert proxy.pid() == pid_before, "a healthy worker was replaced"

    def test_a_failure_whose_type_name_raises_spares_the_reader(self, monkeypatch):
        """Naming the failure is client code too: a metaclass can make even
        ``type(e).__name__`` a property that raises. wrap_exception has guarded
        that for a while; the reader interpolated it bare, so the guard meant
        to keep one bad reply local was itself the thing that killed the
        reader."""
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.add(1, 1) == 2
            reader, pid_before = proxy._reader, proxy.pid()

            class HostileMeta(type):
                @property
                def __name__(cls):  # noqa: N805  # pyright: ignore[reportIncompatibleVariableOverride]
                    raise RuntimeError("the class refuses to name itself")

            class Unnameable(Exception, metaclass=HostileMeta):
                pass

            def unnameable(_payload):
                raise Unnameable("payload")

            monkeypatch.setattr(
                _internal.SmartPickle, "loads", staticmethod(unnameable)
            )
            with pytest.raises(ProcessError):
                proxy.add(2, 2)
            monkeypatch.undo()

            reader.join(timeout=2)  # it must NOT end
            assert not reader.dead
            assert proxy.add(3, 4) == 7
            assert proxy.pid() == pid_before, "a healthy worker was replaced"

    def test_a_failure_whose_traceback_exits_spares_the_reader(self, monkeypatch):
        """A reply that deserializes cleanly is not the end of the client's
        code: the reader then reads __remote_traceback__ to log it, outside
        every guard that had just made the deserialize safe."""
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            assert proxy.add(1, 1) == 2
            reader = proxy._reader

            def hostile(_payload):
                return HostileTraceback("the failure the caller is owed")

            monkeypatch.setattr(_internal.SmartPickle, "loads", staticmethod(hostile))
            with pytest.raises(Exception):
                proxy.add(1)  # fails in the worker, so the reply is an error
            monkeypatch.undo()

            reader.join(timeout=2)  # it must NOT end
            assert not reader.dead
            assert proxy.add(3, 4) == 7


class TestCreateAcceptsAnyCallable:
    def test_a_partial_factory_works(self):
        """create() is typed for any Callable, and functools.partial — the
        obvious way to bind a factory's arguments — has no __qualname__."""
        import functools

        with ProcessProxy.create(functools.partial(adder_factory), timeout=10) as proxy:
            assert proxy.add(2, 3) == 5


class TestAsyncioWorkerUserCode:
    """The loop thread is shared by every call in flight, and under serve() by
    every attached process."""

    def test_a_blocking_connect_does_not_wedge_the_worker(self):
        """A synchronous connect() run on the loop thread stops the deadline
        that was meant to bound it — and every other client with it."""
        with ProcessProxy.create(blocking_connect_factory, timeout=10) as proxy:
            wait_bound(proxy)
            start = time.monotonic()
            with pytest.raises(TimeoutError):
                proxy.with_timeout(0.5).ping()
            assert time.monotonic() - start < 2.0  # the deadline fired, not connect

    def test_a_sync_method_returning_a_coroutine_is_awaited(self):
        """iscoroutinefunction is false for a sync wrapper around an async def,
        so its coroutine went to the serializer instead of being awaited."""
        with ProcessProxy.create(sync_wrapped_async_factory, timeout=10) as proxy:
            assert proxy.add(2, 3) == 5


class TestReaderSpawnRefused:
    def test_a_start_whose_reader_cannot_spawn_leaves_nothing_behind(
        self, monkeypatch
    ):
        """By the spawn, the child and the transport ARE published — that is
        what the reader reads. A spawn that refuses left __init__ raising with a
        live child, a live transport and a socket file, and no proxy for the
        caller to clean any of it up with. The asyncio starts in this package
        roll that back; the gevent ones did not."""
        from gisolate import proxy as proxy_mod

        before = set(os.listdir(proxy_mod._ZMQ_TMPDIR))
        real_spawn = gevent.spawn

        def refuse(fn, *args, **kwargs):
            if getattr(fn, "__name__", "") == "_read_loop":
                raise RuntimeError("the hub refused a greenlet")
            return real_spawn(fn, *args, **kwargs)

        children = set(multiprocessing.process._children)
        monkeypatch.setattr(gevent, "spawn", refuse)
        with pytest.raises(RuntimeError, match="refused"):
            ProcessProxy.create(adder_factory, timeout=10)
        monkeypatch.undo()

        # Read at once, not after a collection: __del__ terminates the child
        # without joining it, and only if something drops the traceback holding
        # the failed __init__'s frame. The rollback is what makes the child gone
        # by the time the caller sees the exception.
        assert not set(multiprocessing.process._children) - children
        assert not set(os.listdir(proxy_mod._ZMQ_TMPDIR)) - before


class TestTeardownWithoutAGreenlet:
    def test_a_stop_whose_spawn_is_refused_still_releases_everything(
        self, monkeypatch
    ):
        """The teardown runs detached so a caller's enclosing timeout cannot
        interrupt it mid-reap. That greenlet is protection, not a requirement: a
        spawn the hub refuses used to raise with the child, the transport and
        the lease already taken off the proxy — reachable from nothing, and past
        __del__'s help."""
        from gisolate import proxy as proxy_mod

        before = set(os.listdir(proxy_mod._ZMQ_TMPDIR))
        proxy = ProcessProxy.create(adder_factory, timeout=10)
        assert proxy.add(1, 1) == 2
        real_spawn = gevent.spawn

        def refuse(fn, *args, **kwargs):
            if getattr(fn, "__name__", "") == "_teardown":
                raise RuntimeError("the hub refused a greenlet")
            return real_spawn(fn, *args, **kwargs)

        monkeypatch.setattr(gevent, "spawn", refuse)
        proxy.shutdown()
        monkeypatch.undo()

        assert not set(os.listdir(proxy_mod._ZMQ_TMPDIR)) - before


class TestErrorReplyResetsTheStreak:
    def test_a_valid_error_reply_is_transport_evidence(self):
        """Error replies used to leave the failure streak standing: six queue
        failures spread across weeks of honest ValueErrors still crossed the
        threshold, and the restart cost a healthy worker its state. Any reply
        that deserializes is proof the transport works, whatever it carries."""
        proxy = ProcessProxy.create(adder_factory, timeout=10)
        try:
            assert proxy.add(1, 1) == 2
            pid = proxy.execute("pid")
            proxy._error_count = proxy.auto_restart_threshold - 1
            with pytest.raises(ValueError, match="intentional"):
                proxy.fail()
            assert proxy._error_count == 0
            assert proxy.execute("pid") == pid, "a healthy worker was restarted"
        finally:
            proxy.shutdown()


class TestCleanupProcessTotality:
    def test_a_raising_terminate_still_escalates_and_forgets(self, monkeypatch):
        """terminate() can raise — racing the child's own exit, or through a
        custom mp_context — and everything after it was skipped: the SIGKILL
        escalation, the reap, and the bookkeeping that keeps each restart from
        leaking a Process and its descriptors."""
        import multiprocessing.process as mp_process

        proxy = ProcessProxy.create(adder_factory, timeout=10)
        try:
            assert proxy.add(1, 1) == 2
            process = proxy._process

            def raising_terminate(_self):
                raise OSError("terminate refused")

            monkeypatch.setattr(type(process), "terminate", raising_terminate)
            proxy._cleanup_process(process)  # must not raise
            monkeypatch.undo()

            assert _proc_exited(process), "the child was never ended"
            assert process not in mp_process._children, (
                "the reaped child stayed in multiprocessing's bookkeeping"
            )
        finally:
            proxy.shutdown()

    def test_a_custom_context_raising_runtime_error_still_escalates(
        self, monkeypatch
    ):
        """OSError is what terminate() raises racing the child's own exit —
        but a custom mp_context, which is supported, can raise anything, and
        the guards only caught OSError: the escalation was skipped and
        shutdown returned with the child still running."""
        import multiprocessing.process as mp_process

        proxy = ProcessProxy.create(adder_factory, timeout=10)
        try:
            assert proxy.add(1, 1) == 2
            process = proxy._process

            def raising_terminate(_self):
                raise RuntimeError("terminate refused by a custom context")

            monkeypatch.setattr(type(process), "terminate", raising_terminate)
            proxy._cleanup_process(process)  # must not raise
            monkeypatch.undo()

            assert _proc_exited(process), "the child was never ended"
            assert process not in mp_process._children
        finally:
            proxy.shutdown()


class TestDelReleasesThroughARaisingClose:
    def test_a_raising_transport_close_still_releases_the_lease(
        self, monkeypatch
    ):
        """__del__ let a BaseException out of transport.close() — term() is a
        switch point — and the interpreter swallowing it is what makes the
        skip silent: the child's socket file stayed behind with the one
        cleanup a dropped proxy ever gets already spent."""
        from gisolate import proxy as proxy_mod

        before = set(os.listdir(proxy_mod._ZMQ_TMPDIR))
        proxy = ProcessProxy.create(adder_factory, timeout=10)
        assert proxy.add(1, 1) == 2
        assert set(os.listdir(proxy_mod._ZMQ_TMPDIR)) - before, "child never bound"
        process = proxy._process

        real_close = _internal.ZmqTransport.close
        armed = [True]

        def raising_close(transport_self):
            if armed[0]:
                armed[0] = False
                raise gevent.Timeout(None)
            return real_close(transport_self)

        monkeypatch.setattr(_internal.ZmqTransport, "close", raising_close)
        with contextlib.suppress(BaseException):
            proxy.__del__()
        monkeypatch.undo()

        assert not set(os.listdir(proxy_mod._ZMQ_TMPDIR)) - before, (
            "the raising close skipped the lease release below it"
        )
        process.join(timeout=5)


class TestTeardownOrdering:
    def test_a_raising_transport_close_still_reaps_the_child(self, monkeypatch):
        """transport.close() can raise out of term()'s switch point, and the
        child cleanup came sequentially after it: the finally released the
        address while the child stayed detached, tracked but never reaped."""
        import multiprocessing.process as mp_process

        proxy = ProcessProxy.create(adder_factory, timeout=10)
        process = None
        try:
            assert proxy.add(1, 1) == 2
            process = proxy._process

            real_close = _internal.ZmqTransport.close
            armed = [True]

            def raising_close(transport_self):
                if armed[0]:
                    armed[0] = False
                    raise gevent.Timeout(None)
                return real_close(transport_self)

            monkeypatch.setattr(_internal.ZmqTransport, "close", raising_close)
            proxy.shutdown()
            monkeypatch.undo()

            assert process not in mp_process._children, (
                "the child was never reaped past the raising transport close"
            )
        finally:
            if process is not None and not _proc_exited(process):
                process.terminate()


class TestFailedLaunchReap:
    def test_a_failed_kill_skips_the_blocking_reap(self, monkeypatch):
        """os.kill can refuse — and waitpid(pid, 0) after a kill that never
        reached the child blocks forever on a bootstrap that will never
        finish. The reap is gated on the kill having taken."""
        import types

        from gisolate import proxy as proxy_mod

        reaps = []

        def refusing_kill(_pid, _sig):
            raise PermissionError("kill refused")

        monkeypatch.setattr(proxy_mod.os, "kill", refusing_kill)
        monkeypatch.setattr(proxy_mod.os, "waitpid", lambda *a: reaps.append(a))
        proxy_mod._publish_failed_launch(
            types.SimpleNamespace(pid=999_999_999), object()
        )
        monkeypatch.undo()
        assert not reaps, "the reap would have blocked on a child the kill never reached"

    def test_an_interrupted_reap_collects_a_lagging_exit_and_reraises(
        self, monkeypatch
    ):
        """Ctrl-C landing in the blocked waitpid is the operator's and must
        propagate — but the kill already took, so skipping the reap outright
        strands a zombie nothing else can collect: _popen was never published,
        so no cleanup path and no exit handler knows the pid. And WNOHANG
        legitimately answers (0, 0) while the killed child is still
        descheduled, so a single probe is a coin toss — the bounded spin
        keeps probing until the exit lands, then the interrupt goes through."""
        import types

        from gisolate import proxy as proxy_mod

        calls = []

        def interrupted_waitpid(pid, flags):
            if flags == 0:
                raise KeyboardInterrupt
            calls.append((pid, flags))
            if len(calls) < 3:
                return (0, 0)  # killed, not yet exited
            return (pid, 0)

        monkeypatch.setattr(proxy_mod.os, "kill", lambda _pid, _sig: None)
        monkeypatch.setattr(proxy_mod.os, "waitpid", interrupted_waitpid)
        with pytest.raises(KeyboardInterrupt):
            proxy_mod._publish_failed_launch(
                types.SimpleNamespace(pid=999_999_999), object()
            )
        monkeypatch.undo()
        assert calls and calls[-1] == (999_999_999, proxy_mod.os.WNOHANG)
        assert len(calls) == 3, "the reap gave up while the exit was still landing"
        assert 999_999_999 not in proxy_mod._orphans, (
            "a collected exit must not stay registered"
        )

    def test_a_reap_that_never_lands_still_honors_the_interrupt(self, monkeypatch):
        """A child whose exit does not land inside the bounded spin must not
        hold the operator's interrupt hostage — and must not be forgotten
        either: it stays registered for the next launch's sweep, because
        (0, 0) at the deadline means "not waitable NOW", not "never"."""
        import types

        from gisolate import proxy as proxy_mod

        def interrupted_waitpid(pid, flags):
            if flags == 0:
                raise KeyboardInterrupt
            return (0, 0)  # not exited yet

        monkeypatch.setattr(proxy_mod.os, "kill", lambda _pid, _sig: None)
        monkeypatch.setattr(proxy_mod.os, "waitpid", interrupted_waitpid)
        started = time.monotonic()
        try:
            with pytest.raises(KeyboardInterrupt):
                proxy_mod._publish_failed_launch(
                    types.SimpleNamespace(pid=999_999_999), object()
                )
            monkeypatch.undo()
            assert time.monotonic() - started < 1.0, "the retry must be bounded"
            assert 999_999_999 in proxy_mod._orphans, (
                "the uncollected exit lost its only reaper"
            )
        finally:
            proxy_mod._orphans.pop(999_999_999, None)

    def test_a_second_interrupt_mid_spin_does_not_lose_the_reap(self, monkeypatch):
        """Signals land between bytecodes: a second Ctrl-C during the spin
        escapes the except Exception around it. The pid is registered BEFORE
        the spin, so the obligation survives the escape."""
        import types

        from gisolate import proxy as proxy_mod

        def interrupted_waitpid(pid, flags):
            raise KeyboardInterrupt  # the first in the block, the second mid-spin

        monkeypatch.setattr(proxy_mod.os, "kill", lambda _pid, _sig: None)
        monkeypatch.setattr(proxy_mod.os, "waitpid", interrupted_waitpid)
        try:
            with pytest.raises(KeyboardInterrupt):
                proxy_mod._publish_failed_launch(
                    types.SimpleNamespace(pid=999_999_999), object()
                )
            monkeypatch.undo()
            assert 999_999_999 in proxy_mod._orphans
        finally:
            proxy_mod._orphans.pop(999_999_999, None)

    def test_the_next_launch_collects_a_registered_orphan(self, monkeypatch):
        """The registered pid's exit is collected by the sweep every launch
        runs, and a pid someone else already reaped is dropped rather than
        retried for ever."""
        import types

        from gisolate import proxy as proxy_mod

        reaped = []
        monkeypatch.setattr(
            proxy_mod.os, "waitpid", lambda pid, flags: (reaped.append(pid), (pid, 0))[1]
        )
        proxy_mod._orphans[999_999_999] = time.monotonic() + 60.0
        try:
            proxy_mod._launch_recoverably(types.SimpleNamespace())
            monkeypatch.undo()
            assert reaped == [999_999_999]
            assert 999_999_999 not in proxy_mod._orphans
        finally:
            proxy_mod._orphans.pop(999_999_999, None)

    def test_someone_elses_reap_releases_the_obligation(self, monkeypatch):
        """ECHILD is the one proof the obligation is gone — libev's
        default-loop SIGCHLD handler reaps children too — and it must drop
        the registration on the spot, not leave a stale integer for the pid
        space to recycle into somebody else's child."""
        import types

        from gisolate import proxy as proxy_mod

        def raising_waitpid(pid, flags):
            raise ChildProcessError

        monkeypatch.setattr(proxy_mod.os, "kill", lambda _pid, _sig: None)
        monkeypatch.setattr(proxy_mod.os, "waitpid", raising_waitpid)
        try:
            proxy_mod._publish_failed_launch(
                types.SimpleNamespace(pid=999_999_999), object()
            )
            monkeypatch.undo()
            assert 999_999_999 not in proxy_mod._orphans
        finally:
            proxy_mod._orphans.pop(999_999_999, None)

    def test_an_unexplained_wait_failure_keeps_the_obligation(self, monkeypatch):
        """Anything short of ECHILD proves nothing about the child: the
        registration stays for the sweep instead of being discarded on the
        first excuse."""
        import types

        from gisolate import proxy as proxy_mod

        def raising_waitpid(pid, flags):
            raise OSError("transient")

        monkeypatch.setattr(proxy_mod.os, "kill", lambda _pid, _sig: None)
        monkeypatch.setattr(proxy_mod.os, "waitpid", raising_waitpid)
        try:
            proxy_mod._publish_failed_launch(
                types.SimpleNamespace(pid=999_999_999), object()
            )
            monkeypatch.undo()
            assert 999_999_999 in proxy_mod._orphans
        finally:
            proxy_mod._orphans.pop(999_999_999, None)

    def test_the_sweep_gives_a_stale_number_back(self, monkeypatch):
        """A pid past its deadline is dropped WITHOUT being probed: the
        number can be RECYCLED once somebody else collected the zombie, and
        even one more waitpid on it is exactly the alias the deadline exists
        to prevent — it would consume an unrelated child's exit. Before the
        deadline the obligation is kept."""
        import types

        from gisolate import proxy as proxy_mod

        probed = []
        monkeypatch.setattr(
            proxy_mod.os,
            "waitpid",
            lambda pid, _flags: (probed.append(pid), (0, 0))[1],
        )
        proxy_mod._orphans[999_999_998] = time.monotonic() + 60.0
        proxy_mod._orphans[999_999_999] = time.monotonic() - 1.0
        try:
            proxy_mod._launch_recoverably(types.SimpleNamespace())
            monkeypatch.undo()
            assert 999_999_998 in proxy_mod._orphans, "dropped before its deadline"
            assert 999_999_999 not in proxy_mod._orphans, "kept past its deadline"
            assert probed == [999_999_998], (
                "an expired pid must be given back unprobed — the probe IS the alias"
            )
        finally:
            proxy_mod._orphans.pop(999_999_998, None)
            proxy_mod._orphans.pop(999_999_999, None)
