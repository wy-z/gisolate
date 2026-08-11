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

from gisolate import hub
from gisolate._internal import ProcessError
from gisolate.proxy import (
    ProcessProxy,
    _proc_exited,
    get_default_mp_context,
    set_default_mp_context,
)

from .helpers import (
    adder_factory,
    cancel_leaker_factory,
    slow_connect_factory,
    swallower_factory,
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

    def test_private_attr_raises(self):
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            with pytest.raises(AttributeError):
                proxy._secret

    def test_cancelled_connect_does_not_poison_client(self):
        """A per-call deadline cancelling the client's async connect() must
        not cache the half-connected client — the next call rebuilds it."""
        with ProcessProxy.create(slow_connect_factory, timeout=10) as proxy:
            wait_bound(proxy)  # else the 0.3s is spent on child startup
            with pytest.raises(TimeoutError):
                proxy.with_timeout(0.3).is_ready()
            assert proxy.is_ready() is True

    def test_cancelled_connect_closes_orphan(self):
        """The half-connected client abandoned by a deadline-cancelled
        connect() must be close()d in the child, not leaked."""
        with ProcessProxy.create(slow_connect_factory, timeout=10) as proxy:
            wait_bound(proxy)  # else the 0.3s is spent on child startup
            with pytest.raises(TimeoutError):
                proxy.with_timeout(0.3).close_count()
            assert proxy.close_count() == 1


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
            proxy._reader.kill = lambda *a, **k: gevent.sleep(5)
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
        proxy.shutdown()  # tears down the live process: _sock=None, _pending cleared
        stub = MagicMock()
        proxy._sock = stub

        # Stale: req_id not pending -> dropped.
        proxy._raw_send(123, [b"a", b"b"])
        stub.send_multipart.assert_not_called()

        # Live: req_id pending -> forwarded.
        proxy._pending[7] = hub.AsyncResult()
        proxy._raw_send(7, [b"c", b"d"])
        stub.send_multipart.assert_called_once()
