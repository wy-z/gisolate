# pyright: reportAttributeAccessIssue=false, reportGeneralTypeIssues=false
"""Tests for gisolate.proxy module (ProcessProxy)."""

import multiprocessing
import os
from unittest.mock import MagicMock

import gevent
import pytest

from gisolate import hub
from gisolate._internal import ProcessError
from gisolate.proxy import ProcessProxy, get_default_mp_context, set_default_mp_context

from .helpers import adder_factory, slow_connect_factory, tracker_factory


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

    def test_private_attr_raises(self):
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            with pytest.raises(AttributeError):
                proxy._secret

    def test_cancelled_connect_does_not_poison_client(self):
        """A per-call deadline cancelling the client's async connect() must
        not cache the half-connected client — the next call rebuilds it."""
        with ProcessProxy.create(slow_connect_factory, timeout=10) as proxy:
            with pytest.raises(TimeoutError):
                proxy.with_timeout(0.3).is_ready()
            assert proxy.is_ready() is True

    def test_cancelled_connect_closes_orphan(self):
        """The half-connected client abandoned by a deadline-cancelled
        connect() must be close()d in the child, not leaked."""
        with ProcessProxy.create(slow_connect_factory, timeout=10) as proxy:
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
            assert not proc.is_alive()
        finally:
            if proc.is_alive():
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
            assert not proc.is_alive()
        finally:
            if proc.is_alive():
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
