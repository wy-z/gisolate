# pyright: reportAttributeAccessIssue=false, reportGeneralTypeIssues=false
"""Tests for gisolate.proxy module (ProcessProxy)."""

import multiprocessing
import os

import gevent
import pytest

from gisolate.proxy import ProcessProxy, get_default_mp_context, set_default_mp_context

from .helpers import adder_factory, tracker_factory


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

    def test_private_attr_raises(self):
        with ProcessProxy.create(adder_factory, timeout=10) as proxy:
            with pytest.raises(AttributeError):
                proxy._secret


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
