"""Tests for gisolate.proxy module (ProcessProxy)."""

import multiprocessing
import os

import gevent
import pytest

from gisolate.proxy import ProcessProxy, get_default_mp_context, set_default_mp_context

from .helpers import adder_factory


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
