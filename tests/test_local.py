# pyright: reportAttributeAccessIssue=false
"""Tests for gisolate.local module."""

import gevent

from gisolate.local import ThreadLocalProxy


class _Counter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value


class TestThreadLocalProxy:
    def test_attribute_access(self):
        proxy = ThreadLocalProxy(_Counter)
        assert proxy.value == 0

    def test_method_call(self):
        proxy = ThreadLocalProxy(_Counter)
        assert proxy.increment() == 1
        assert proxy.increment() == 2

    def test_setattr(self):
        proxy = ThreadLocalProxy(_Counter)
        proxy.value = 100
        assert proxy.value == 100

    def test_delattr(self):
        proxy = ThreadLocalProxy(_Counter)
        proxy.extra = "test"
        assert proxy.extra == "test"
        del proxy.extra
        assert not hasattr(proxy._get_instance(), "extra")

    def test_thread_isolation(self):
        proxy = ThreadLocalProxy(_Counter)
        proxy.increment()  # main thread: value=1

        # Use gevent threadpool to run in a real OS thread
        other_value = gevent.get_hub().threadpool.apply(lambda: proxy.value)

        assert proxy.value == 1
        assert other_value == 0  # fresh instance in real thread

    def test_lazy_creation(self):
        call_count = 0

        def factory():
            nonlocal call_count
            call_count += 1
            return _Counter()

        proxy = ThreadLocalProxy(factory)
        assert call_count == 0
        proxy.value  # triggers creation
        assert call_count == 1
        proxy.value  # reuses existing
        assert call_count == 1


class TestConcurrentFirstAccess:
    def test_two_greenlets_on_one_thread_see_one_instance(self):
        """The factory opens things, which under gevent is a switch point: both
        greenlets passed the hasattr check, both built, and the second
        overwrote the first — so calls on one thread ran against two different
        clients."""
        import gevent

        built = []

        def factory():
            gevent.sleep(0.05)  # a connection being opened
            obj = object()
            built.append(obj)
            return obj

        proxy = ThreadLocalProxy(factory)
        seen = gevent.joinall(
            [gevent.spawn(proxy._get_instance) for _ in range(2)], timeout=5
        )
        results = [g.value for g in seen]
        assert results[0] is results[1], "one thread, two instances"
        # And only one was ever built: publishing one of two is not enough,
        # since nothing here closes the loser.
        assert len(built) == 1, f"the factory ran {len(built)} times on one thread"
