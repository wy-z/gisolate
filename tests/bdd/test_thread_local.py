"""BDD steps for thread_local.feature."""

import gevent
import pytest
from pytest_bdd import given, scenario, then, when

from gisolate.local import ThreadLocalProxy


class _Counter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value


@pytest.fixture
def tl_ctx():
    return {"proxy": None, "factory_calls": 0, "main_value": None, "other_value": None}


@scenario("thread_local.feature", "Each thread gets its own instance")
def test_thread_isolation():
    pass


@scenario("thread_local.feature", "Lazy instance creation")
def test_lazy_creation():
    pass


@given("a ThreadLocalProxy wrapping a Counter")
def create_proxy(tl_ctx):
    tl_ctx["factory_calls"] = 0

    def factory():
        tl_ctx["factory_calls"] += 1
        return _Counter()

    tl_ctx["proxy"] = ThreadLocalProxy(factory)


@when("I increment from the main thread")
def increment_main(tl_ctx):
    tl_ctx["proxy"].increment()
    tl_ctx["main_value"] = tl_ctx["proxy"].value


@when("I read the value from another thread")
def read_other_thread(tl_ctx):
    tl_ctx["other_value"] = gevent.get_hub().threadpool.apply(
        lambda: tl_ctx["proxy"].value
    )


@when("I access an attribute on the proxy")
def access_attr(tl_ctx):
    _ = tl_ctx["proxy"].value


@then("the main thread value should be 1")
def check_main_value(tl_ctx):
    assert tl_ctx["main_value"] == 1


@then("the other thread value should be 0")
def check_other_value(tl_ctx):
    assert tl_ctx["other_value"] == 0


@then("the factory should not have been called yet")
def check_not_called(tl_ctx):
    assert tl_ctx["factory_calls"] == 0


@then("the factory should have been called once")
def check_called_once(tl_ctx):
    assert tl_ctx["factory_calls"] == 1
