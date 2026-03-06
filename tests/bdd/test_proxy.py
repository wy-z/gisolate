"""BDD steps for process_proxy.feature."""

import contextlib
import os

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from gisolate.proxy import ProcessProxy

from ..helpers import adder_factory


@pytest.fixture
def proxy_ctx():
    ctx = {"proxy": None, "result": None, "error": None, "pid1": None, "pid2": None}
    yield ctx
    with contextlib.suppress(Exception):
        if ctx["proxy"] is not None:
            ctx["proxy"].shutdown()


@scenario("process_proxy.feature", "Execute a method on a remote object")
def test_execute_method():
    pass


@scenario("process_proxy.feature", "Remote exceptions propagate to the caller")
def test_remote_exception():
    pass


@scenario("process_proxy.feature", "Child process runs in a separate PID")
def test_separate_pid():
    pass


@scenario("process_proxy.feature", "Proxy restart gives a new child process")
def test_restart():
    pass


@scenario("process_proxy.feature", "Shutdown prevents further calls")
def test_shutdown():
    pass


@given("a ProcessProxy wrapping an Adder")
def create_proxy(proxy_ctx):
    proxy_ctx["proxy"] = ProcessProxy.create(adder_factory, timeout=10)


@when(parsers.parse("I call add with {a:d} and {b:d}"))
def call_add(proxy_ctx, a, b):
    proxy_ctx["result"] = proxy_ctx["proxy"].add(a, b)


@when("I call a method that raises ValueError")
def call_failing_method(proxy_ctx):
    try:
        proxy_ctx["proxy"].fail()
    except ValueError as e:
        proxy_ctx["error"] = e


@when("I request the child PID")
def request_pid(proxy_ctx):
    proxy_ctx["pid1"] = proxy_ctx["proxy"].pid()


@when("I restart the proxy")
def restart_proxy(proxy_ctx):
    proxy_ctx["proxy"].restart_process()


@when("I request the child PID again")
def request_pid_again(proxy_ctx):
    proxy_ctx["pid2"] = proxy_ctx["proxy"].pid()


@when("I shutdown the proxy")
def shutdown_proxy(proxy_ctx):
    proxy_ctx["proxy"].shutdown()


@then(parsers.parse("the result should be {expected:d}"))
def check_result(proxy_ctx, expected):
    assert proxy_ctx["result"] == expected


@then(parsers.parse('a ValueError should be raised with message "{msg}"'))
def check_value_error(proxy_ctx, msg):
    assert proxy_ctx["error"] is not None
    assert msg in str(proxy_ctx["error"])


@then("it should differ from the current PID")
def check_different_pid(proxy_ctx):
    assert proxy_ctx["pid1"] != os.getpid()


@then("the two PIDs should differ")
def check_pids_differ(proxy_ctx):
    assert proxy_ctx["pid1"] != proxy_ctx["pid2"]


@then("calling a method should raise RuntimeError")
def check_shutdown_raises(proxy_ctx):
    with pytest.raises(RuntimeError):
        proxy_ctx["proxy"].add(1, 2)
