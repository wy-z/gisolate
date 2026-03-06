"""BDD steps for subprocess_run.feature."""

import os

import pytest
from pytest_bdd import scenario, then, when

from gisolate.subprocess import run_in_subprocess

from ..helpers import add, get_pid, raise_value_error, slow_func


@pytest.fixture
def sub_ctx():
    return {"result": None, "error": None}


@scenario("subprocess_run.feature", "Run a simple function")
def test_simple():
    pass


@scenario("subprocess_run.feature", "Subprocess propagates exceptions")
def test_exception():
    pass


@scenario("subprocess_run.feature", "Subprocess times out")
def test_timeout():
    pass


@scenario("subprocess_run.feature", "Subprocess runs in a different process")
def test_different_pid():
    pass


@when("I run a function that adds 5 and 8 in a subprocess")
def run_add(sub_ctx):
    sub_ctx["result"] = run_in_subprocess(add, args=(5, 8))


@when("I run a function that raises ValueError in a subprocess")
def run_raise(sub_ctx):
    try:
        run_in_subprocess(raise_value_error)
    except ValueError as e:
        sub_ctx["error"] = e


@when("I run a slow function with a short timeout")
def run_slow(sub_ctx):
    try:
        run_in_subprocess(slow_func, timeout=0.5)
    except TimeoutError as e:
        sub_ctx["error"] = e


@when("I run a function that returns its PID in a subprocess")
def run_pid(sub_ctx):
    sub_ctx["result"] = run_in_subprocess(get_pid)


@then("the subprocess result should be 13")
def check_result_13(sub_ctx):
    assert sub_ctx["result"] == 13


@then("a ValueError should be raised from the subprocess")
def check_value_error(sub_ctx):
    assert isinstance(sub_ctx["error"], ValueError)


@then("a TimeoutError should be raised from the subprocess")
def check_timeout(sub_ctx):
    assert isinstance(sub_ctx["error"], TimeoutError)


@then("the returned PID should differ from the current PID")
def check_pid(sub_ctx):
    assert sub_ctx["result"] != os.getpid()
