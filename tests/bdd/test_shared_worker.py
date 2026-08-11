# pyright: reportAttributeAccessIssue=false
"""BDD steps for shared_worker.feature."""

import contextlib
import os
import uuid

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from gisolate.proxy import _ZMQ_TMPDIR, ProcessProxy

from ..helpers import host_process, serve_adder, serve_marker


@pytest.fixture
def shared_ctx(spawn_ctx, tmp_path):
    with contextlib.ExitStack() as stack:
        ctx = {
            "spawn": spawn_ctx,
            "stack": stack,
            # gisolate's own ipc dir: a pytest tmp_path can exceed the ~104-char
            # limit the OS puts on unix-socket paths.
            "address": f"ipc://{_ZMQ_TMPDIR}/bdd-{uuid.uuid4().hex[:12]}.sock",
            "marker": tmp_path / "ran",
            "hosts": [],
            "proxies": [],
            "result": None,
        }
        yield ctx
        for proxy in ctx["proxies"]:
            with contextlib.suppress(Exception):
                proxy.shutdown()


def _start_host(ctx, target, *args):
    proc = ctx["stack"].enter_context(
        host_process(ctx["spawn"], target, ctx["address"], *args)
    )
    ctx["hosts"].append(proc)
    return proc


@scenario("shared_worker.feature", "An attached proxy runs its calls in the host process")
def test_calls_run_in_the_host():
    pass


@scenario("shared_worker.feature", "One client leaving keeps the host serving the others")
def test_one_client_leaving():
    pass


@scenario("shared_worker.feature", "A call that expired before the host existed never runs")
def test_expired_call_never_runs():
    pass


@given("a host process serving an Adder")
def start_host(shared_ctx):
    _start_host(shared_ctx, serve_adder)


@given("an address no host has bound yet")
def no_host(shared_ctx):
    assert not shared_ctx["hosts"]


@given("a proxy attached to the host")
@given("a second proxy attached to the host")
def attach_proxy(shared_ctx):
    shared_ctx["proxies"].append(ProcessProxy.attach(shared_ctx["address"], timeout=15))


@given("a proxy attached to that address")
def attach_to_unbound(shared_ctx):
    shared_ctx["proxies"].append(ProcessProxy.attach(shared_ctx["address"], timeout=1))


@when(parsers.parse("I call add with {a:d} and {b:d}"))
def call_add(shared_ctx, a, b):
    shared_ctx["result"] = shared_ctx["proxies"][0].add(a, b)


@when("I shutdown the first proxy")
def shutdown_first(shared_ctx):
    shared_ctx["proxies"][0].shutdown()


@when("the call times out and a host starts afterwards")
def timeout_then_host(shared_ctx):
    with pytest.raises(TimeoutError):
        shared_ctx["proxies"][0].mark()
    _start_host(shared_ctx, serve_marker, str(shared_ctx["marker"]))


@then(parsers.parse("the result should be {expected:d}"))
def check_result(shared_ctx, expected):
    assert shared_ctx["result"] == expected


@then("the call should have run in the host process")
def check_ran_in_host(shared_ctx):
    assert shared_ctx["proxies"][0].pid() == shared_ctx["hosts"][0].pid


@then("the second proxy should still reach the host")
def check_second_still_works(shared_ctx):
    assert shared_ctx["proxies"][1].pid() == shared_ctx["hosts"][0].pid


@then("the host socket should still exist")
def check_socket_kept(shared_ctx):
    assert os.path.exists(shared_ctx["address"].removeprefix("ipc://"))


@then("the call should never have run in the host")
def check_never_ran(shared_ctx):
    # A reply proves the host is up and reachable, so the marker's absence
    # means the queued call was rejected, not that it never arrived. The host
    # has one slot, so a wrongly admitted mark() would have had to finish first.
    assert shared_ctx["proxies"][0].with_timeout(20).ping() == "pong"
    assert not shared_ctx["marker"].exists()
