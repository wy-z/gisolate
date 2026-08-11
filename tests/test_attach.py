# pyright: reportAttributeAccessIssue=false
"""Tests for the shared-worker mode: one ``serve()`` host, many attached proxies.

The point of the mode is that N client PROCESSES share ONE worker instead of each
spawning its own: whose process ran the call, and what one client's lifecycle does
to another's.
"""

import os
import uuid

import gevent
import pytest
import zmq

import gisolate
from gisolate.proxy import ProcessProxy, _ZMQ_TMPDIR

from .helpers import adder_factory, host_process, serve_adder, serve_marker


@pytest.fixture
def host(spawn_ctx):
    """A worker process serving Adder at a fixed address. Yields (address, pid)."""
    # gisolate's own ipc dir: a pytest tmp_path can exceed the ~104-char limit
    # the OS puts on unix-socket paths.
    address = f"ipc://{_ZMQ_TMPDIR}/host-{uuid.uuid4().hex[:12]}.sock"
    with host_process(spawn_ctx, serve_adder, address) as proc:
        yield address, proc.pid


class TestAttach:
    def test_call_runs_in_the_host_process(self, host):
        address, host_pid = host
        with ProcessProxy.attach(address, timeout=15) as proxy:
            assert proxy.add(2, 3) == 5
            # The host IS the worker: no child was spawned for us, and the call
            # did not run here either.
            assert proxy.pid() == host_pid
            assert proxy.pid() != os.getpid()

    def test_clients_with_colliding_ids_do_not_read_each_others_replies(self, host):
        address, host_pid = host
        with ProcessProxy.attach(address, timeout=15) as first:
            with ProcessProxy.attach(address, timeout=15) as second:
                assert first.pid() == second.pid() == host_pid
                # Request ids are a per-proxy counter, so two clients hand out
                # the same ids independently — and here both are outstanding at
                # once. Only the ROUTER's peer identity keeps the answers apart.
                slow = gevent.spawn(first.slow, 0.5)
                quick = gevent.spawn(second.echo, "second")
                assert (slow.get(timeout=20), quick.get(timeout=20)) == (
                    "done",
                    "second",
                )

    def test_one_client_leaving_does_not_stop_the_host(self, host):
        address, host_pid = host
        first = ProcessProxy.attach(address, timeout=15)
        second = ProcessProxy.attach(address, timeout=15)
        assert first.add(1, 1) == 2
        first.shutdown()
        # An owned child exits on the SHUTDOWN frame; a shared one must not —
        # nor may the departing client unlink the host's socket file.
        assert os.path.exists(address.removeprefix("ipc://"))
        assert second.pid() == host_pid
        # ...and the departing client really did let go, rather than skipping
        # teardown because it had no process to stop.
        assert first._sock is None and first._reader is None
        second.shutdown()

    def test_a_lost_local_transport_is_rebuilt_on_the_next_call(self, host):
        address, host_pid = host
        with ProcessProxy.attach(address, timeout=15) as proxy:
            assert proxy.add(1, 1) == 2
            # What a local ZMQ failure leaves behind: the reader's `finally`
            # tears the transport down, and nothing owns a process to restart.
            proxy._stop()
            assert proxy._sock is None
            assert proxy.pid() == host_pid
            assert proxy._sock is not None

    def test_a_call_that_expired_before_the_host_existed_never_runs(
        self, spawn_ctx, tmp_path
    ):
        # Attaching to an address nobody has bound is legal, so the request sits
        # in the DEALER's queue until a host appears — possibly long after the
        # caller gave up. The deadline travels with the request so that host
        # rejects it instead of running it.
        address = f"ipc://{_ZMQ_TMPDIR}/late-{uuid.uuid4().hex[:12]}.sock"
        marker = tmp_path / "ran"
        with ProcessProxy.attach(address, timeout=1) as proxy:
            with pytest.raises(TimeoutError):
                proxy.mark()
            with host_process(spawn_ctx, serve_marker, address, str(marker)):
                # A reply proves the host came up and this proxy reaches it, so
                # the marker's absence means "rejected", not "never delivered".
                # The host has one slot, so a wrongly admitted mark() would have
                # had to finish before this ping could be answered.
                assert proxy.with_timeout(20).ping() == "pong"
                assert not marker.exists()

    @pytest.mark.parametrize(
        "address",
        [
            "tcp://127.0.0.1:5999",  # another machine's clock, and an open port
            "ipc://@abstract",  # Linux abstract namespace: no file to protect
            "ipc://relative.sock",  # a different socket per working directory
        ],
    )
    def test_only_an_absolute_ipc_address_is_accepted_by_either_end(self, address):
        for call in (
            lambda: gisolate.serve(adder_factory, address),
            lambda: ProcessProxy.attach(address),
        ):
            with pytest.raises(ValueError, match="ipc:///"):
                call()

    def test_a_host_that_cannot_bind_says_so(self):
        # Not silence: serve() returning looks exactly like a clean shutdown,
        # and the host process would exit 0 having served nothing. The missing
        # directory is unique per run — if it ever existed, serve() would bind
        # and block forever, hanging the suite rather than failing it.
        missing = f"ipc://{_ZMQ_TMPDIR}/gone-{uuid.uuid4().hex[:12]}/x.sock"
        with pytest.raises(zmq.ZMQError):
            gisolate.serve(adder_factory, missing)

    def test_restarting_a_client_leaves_the_host_alone(self, host):
        address, host_pid = host
        with ProcessProxy.attach(address, timeout=15) as proxy:
            assert proxy.add(1, 1) == 2
            before = proxy._sock
            proxy.restart_process()  # for an attached proxy: reconnect, no spawn
            assert proxy._sock is not before
            assert os.path.exists(address.removeprefix("ipc://"))
            assert proxy.pid() == host_pid
