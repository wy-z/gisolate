"""Tests for gisolate.bridge module."""

import asyncio
import contextlib
import os
import tempfile
import uuid

import gevent
import pytest
import zmq
import zmq.green

from gisolate._internal import IpcLease, ProcessError, ZmqTransport
from gisolate.bridge import ProcessBridge

from .helpers import (
    raises_a_hostile_traceback,
    returns_a_value_that_exits_on_arrival,
)


def _fake_transport(sock):
    """A transport around a stand-in socket, for the paths a real ZMQ socket
    cannot be driven into."""
    return ZmqTransport(sock, None, _make_addr(), IpcLease.none())


def _make_addr():
    return f"ipc://{tempfile.gettempdir()}/gisolate-test-{uuid.uuid4().hex}.sock"


_ticks: list[int] = []


def _tick_forever():
    import gevent

    while True:
        _ticks.append(1)
        gevent.sleep(0.05)


def _raise_greenlet_exit():
    import gevent

    raise gevent.GreenletExit("server function killed")


class TestProcessBridgeServer:
    def test_start_server(self):
        addr = _make_addr()
        bridge = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER)
        assert bridge.start() is bridge  # returns self for chaining
        bridge.close()

    def test_address_is_plain_property(self):
        addr = _make_addr()
        bridge = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
        assert bridge.address == addr  # no side effects
        bridge.close()

    def test_close_idempotent_before_start(self):
        addr = _make_addr()
        bridge = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER)
        bridge.close()  # never started, should not raise
        bridge.close()  # double close, should not raise

    def test_start_idempotent(self):
        addr = _make_addr()
        bridge = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER)
        bridge.start()
        bridge.start()  # second start is no-op
        bridge.close()

    def test_server_mode_cannot_call(self):
        addr = _make_addr()
        bridge = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER)

        async def try_call():
            await bridge.call(lambda: 42)

        with pytest.raises(RuntimeError, match="server mode"):
            asyncio.run(try_call())

        bridge.close()


class TestProcessBridgeRPC:
    def test_server_client_roundtrip(self):
        """Server handles a function call from an asyncio client."""
        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER)
        server.start()

        async def client_call():
            client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
            try:
                result = await client.call(lambda x, y: x * y, 6, 7, timeout=5)
                return result
            finally:
                client.close()

        result = asyncio.run(client_call())
        assert result == 42
        server.close()

    def test_server_client_exception(self):
        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER)
        server.start()

        async def client_call():
            client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
            try:

                def fail():
                    raise ValueError("bridge error")

                await client.call(fail, timeout=5)
            finally:
                client.close()

        with pytest.raises(ValueError, match="bridge error"):
            asyncio.run(client_call())

        server.close()

    def test_a_reply_that_exits_while_arriving_stays_a_failed_call(self):
        """Reconstruction runs the sender's code — a __setstate__, a reduce
        callable — and this end had no guard at all: a reply whose unpickling
        raised SystemExit left call() raising it straight past every ordinary
        `except Exception` and could end the client host. ProcessProxy's reader
        already splits this; the bridge did not."""
        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER)
        server.start()

        async def client_call():
            client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
            try:
                await client.call(returns_a_value_that_exits_on_arrival, timeout=5)
            finally:
                client.close()

        with pytest.raises(ProcessError, match="Bad response"):
            asyncio.run(client_call())

        server.close()

    def test_a_failure_whose_traceback_exits_is_still_a_failed_call(self):
        """Unpickling is not the last of the sender's code this end runs: the
        reply is then asked for its __remote_traceback__ so it can be logged,
        and a hostile __getattribute__ made that read the caller's own exit —
        behind the guard that had just made the deserialize safe."""
        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER)
        server.start()

        async def client_call():
            client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
            try:
                await client.call(raises_a_hostile_traceback, timeout=5)
            finally:
                client.close()

        with pytest.raises(Exception) as excinfo:
            asyncio.run(client_call())
        assert not isinstance(excinfo.value, SystemExit)

        server.close()

    def test_a_stale_server_loop_stops_once_a_new_one_is_published(self):
        """Every other loop here is bound to the transport it started on, and
        this one was not: it watched the shared _shutdown flag, which a
        concurrent start() resets. Its close then yields in join(), the restart
        clears the flag, and generation A goes back to serving its own socket
        alongside B."""
        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER).start()
        stale = server._transport
        server._transport = _fake_transport(None)  # what a restart published

        loop = gevent.spawn(server._serve, stale)
        loop.join(timeout=3)
        assert loop.dead, "the stale loop kept serving the old generation"

        server._transport = stale
        server.close()

    def test_multiple_calls(self):
        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER)
        server.start()

        async def client_calls():
            client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
            try:
                results = []
                for i in range(5):
                    r = await client.call(lambda x: x**2, i, timeout=5)
                    results.append(r)
                return results
            finally:
                client.close()

        results = asyncio.run(client_calls())
        assert results == [0, 1, 4, 9, 16]
        server.close()

    def test_multiple_asyncio_run_calls(self):
        """Client survives across separate asyncio.run() calls (reader task revival)."""
        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER)
        server.start()

        client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
        for i in range(3):
            result = asyncio.run(client.call(lambda x: x * 2, i, timeout=5))
            assert result == i * 2
        client.close()
        server.close()


class TestServerAddressOwnership:
    def test_closing_an_old_server_leaves_the_live_one_reachable(self):
        """libzmq unlinks an ipc path before binding it, so a replacement
        server silently takes the address over rather than failing. The
        departing server must not remove that file: the survivor would stay
        reachable to its existing peers and invisible to every new connect."""
        addr = _make_addr()
        path = addr.removeprefix("ipc://")
        old = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER).start()
        new = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER).start()
        try:
            old.close()
            assert os.path.exists(path)
        finally:
            new.close()

    def test_close_after_a_failed_socket_allocation(self, monkeypatch):
        """close() gates teardown on ``_started``. Claiming it before the
        socket existed made close() raise AttributeError over a None socket —
        and clear the flag on its way out, so neither a later close() nor
        __del__ ever reclaimed the live context."""

        def allocation_fails(*_args, **_kwargs):
            raise zmq.ZMQError(zmq.EINVAL)

        monkeypatch.setattr(zmq.green.Context, "socket", allocation_fails)
        bridge = ProcessBridge(_make_addr(), mode=ProcessBridge.Mode.SERVER)
        with pytest.raises(zmq.ZMQError):
            bridge.start()
        assert not bridge._started
        bridge.close()  # must not raise

    def test_a_lone_server_removes_its_own_socket(self):
        """The other half of the rule: when nobody took the address over, the
        file IS ours and must go — a service churning unique addresses would
        otherwise leave one socket inode behind per bridge it ever opened."""
        addr = _make_addr()
        bridge = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER).start()
        assert os.path.exists(addr.removeprefix("ipc://"))
        bridge.close()
        assert not os.path.exists(addr.removeprefix("ipc://"))

    def test_a_function_raising_base_exception_still_replies(self):
        """gevent.GreenletExit ends the handling greenlet the way a normal
        return does, so `except Exception` sent nothing at all and the caller
        learned only of its own timeout."""

        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER).start()

        async def go():
            client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
            try:
                with pytest.raises(Exception) as excinfo:
                    await client.call(_raise_greenlet_exit, timeout=10)
                return str(excinfo.value)
            finally:
                client.close()

        try:
            message = asyncio.run(go())
        finally:
            server.close()
        assert "server function killed" in message
        assert "Timed out" not in message  # the server's reply, not our grace

    def test_close_stops_handlers_still_running(self):
        """close() returns to a caller that considers the bridge done. A
        handler left looping went on running its side effects — against a
        closed socket — for the life of the process: the group is local to
        _serve, and killing _serve does not kill its members."""

        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER).start()
        _ticks.clear()

        async def go():
            client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
            try:
                with contextlib.suppress(TimeoutError):
                    await client.call(_tick_forever, timeout=1)
            finally:
                client.close()

        try:
            asyncio.run(go())  # dispatches a call that never returns
            gevent.sleep(0.5)  # the hub is ours again; the handler starts
            assert _ticks, "the handler should be running"
        finally:
            server.close()
        settled = len(_ticks)
        gevent.sleep(0.5)
        assert len(_ticks) == settled  # ...and stopped when the bridge closed

    def test_a_client_start_without_a_running_loop_refuses(self):
        """On 3.12 and 3.13 ensure_future does not refuse a caller with no
        running loop — it puts the reader on a dormant policy loop that never
        runs it, so a live server's replies are never consumed and every call
        times out. Nothing may be allocated before that is ruled out."""
        bridge = ProcessBridge(_make_addr(), mode=ProcessBridge.Mode.CLIENT)
        with pytest.raises(RuntimeError):
            bridge.start()  # no running loop here
        assert not bridge._started
        assert bridge._transport is None

    def test_a_refused_lock_leaves_nothing_claimed(self):
        """The send lock was allocated after the transport was opened AND
        published: a refusal left a live connector behind _started=False,
        where close() and __del__ skipped it and a retried start() overwrote
        the only reference."""

        async def go():
            real_lock = asyncio.Lock
            refuse = [True]

            def refusing(*a, **k):
                if refuse[0]:
                    refuse[0] = False
                    raise MemoryError("no lock for you")
                return real_lock(*a, **k)

            asyncio.Lock = refusing
            try:
                bridge = ProcessBridge(_make_addr(), mode=ProcessBridge.Mode.CLIENT)
                with pytest.raises(MemoryError):
                    bridge.start()
                assert bridge._transport is None, "a live transport was left behind"
                assert not bridge._started
            finally:
                asyncio.Lock = real_lock

        asyncio.run(go())

    def test_a_client_start_failure_leaves_nothing_claimed(self):
        """Claiming the transport before the reader task existed left a live
        socket behind a _started flag: close() was the only way out, and the
        obvious retry — start() again — silently no-opped."""

        async def go():
            def refuse(coro, *_args, **_kwargs):
                coro.close()  # dispose it here; nothing will ever await it
                raise RuntimeError("task factory refused")

            loop = asyncio.get_running_loop()
            original = loop.create_task
            loop.create_task = refuse  # only ours; asyncio.run needs its own
            try:
                bridge = ProcessBridge(_make_addr(), mode=ProcessBridge.Mode.CLIENT)
                with pytest.raises(RuntimeError):
                    bridge.start()
                assert not bridge._started
                assert bridge._transport is None
            finally:
                loop.create_task = original

        asyncio.run(go())

    def test_a_call_never_sends_on_a_generation_it_did_not_start_with(self):
        """Under DEALER backpressure a send waits on the lock. A close()+start()
        in between has already told this caller its call failed — reading
        self._transport at send time then put it on the NEW socket and the
        server ran it anyway.

        The old socket gets nothing either: a swapped transport means the one
        we captured was closed, so that send could only fail, and the caller is
        owed the ConnectionError close() already recorded for it."""

        class Recorder:
            def __init__(self):
                self.sends = 0

            async def send_multipart(self, _parts):
                self.sends += 1

        async def go():
            bridge = ProcessBridge(_make_addr(), mode=ProcessBridge.Mode.CLIENT)
            old, new = Recorder(), Recorder()
            bridge._started = True
            bridge._transport = _fake_transport(old)
            bridge._send_lock = asyncio.Lock()
            bridge._loop = asyncio.get_running_loop()
            bridge._reader_task = asyncio.ensure_future(asyncio.sleep(30))
            await bridge._send_lock.acquire()  # a send is already in flight
            call = asyncio.ensure_future(bridge.call(len, "abc", timeout=0.3))
            await asyncio.sleep(0)  # ours queues behind it
            bridge._transport = _fake_transport(new)  # ...a restart swaps it
            bridge._send_lock.release()
            with pytest.raises(ConnectionError):
                await call
            bridge._reader_task.cancel()
            with contextlib.suppress(BaseException):
                await bridge._reader_task
            return old.sends, new.sends

        assert asyncio.run(go()) == (0, 0)

    def test_the_timeout_covers_the_send(self):
        """A DEALER whose peer never arrives queues to its high-water mark and
        then blocks in send_multipart. The budget started only after the send,
        so it bounded nothing in exactly the case it was for."""

        async def go():
            bridge = ProcessBridge(_make_addr(), mode=ProcessBridge.Mode.CLIENT)
            bridge._started = True
            bridge._send_lock = asyncio.Lock()
            bridge._loop = asyncio.get_running_loop()
            bridge._reader_task = asyncio.ensure_future(asyncio.sleep(30))

            class Blocked:
                async def send_multipart(self, _parts):
                    await asyncio.sleep(30)  # backpressure: never completes

            bridge._transport = _fake_transport(Blocked())
            start = asyncio.get_running_loop().time()
            with pytest.raises(TimeoutError):
                await bridge.call(len, "abc", timeout=0.3)
            elapsed = asyncio.get_running_loop().time() - start
            bridge._reader_task.cancel()
            with contextlib.suppress(BaseException):
                await bridge._reader_task
            return elapsed

        assert asyncio.run(go()) < 5  # bounded, not the 30s send


class TestConcurrentClose:
    def test_two_closers_do_not_trip_over_each_other(self):
        """close() joins the server greenlet, which switches. Flipping
        ``_started`` only on the way out let a second closer past the guard,
        onto the same join — and then onto the ``.dead`` of the None the first
        one had left behind."""
        server = ProcessBridge(_make_addr(), mode=ProcessBridge.Mode.SERVER).start()
        closers = [gevent.spawn(server.close) for _ in range(2)]
        gevent.joinall(closers, timeout=10)
        for closer in closers:
            assert closer.successful(), closer.exception

    def test_an_interrupted_close_still_releases_the_transport(self):
        """Claiming ``_started`` up front makes this closer the only one: a
        caller's enclosing timeout landing in the join would otherwise leave a
        live socket and context that neither a later close() nor __del__ comes
        back for."""
        server = ProcessBridge(_make_addr(), mode=ProcessBridge.Mode.SERVER).start()
        assert server._transport is not None
        sock = server._transport.sock
        # Let _serve reach its 100ms poll, so the close below really does wait
        # on it and the timeout below really does land in the join.
        gevent.sleep(0.01)
        with pytest.raises(gevent.Timeout):
            with gevent.Timeout(0.02):
                server.close()
        assert sock.closed

    def test_a_start_during_close_keeps_its_own_generation(self):
        """close() waits on the server greenlet, which switches. Reading the
        bridge's fields again afterwards let the closer kill, close and unlink
        the generation a concurrent start() had just published — and leak the
        one it meant to tear down."""
        server = ProcessBridge(_make_addr(), mode=ProcessBridge.Mode.SERVER).start()
        gevent.sleep(0.01)  # let _serve reach its poll
        closer = gevent.spawn(server.close)
        gevent.sleep(0)  # the closer claims the bridge, then waits on _serve
        server.start()
        assert server._transport is not None
        new_sock = server._transport.sock
        try:
            closer.join(timeout=10)
            assert closer.successful(), closer.exception
            assert server._started
            assert not new_sock.closed, "the closer took the new generation down"
        finally:
            server.close()


class TestClosedDuringCall:
    """pyzmq's Socket.close() cancels the futures of sends still in flight, so
    closing a client bridge lands a cancellation inside call()."""

    @staticmethod
    def _client(sock):
        bridge = ProcessBridge(_make_addr(), mode=ProcessBridge.Mode.CLIENT)
        bridge._started = True
        bridge._send_lock = asyncio.Lock()
        bridge._loop = asyncio.get_running_loop()
        bridge._reader_task = asyncio.ensure_future(asyncio.sleep(30))
        bridge._transport = _fake_transport(sock)
        return bridge

    def test_a_close_during_a_blocked_send_reports_the_bridge(self):
        """Escaping as CancelledError, it reads to the caller as its own task
        being cancelled — `except Exception` does not see it at all."""

        class Cancelled:
            async def send_multipart(self, _parts):
                raise asyncio.CancelledError  # what close() does to a live send

        async def go():
            bridge = self._client(Cancelled())
            try:
                with pytest.raises(ConnectionError):
                    await bridge.call(len, "abc", timeout=5)
            finally:
                bridge._reader_task.cancel()

        asyncio.run(go())

    def test_the_callers_own_cancellation_still_cancels(self):
        """The other half: a task cancelled by its owner must not come back as
        a ConnectionError."""

        class Blocked:
            async def send_multipart(self, _parts):
                await asyncio.sleep(30)

        async def go():
            bridge = self._client(Blocked())
            call = asyncio.ensure_future(bridge.call(len, "abc", timeout=30))
            await asyncio.sleep(0.05)
            call.cancel()
            try:
                with pytest.raises(asyncio.CancelledError):
                    await call
            finally:
                bridge._reader_task.cancel()

        asyncio.run(go())

    def test_a_call_queued_behind_a_send_reports_the_close(self):
        """close() releases the socket along with the generation, so a call
        that captured the transport and then waited for the send lock finds it
        gone — and an AttributeError over a None socket is not what a closed
        bridge owes its caller."""

        class Blocked:
            async def send_multipart(self, _parts):
                await asyncio.sleep(30)

        async def go():
            bridge = self._client(Blocked())
            await bridge._send_lock.acquire()  # an earlier send is in flight
            call = asyncio.ensure_future(bridge.call(len, "abc", timeout=5))
            await asyncio.sleep(0)  # ours queues behind it
            bridge.close()
            bridge._send_lock.release()
            with pytest.raises(ConnectionError):
                await call

        asyncio.run(go())


class TestModeNormalisation:
    def test_a_string_mode_is_not_read_as_server(self):
        """Every dispatch compares by identity, so a plain "client" fell
        through to the server branch and bound the address a real server
        holds — then failed inside call() on a send lock never created."""
        bridge = ProcessBridge(_make_addr(), mode="client")
        assert bridge._mode is ProcessBridge.Mode.CLIENT

    def test_an_unknown_mode_is_refused(self):
        with pytest.raises(ValueError):
            ProcessBridge(_make_addr(), mode="listener")


async def _call_and_report(bridge):
    result = await bridge.call(len, "abc", timeout=10)
    return result, bridge._transport


class TestLoopChange:
    def test_a_new_loop_gets_a_new_send_lock(self):
        """pyzmq migrates the socket between loops; asyncio.Lock does not. Once
        contended under the loop that is gone it stays bound to it, and the
        second concurrent call under the next asyncio.run() raises "bound to a
        different event loop" rather than completing or timing out."""

        class Blocked:
            async def send_multipart(self, _parts):
                await asyncio.sleep(30)

            async def recv_multipart(self):  # for the reader the revival spawns
                await asyncio.sleep(30)

        bridge = ProcessBridge(_make_addr(), mode=ProcessBridge.Mode.CLIENT)

        async def under_first_loop():
            bridge._started = True
            bridge._send_lock = asyncio.Lock()
            bridge._loop = asyncio.get_running_loop()
            bridge._transport = _fake_transport(Blocked())
            bridge._reader_task = asyncio.ensure_future(asyncio.sleep(30))
            with contextlib.suppress(BaseException):
                await bridge.call(len, "abc", timeout=0.2)
            bridge._reader_task.cancel()
            with contextlib.suppress(BaseException):
                await bridge._reader_task
            return bridge._send_lock

        first_lock = asyncio.run(under_first_loop())

        async def under_second_loop():
            with contextlib.suppress(BaseException):
                await bridge.call(len, "abc", timeout=0.2)
            reader = bridge._reader_task
            if reader is not None:
                reader.cancel()
                with contextlib.suppress(BaseException):
                    await reader
            return bridge._send_lock

        assert asyncio.run(under_second_loop()) is not first_lock

    def test_a_call_after_the_old_loop_closed(self):
        """The reader belongs to a loop that no longer runs, and cancelling it
        there raises "Event loop is closed" — failing the very call that came
        to rebuild the client."""
        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER).start()
        client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
        loop = asyncio.new_event_loop()
        try:
            assert loop.run_until_complete(client.call(len, "abc", timeout=10)) == 3
        finally:
            loop.close()
        try:
            assert asyncio.run(client.call(len, "abcd", timeout=10)) == 4
        finally:
            client.close()
            server.close()

    @pytest.mark.filterwarnings("ignore:Task was destroyed but it is pending")
    def test_a_call_after_the_old_loop_closed_with_one_pending(self):
        """Same as above one statement further in: a future belonging to the
        closed loop cannot be completed either — set_exception schedules its
        callbacks there and raises. Its waiter went with that loop; the call
        that came to rebuild the client must not go with it too."""
        addr = _make_addr()
        client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)  # no server
        loop = asyncio.new_event_loop()

        async def leave_one_pending():
            asyncio.ensure_future(client.call(len, "abc", timeout=30))
            await asyncio.sleep(0.2)  # let it register and send into the void

        try:
            loop.run_until_complete(leave_one_pending())
        finally:
            loop.close()

        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER).start()
        try:
            assert asyncio.run(client.call(len, "abcd", timeout=10)) == 4
        finally:
            client.close()
            server.close()

    def test_a_new_loop_gets_a_new_transport(self):
        """pyzmq migrates the FD watcher between loops but keeps one receive
        queue, so a reader left behind on the stopped loop stays ahead of the
        new one and the reply goes to a future that will never run. Closing is
        what clears that queue — the whole client is rebuilt."""
        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER).start()
        client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
        try:
            first = asyncio.run(_call_and_report(client))
            second = asyncio.run(_call_and_report(client))
            assert first[0] == second[0] == 3  # both calls really answered
            assert first[1] is not second[1]  # and on different transports
        finally:
            client.close()
            server.close()


class TestServerSpawnRefused:
    def test_a_refused_spawn_leaves_the_bridge_startable(self, monkeypatch):
        """_started and the transport are published before the serving greenlet
        exists. A spawn that refuses left the bridge claiming to be up over a
        bound socket nothing serves — and start() no-ops on the retry."""
        real_spawn = gevent.spawn

        def refuse(fn, *args, **kwargs):
            if getattr(fn, "__name__", "") == "_serve":
                raise RuntimeError("the hub refused a greenlet")
            return real_spawn(fn, *args, **kwargs)

        addr = _make_addr()
        monkeypatch.setattr(gevent, "spawn", refuse)
        bridge = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER)
        with pytest.raises(RuntimeError, match="refused"):
            bridge.start()
        assert not bridge._started and bridge._transport is None
        assert not os.path.exists(addr[6:])

        monkeypatch.undo()
        bridge.start()  # and the retry works
        bridge.close()


class TestHandlerSpawnRefused:
    def test_it_answers_the_call_and_keeps_serving(self, monkeypatch):
        """A spawn the hub refuses is one request's failure. Letting it out of
        the serve loop ended the server with _started still true over a bound
        socket: start() no-opped, and every later caller waited out its own
        timeout."""
        import gevent.pool

        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER).start()
        real_spawn = gevent.pool.Group.spawn
        refuse = [True]

        def maybe_refuse(self, *args, **kwargs):
            if refuse[0]:
                refuse[0] = False
                raise RuntimeError("the hub refused a greenlet")
            return real_spawn(self, *args, **kwargs)

        monkeypatch.setattr(gevent.pool.Group, "spawn", maybe_refuse)

        async def go():
            client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
            try:
                with pytest.raises(Exception, match="refused"):
                    await client.call(len, "abc", timeout=5)
                # And the server is still there for the next one.
                return await client.call(len, "abcd", timeout=5)
            finally:
                client.close()

        assert asyncio.run(go()) == 4
        server.close()


_unwound: list[str] = []


def _hang_until_killed():
    import gevent

    try:
        while True:
            gevent.sleep(0.05)
    finally:
        gevent.sleep(0.5)
        _unwound.append("unwound")


class TestServeLoopFailure:
    def test_a_refused_spawn_replies_under_the_send_lock(self, monkeypatch):
        """The refusal reply is sent from the serve loop while a handler may be
        mid-send: a green multipart send yields between frames, so an unlocked
        send can interleave its frames with a reply already half on the wire."""
        import gevent.lock
        import gevent.pool

        from gisolate._workers import ERR

        sems = []
        real_sem = gevent.lock.Semaphore

        class Recorded(real_sem):
            def __init__(self, *a, **k):
                super().__init__(*a, **k)
                sems.append(self)

        locked_at_err_send = []
        real_send = zmq.green.Socket.send_multipart

        def observing(sock, frames, *a, **k):
            if len(frames) == 4 and frames[2] == ERR:
                locked_at_err_send.append(any(s.locked() for s in sems))
            return real_send(sock, frames, *a, **k)

        real_spawn = gevent.pool.Group.spawn
        refuse = [True]

        def maybe_refuse(group, fn, *args, **kwargs):
            if refuse[0] and getattr(fn, "__name__", "") == "_handle":
                refuse[0] = False
                raise MemoryError("the hub refused a greenlet")
            return real_spawn(group, fn, *args, **kwargs)

        monkeypatch.setattr(gevent.lock, "Semaphore", Recorded)
        monkeypatch.setattr(zmq.green.Socket, "send_multipart", observing)
        monkeypatch.setattr(gevent.pool.Group, "spawn", maybe_refuse)

        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER).start()
        ctx = zmq.green.Context()
        client = ctx.socket(zmq.DEALER)
        try:
            client.connect(addr)
            client.send_multipart([(1).to_bytes(8), b"junk"])
            assert client.poll(10_000), "the refused request went unanswered"
            assert client.recv_multipart()[1] == ERR
            # Exactly the serve loop's lock, so an unrelated locked semaphore
            # cannot vouch for an unlocked send.
            assert len(sems) == 1, sems
            assert locked_at_err_send == [True], (
                "the refusal reply went out without the send lock"
            )
        finally:
            client.close(linger=0)
            ctx.term()
            server.close()

    def test_a_refused_prelude_releases_the_claim(self, monkeypatch):
        """_serve's own allocations — the group, the send lock, the handler's
        function object — sit before its loop: a refusal there died with
        _started still true over the bound transport, and start() no-opped
        for good."""
        import time

        import gevent.lock

        real_sem = gevent.lock.Semaphore
        refuse = [True]

        def refusing_sem(*a, **k):
            if refuse[0]:
                refuse[0] = False
                raise MemoryError("no semaphore for you")
            return real_sem(*a, **k)

        monkeypatch.setattr(gevent.lock, "Semaphore", refusing_sem)
        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER).start()
        try:
            deadline = time.monotonic() + 5
            while server._started and time.monotonic() < deadline:
                gevent.sleep(0.05)
            assert not server._started, "the dead prelude left the bridge claimed"
            monkeypatch.undo()

            server.start()

            async def go():
                client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
                try:
                    return await client.call(lambda: "reborn", timeout=10)
                finally:
                    client.close()

            assert asyncio.run(go()) == "reborn"
        finally:
            server.close()

    def test_a_dead_serve_loop_releases_its_claim(self):
        """A real mid-serve failure — poll raising — ended the loop while
        _started stayed True over a bound socket: start() no-opped forever and
        every later call timed out against a bridge that looked alive."""
        import time

        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER).start()
        try:
            assert server._transport is not None
            server._transport.sock.close()  # the loop's next poll raises
            deadline = time.monotonic() + 5
            while server._started and time.monotonic() < deadline:
                gevent.sleep(0.05)
            assert not server._started, "the dead loop left the bridge claimed"

            server.start()  # the claim was released, so this rebuilds

            async def go():
                client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
                try:
                    return await client.call(lambda: "reborn", timeout=10)
                finally:
                    client.close()

            assert asyncio.run(go()) == "reborn"
        finally:
            server.close()

    def test_close_returns_only_after_handlers_unwind(self):
        """group.kill(block=False) only SCHEDULED the GreenletExit: close()
        returned while a handler was still unwinding through its finally, its
        side effects landing after the bridge was reportedly done."""
        addr = _make_addr()
        server = ProcessBridge(addr, mode=ProcessBridge.Mode.SERVER).start()
        _unwound.clear()

        async def go():
            client = ProcessBridge(addr, mode=ProcessBridge.Mode.CLIENT)
            try:
                with contextlib.suppress(TimeoutError):
                    await client.call(_hang_until_killed, timeout=1)
            finally:
                client.close()

        try:
            asyncio.run(go())
            gevent.sleep(0.5)  # the hub is ours again; the handler parks
        finally:
            server.close()
        assert _unwound, "close() returned before the handler's finally ran"
