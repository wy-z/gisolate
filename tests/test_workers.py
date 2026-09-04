"""Tests for gisolate._workers module."""

import contextlib
import os
import pathlib
import time


from gisolate import _workers
from gisolate._internal import SmartPickle
from gisolate._workers import (ERR, OK, SHUTDOWN, _malformed, safe_close,
                               safe_dumps)

from .helpers import ExitsOnSecondPickle, FailsToSerializeHostilely


class TestSafeClose:
    def test_calls_close(self):
        closed = []

        class Client:
            def close(self):
                closed.append(True)

        safe_close(Client())
        assert closed == [True]

    def test_no_close_method(self):
        safe_close(object())  # should not raise

    def test_close_raises(self):
        class BadClient:
            def close(self):
                raise RuntimeError("boom")

        safe_close(BadClient())  # should not raise

    def test_looking_the_method_up_raises(self):
        """The lookup is the client's code too — a __getattr__ or a property
        runs here. Outside the guard, a SystemExit from it left serve() instead
        of staying the client's own cleanup decision."""

        class HostileLookup:
            def __getattr__(self, name):
                raise SystemExit(f"exiting rather than answering {name!r}")

        safe_close(HostileLookup())  # should not raise


class TestSafeDumpsBaseException:
    def test_a_base_exception_from_serialization_becomes_a_reply(self):
        """Last step before a reply goes on the wire. A __reduce__ raising a
        BaseException — or a SIGINT landing here — used to kill the handler
        with nothing sent, and in the asyncio worker a SystemExit escaping a
        task takes the whole worker down."""

        class Hostile:
            def __reduce__(self):
                raise KeyboardInterrupt("SIGINT while serializing the reply")

        payload, ok = safe_dumps(Hostile(), True)
        assert ok is False
        assert isinstance(payload, bytes) and payload

    def test_a_reply_that_cannot_be_serialized_either_still_becomes_one(self):
        """The error reply is serialized a SECOND time, outside the guard that
        made the first one safe. wrap_exception only proves the exception
        pickles ONCE — a reducer that answers differently on the next call, or
        a MemoryError on a large error object, escaped from there and killed
        the handler with nothing sent."""
        ExitsOnSecondPickle.calls = 0

        payload, ok = safe_dumps(FailsToSerializeHostilely(), True)
        assert ok is False
        assert isinstance(SmartPickle.loads(payload), Exception)


class TestMalformedRequest:
    def test_an_unprintable_parse_failure_still_becomes_a_reply(self):
        """_malformed runs in the request loop, and repr() is the client's code
        as much as str() is: letting it out ends the worker for every client."""

        class Unprintable(Exception):
            def __repr__(self):
                raise KeyboardInterrupt("raised while formatting the failure")

        wrapped = _malformed(Unprintable())
        assert isinstance(wrapped, Exception)
        assert "malformed request" in str(wrapped)


class TestTeardownOutlivesTheClient:
    def test_a_close_that_raises_still_releases_the_transport(self, monkeypatch):
        """The client's close() is the last user code the worker runs. Under
        serve() no process exit follows it, so a BaseException escaping it —
        an expiring shutdown hook — stranded the ROUTER socket and its context
        on a host that goes on to restart the worker."""
        import tempfile
        import uuid

        import dill
        import gevent
        import zmq.green

        from gisolate._workers import WorkerConfig, gevent_worker

        from .helpers import unclosable_factory

        addr = f"ipc://{tempfile.gettempdir()}/gi-worker-{uuid.uuid4().hex}.sock"
        sockets = []
        real_socket = zmq.green.Context.socket

        def recording(self, *args, **kwargs):
            sock = real_socket(self, *args, **kwargs)
            sockets.append(sock)
            return sock

        monkeypatch.setattr(zmq.green.Context, "socket", recording)
        worker = gevent.spawn(
            gevent_worker,
            WorkerConfig(ipc_addr=addr, factory_bytes=dill.dumps(unclosable_factory)),
            {},
        )
        try:
            for _ in range(100):
                if sockets and os.path.exists(addr.removeprefix("ipc://")):
                    break
                gevent.sleep(0.05)
            assert sockets, "worker never opened its socket"

            ctx = zmq.green.Context()
            client = ctx.socket(zmq.DEALER)
            try:
                client.connect(addr)
                # One real call first: the client is built lazily, and a worker
                # that never built one has nothing to close.
                req = SmartPickle.dumps(("ping", (), {}, time.monotonic() + 10))
                client.send_multipart([(1).to_bytes(8), req])
                assert client.poll(10_000), "worker never replied"
                reply = client.recv_multipart()
                assert reply[1] == OK and SmartPickle.loads(reply[2]) == "pong"

                client.send_multipart([b"0", SHUTDOWN])
                worker.join(timeout=10)
            finally:
                client.close(linger=0)
                ctx.term()

            assert worker.dead
            assert isinstance(worker.exception, gevent.Timeout)  # close() escaped
            assert sockets[0].closed, "the worker's ROUTER was left open"
        finally:
            worker.kill(block=True, timeout=5)
            with contextlib.suppress(OSError):
                os.unlink(addr.removeprefix("ipc://"))


class TestStragglingHandlers:
    def test_a_handler_outliving_the_join_is_stopped(self, monkeypatch):
        """The worker loop is not followed by a process exit under serve(): a
        handler still running when it returns holds the old client and goes on
        producing side effects for a host that has already moved on."""
        import tempfile
        import uuid

        import dill
        import gevent
        import zmq.green

        from gisolate._workers import WorkerConfig, gevent_worker

        from .helpers import _worker_ticks, ticker_factory

        monkeypatch.setattr(_workers, "_HANDLER_DRAIN_GRACE", 0.5)
        addr = f"ipc://{tempfile.gettempdir()}/gi-worker-{uuid.uuid4().hex}.sock"
        worker = gevent.spawn(
            gevent_worker,
            WorkerConfig(ipc_addr=addr, factory_bytes=dill.dumps(ticker_factory)),
            {},
        )
        ctx = zmq.green.Context()
        client = ctx.socket(zmq.DEALER)
        try:
            for _ in range(100):
                if os.path.exists(addr.removeprefix("ipc://")):
                    break
                gevent.sleep(0.05)
            client.connect(addr)
            # A deadline far past the worker's own join, so only the
            # kill after it can end this call.
            req = SmartPickle.dumps(("tick_forever", (), {}, time.monotonic() + 120))
            client.send_multipart([(1).to_bytes(8), req])
            for _ in range(100):
                if _worker_ticks:
                    break
                gevent.sleep(0.05)
            assert _worker_ticks, "the call never started"

            client.send_multipart([b"0", SHUTDOWN])
            worker.join(timeout=30)
            assert worker.dead

            settled = len(_worker_ticks)
            gevent.sleep(0.3)
            assert len(_worker_ticks) == settled, "the handler outlived the worker"
        finally:
            worker.kill(block=True, timeout=5)
            client.close(linger=0)
            ctx.term()
            with contextlib.suppress(OSError):
                os.unlink(addr.removeprefix("ipc://"))

    def test_the_client_closes_only_after_the_stragglers_unwind(self, monkeypatch):
        """kill() SCHEDULES the GreenletExit; it does not deliver it. Closing
        the client in the next statement therefore ran the client's own close()
        while a handler was still in its finally — cleaning up against the
        object being closed."""
        import tempfile
        import uuid

        import dill
        import gevent
        import zmq.green

        from gisolate._workers import WorkerConfig, gevent_worker

        from .helpers import _unwind_order, unwind_order_factory

        monkeypatch.setattr(_workers, "_HANDLER_DRAIN_GRACE", 0.5)
        addr = f"ipc://{tempfile.gettempdir()}/gi-unwind-{uuid.uuid4().hex[:8]}.sock"
        worker = gevent.spawn(
            gevent_worker,
            WorkerConfig(ipc_addr=addr, factory_bytes=dill.dumps(unwind_order_factory)),
            {},
        )
        ctx = zmq.green.Context()
        client = ctx.socket(zmq.DEALER)
        try:
            for _ in range(200):
                if os.path.exists(addr.removeprefix("ipc://")):
                    break
                gevent.sleep(0.05)
            client.connect(addr)
            # Past the worker's own join, so only the kill ends it.
            req = SmartPickle.dumps(("wait_forever", (), {}, time.monotonic() + 120))
            client.send_multipart([(1).to_bytes(8), req])
            gevent.sleep(0.5)  # let the call start

            client.send_multipart([b"0", SHUTDOWN])
            worker.join(timeout=30)
            assert worker.dead

            assert _unwind_order == ["handler", "close"], _unwind_order
        finally:
            worker.kill(block=True, timeout=5)
            client.close(linger=0)
            ctx.term()
            with contextlib.suppress(OSError):
                os.unlink(addr.removeprefix("ipc://"))

    def test_a_worker_killed_mid_join_still_releases_the_transport(self):
        """The teardown join is a switch point, and under serve() nothing
        follows this loop: a host killing the greenlet it ran serve() in left
        the ROUTER and its context open, with peers still connected."""
        import tempfile
        import uuid

        import dill
        import gevent
        import zmq.green

        from gisolate._workers import WorkerConfig, gevent_worker

        from .helpers import _worker_ticks, ticker_factory

        addr = f"ipc://{tempfile.gettempdir()}/gi-worker-{uuid.uuid4().hex}.sock"
        sockets = []
        real_socket = zmq.green.Context.socket

        def recording(self, *args, **kwargs):
            sock = real_socket(self, *args, **kwargs)
            sockets.append(sock)
            return sock

        zmq.green.Context.socket = recording
        try:
            worker = gevent.spawn(
                gevent_worker,
                WorkerConfig(ipc_addr=addr, factory_bytes=dill.dumps(ticker_factory)),
                {},
            )
            for _ in range(100):
                if sockets and os.path.exists(addr.removeprefix("ipc://")):
                    break
                gevent.sleep(0.05)
            assert sockets, "worker never opened its socket"
        finally:
            zmq.green.Context.socket = real_socket

        ctx = zmq.green.Context()
        client = ctx.socket(zmq.DEALER)
        try:
            client.connect(addr)
            # A call that never returns, so SHUTDOWN leaves the loop waiting in
            # its join — where the kill below lands.
            before = len(_worker_ticks)
            req = SmartPickle.dumps(("tick_forever", (), {}, time.monotonic() + 120))
            client.send_multipart([(1).to_bytes(8), req])
            for _ in range(100):
                if len(_worker_ticks) > before:
                    break
                gevent.sleep(0.05)
            assert len(_worker_ticks) > before, "the call never started"

            client.send_multipart([b"0", SHUTDOWN])
            # Past the 500ms poll that carries the SHUTDOWN, so the kill lands
            # in the join rather than in the loop it already left.
            gevent.sleep(1.0)
            worker.kill(block=False)

            for _ in range(200):
                if sockets[0].closed:
                    break
                gevent.sleep(0.05)
            assert sockets[0].closed, "the worker's ROUTER was left open"
        finally:
            worker.kill(block=True, timeout=5)
            client.close(linger=0)
            ctx.term()
            with contextlib.suppress(OSError):
                os.unlink(addr.removeprefix("ipc://"))


class TestAsyncioWorkerReturns:
    def test_a_build_that_never_finishes_does_not_hold_the_worker(self, monkeypatch):
        """The build is shielded, so it is never cancelled — an ordinary async
        connect() waiting on something that never arrives is not the
        swallow-your-cancellation case, and an unbounded teardown join means
        serve() never returns at all. A spawned child hides this: its parent
        terminates it either way."""
        import tempfile
        import uuid

        import dill
        import gevent
        import gevent.monkey
        import zmq.green

        from gisolate._workers import WorkerConfig, asyncio_worker

        from .helpers import never_connects_factory

        monkeypatch.setattr(_workers, "_HANDLER_DRAIN_GRACE", 0.5)
        monkeypatch.setattr(_workers, "_BUILD_DRAIN_GRACE", 0.5)
        Thread = gevent.monkey.get_original("threading", "Thread")
        addr = f"ipc://{tempfile.gettempdir()}/gi-async-{uuid.uuid4().hex[:8]}.sock"
        worker = Thread(
            target=asyncio_worker,
            args=(
                WorkerConfig(
                    ipc_addr=addr, factory_bytes=dill.dumps(never_connects_factory)
                ),
            ),
            daemon=True,
        )
        worker.start()
        ctx = zmq.green.Context()
        client = ctx.socket(zmq.DEALER)
        try:
            for _ in range(200):
                if os.path.exists(addr.removeprefix("ipc://")):
                    break
                gevent.sleep(0.05)
            assert os.path.exists(addr.removeprefix("ipc://")), "worker never bound"
            client.connect(addr)
            # A call that starts the build and then gives up on it.
            req = SmartPickle.dumps(("ping", (), {}, time.monotonic() + 0.3))
            client.send_multipart([(1).to_bytes(8), req])
            assert client.poll(10_000), "worker never replied"
            client.recv_multipart()

            client.send_multipart([b"0", SHUTDOWN])
            deadline = time.monotonic() + 25
            while worker.is_alive() and time.monotonic() < deadline:
                gevent.sleep(0.1)
            assert not worker.is_alive(), "the worker never returned"
        finally:
            client.close(linger=0)
            ctx.term()
            with contextlib.suppress(OSError):
                os.unlink(addr.removeprefix("ipc://"))


class TestBuildCancelledAtTeardown:
    def test_close_never_precedes_the_connect_it_would_release(self, tmp_path, monkeypatch):
        """Past the teardown join, asyncio.run cancels the build — but off_loop
        runs a synchronous connect where cancellation does not reach. Treating
        that cancellation as a failed connect closed the client before it had
        anything to release, and the fd its thread went on to open was left
        with a close that had already happened."""
        import functools
        import tempfile
        import uuid

        import dill
        import gevent
        import gevent.monkey
        import zmq.green

        from gisolate._workers import WorkerConfig, asyncio_worker

        from .helpers import late_connect_factory

        monkeypatch.setattr(_workers, "_HANDLER_DRAIN_GRACE", 0.5)
        monkeypatch.setattr(_workers, "_BUILD_DRAIN_GRACE", 0.5)
        Thread = gevent.monkey.get_original("threading", "Thread")
        marker = tmp_path / "events.txt"
        addr = f"ipc://{tempfile.gettempdir()}/gi-late-{uuid.uuid4().hex[:8]}.sock"
        worker = Thread(
            target=asyncio_worker,
            args=(
                WorkerConfig(
                    ipc_addr=addr,
                    factory_bytes=dill.dumps(
                        functools.partial(late_connect_factory, str(marker), 3.0)
                    ),
                ),
            ),
            daemon=True,
        )
        worker.start()
        ctx = zmq.green.Context()
        client = ctx.socket(zmq.DEALER)
        try:
            for _ in range(200):
                if os.path.exists(addr.removeprefix("ipc://")):
                    break
                gevent.sleep(0.05)
            client.connect(addr)
            req = SmartPickle.dumps(("ping", (), {}, time.monotonic() + 0.3))
            client.send_multipart([(1).to_bytes(8), req])
            assert client.poll(10_000), "worker never replied"
            client.recv_multipart()

            client.send_multipart([b"0", SHUTDOWN])
            deadline = time.monotonic() + 25
            while worker.is_alive() and time.monotonic() < deadline:
                gevent.sleep(0.1)
            assert not worker.is_alive(), "the worker never returned"

            # The executor thread is joined before asyncio.run returns, so the
            # connect has landed by now; asserting it PRESENT is what keeps an
            # empty marker from passing.
            events = marker.read_text().split() if marker.exists() else []
            assert events == ["connected"], f"closed before connecting: {events}"
        finally:
            client.close(linger=0)
            ctx.term()
            with contextlib.suppress(OSError):
                os.unlink(addr.removeprefix("ipc://"))


class TestClientExitOnClose:
    def test_it_does_not_end_the_serve_host(self):
        """A client calling sys.exit(2) in its own cleanup is that client's
        decision. Under serve() the worker had already released everything, and
        letting it out ended a host that an `except Exception` restart loop
        could not even catch. Run in a child, because without the fix the
        SystemExit escapes to whatever process runs it."""
        import subprocess
        import sys
        import tempfile
        import textwrap
        import uuid

        addr = f"ipc://{tempfile.gettempdir()}/gi-exit-{uuid.uuid4().hex[:8]}.sock"
        script = textwrap.dedent(
            f"""
            from gevent import monkey

            monkey.patch_all()

            import dill
            import gevent
            import zmq
            import zmq.green

            from gisolate._internal import SmartPickle
            from gisolate._workers import SHUTDOWN, WorkerConfig, gevent_worker
            from tests.helpers import exits_on_close_factory

            import time

            addr = {addr!r}
            cfg = WorkerConfig(
                ipc_addr=addr, factory_bytes=dill.dumps(exits_on_close_factory)
            )
            worker = gevent.spawn(gevent_worker, cfg, {{}})
            ctx = zmq.green.Context()
            client = ctx.socket(zmq.DEALER)
            for _ in range(200):
                if __import__("os").path.exists(addr[6:]):
                    break
                gevent.sleep(0.05)
            client.connect(addr)
            req = SmartPickle.dumps(("ping", (), {{}}, time.monotonic() + 10))
            client.send_multipart([(1).to_bytes(8), req])
            client.poll(10000)
            client.recv_multipart()
            client.send_multipart([b"0", SHUTDOWN])
            worker.join(timeout=20)
            print("HOST SURVIVED", flush=True)
            """
        )
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            cwd=str(pathlib.Path(__file__).resolve().parent.parent),
            timeout=60,
        )
        with contextlib.suppress(OSError):
            os.unlink(addr.removeprefix("ipc://"))
        assert "HOST SURVIVED" in proc.stdout, (proc.returncode, proc.stderr[-800:])


class TestHandlersStopBeforeDispose:
    def test_no_call_runs_after_the_client_is_closed(self, tmp_path):
        """The teardown's wait for the build can complete it — and the
        shield's wake was registered before that wait's, so a handler parked
        on the build resumes AHEAD of teardown and submits the very call
        shutdown exists to prevent, into an object _dispose is about to
        close. Handlers are cancelled BEFORE the build wait instead.

        Run in a child with no monkey patching — the worker's real shape. In
        a patched host the executor's threads are greenlets nothing drives,
        so the schedule never happens there and the old in-process version of
        this test passed vacuously; "closed" is asserted PRESENT so an empty
        marker can never pass again."""
        import subprocess
        import sys
        import tempfile
        import textwrap
        import uuid

        marker = tmp_path / "events.txt"
        addr = f"ipc://{tempfile.gettempdir()}/gi-drain-{uuid.uuid4().hex[:8]}.sock"
        script = textwrap.dedent(
            f"""
            import functools
            import threading
            import time

            import dill
            import zmq

            from gisolate import _workers
            from gisolate._internal import SmartPickle
            from gisolate._workers import SHUTDOWN, WorkerConfig, asyncio_worker
            from tests.helpers import slow_build_marker

            _workers._HANDLER_DRAIN_GRACE = 1.0  # the build below outlasts it
            addr = {addr!r}
            cfg = WorkerConfig(
                ipc_addr=addr,
                factory_bytes=dill.dumps(
                    functools.partial(slow_build_marker, {str(marker)!r}, 2.0)
                ),
            )
            worker = threading.Thread(target=asyncio_worker, args=(cfg,), daemon=True)
            worker.start()
            ctx = zmq.Context()
            client = ctx.socket(zmq.DEALER)
            for _ in range(200):
                if __import__("os").path.exists(addr[6:]):
                    break
                time.sleep(0.05)
            client.connect(addr)
            # A deadline long enough that the handler is still waiting on the
            # build when the shutdown drain gives up on it.
            req = SmartPickle.dumps(("ping", (), {{}}, time.monotonic() + 120))
            client.send_multipart([(1).to_bytes(8), req])
            time.sleep(0.5)
            client.send_multipart([b"0", SHUTDOWN])
            worker.join(timeout=30)
            print("WORKER DEAD" if not worker.is_alive() else "WORKER ALIVE", flush=True)
            client.close(linger=0)
            ctx.term()
            """
        )
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            cwd=str(pathlib.Path(__file__).resolve().parent.parent),
            timeout=90,
        )
        with contextlib.suppress(OSError):
            os.unlink(addr.removeprefix("ipc://"))
        assert "WORKER DEAD" in proc.stdout, (proc.returncode, proc.stderr[-800:])
        events = marker.read_text().split() if marker.exists() else []
        assert "closed" in events, f"the dispose never saw the build's client: {events}"
        assert "called" not in events, f"a call ran against a closing client: {events}"


class TestSlotAccounting:
    def test_a_refused_invocation_spawn_gives_the_slot_back(self, monkeypatch):
        """The slot is released by a link on the invoking greenlet, so a spawn
        the hub refuses left nothing to release it: with max_concurrency=1 the
        one slot stayed taken and every later call waited out its deadline."""
        import tempfile
        import uuid

        import dill
        import gevent
        import gevent.pool
        import zmq.green

        from gisolate._workers import WorkerConfig, gevent_worker

        from .helpers import adder_factory

        addr = f"ipc://{tempfile.gettempdir()}/gi-slot-{uuid.uuid4().hex[:8]}.sock"
        worker = gevent.spawn(
            gevent_worker,
            WorkerConfig(
                ipc_addr=addr,
                factory_bytes=dill.dumps(adder_factory),
                max_concurrency=1,
            ),
            {},
        )
        real_spawn = gevent.pool.Group.spawn
        refuse = [True]

        def maybe_refuse(self, fn, *args, **kwargs):
            if refuse[0] and getattr(fn, "__name__", "") == "_invoke":
                refuse[0] = False
                raise RuntimeError("the hub refused a greenlet")
            return real_spawn(self, fn, *args, **kwargs)

        ctx = zmq.green.Context()
        client = ctx.socket(zmq.DEALER)
        try:
            for _ in range(200):
                if os.path.exists(addr.removeprefix("ipc://")):
                    break
                gevent.sleep(0.05)
            client.connect(addr)
            monkeypatch.setattr(gevent.pool.Group, "spawn", maybe_refuse)
            req = SmartPickle.dumps(("add", (1, 1), {}, time.monotonic() + 10))
            client.send_multipart([(1).to_bytes(8), req])
            assert client.poll(10_000), "no reply to the refused call"
            assert client.recv_multipart()[1] == ERR
            monkeypatch.undo()

            # The slot has to be back, or this one never runs.
            req = SmartPickle.dumps(("add", (2, 3), {}, time.monotonic() + 10))
            client.send_multipart([(2).to_bytes(8), req])
            assert client.poll(10_000), "the slot was never given back"
            reply = client.recv_multipart()
            assert reply[1] == OK and SmartPickle.loads(reply[2]) == 5
        finally:
            client.close(linger=0)
            ctx.term()
            worker.kill(block=True, timeout=5)
            with contextlib.suppress(OSError):
                os.unlink(addr.removeprefix("ipc://"))


class TestRawlinkRefused:
    def test_a_refused_link_kills_the_invocation_it_would_release(
        self, monkeypatch, tmp_path
    ):
        """The spawn took, so _invoke was scheduled: releasing the slot and
        answering with an error left the call RUNNING — its side effects landed
        behind a reply that said they had not, and a second call could run
        beside it through max_concurrency=1."""
        import functools
        import tempfile
        import uuid

        import dill
        import gevent
        import gevent.pool
        import zmq.green

        from gisolate._workers import WorkerConfig, gevent_worker

        from .helpers import Marker

        marker = tmp_path / "ran.txt"
        addr = f"ipc://{tempfile.gettempdir()}/gi-link-{uuid.uuid4().hex[:8]}.sock"
        worker = gevent.spawn(
            gevent_worker,
            WorkerConfig(
                ipc_addr=addr,
                factory_bytes=dill.dumps(functools.partial(Marker, str(marker))),
                max_concurrency=1,
            ),
            {},
        )

        real_spawn = gevent.pool.Group.spawn
        refuse = [True]

        class LinkRefused:
            def __init__(self, g):
                self._g = g

            def rawlink(self, _cb):
                raise MemoryError("the link was refused")

            def __getattr__(self, name):
                return getattr(self._g, name)

        def wrap(group, fn, *args, **kwargs):
            g = real_spawn(group, fn, *args, **kwargs)
            if refuse[0] and getattr(fn, "__name__", "") == "_invoke":
                refuse[0] = False
                return LinkRefused(g)
            return g

        import zmq

        ctx = zmq.green.Context()
        client = ctx.socket(zmq.DEALER)
        try:
            for _ in range(200):
                if os.path.exists(addr.removeprefix("ipc://")):
                    break
                gevent.sleep(0.05)
            client.connect(addr)
            monkeypatch.setattr(gevent.pool.Group, "spawn", wrap)
            req = SmartPickle.dumps(("mark", (), {}, time.monotonic() + 10))
            client.send_multipart([(1).to_bytes(8), req])
            assert client.poll(10_000), "no reply to the refused call"
            assert client.recv_multipart()[1] == ERR
            monkeypatch.undo()

            # The reply said the call failed; nothing may run behind it.
            gevent.sleep(0.3)
            assert not marker.exists(), "the invocation ran despite the error reply"

            # And the slot is back.
            req = SmartPickle.dumps(("ping", (), {}, time.monotonic() + 10))
            client.send_multipart([(2).to_bytes(8), req])
            assert client.poll(10_000), "the slot was never given back"
            reply = client.recv_multipart()
            assert reply[1] == OK and SmartPickle.loads(reply[2]) == "pong"
        finally:
            client.close(linger=0)
            ctx.term()
            worker.kill(block=True, timeout=5)
            with contextlib.suppress(OSError):
                os.unlink(addr.removeprefix("ipc://"))

    def test_a_refused_kill_still_gives_the_slot_back(self, monkeypatch, tmp_path):
        """The kill can itself refuse under the same pressure that broke the
        link. Skipping the release then wedged max_concurrency=1 for the life
        of the worker — the call runs anyway (the kill failed), but the slot
        must come back."""
        import functools
        import tempfile
        import uuid

        import dill
        import gevent
        import gevent.pool
        import zmq
        import zmq.green

        from gisolate._workers import WorkerConfig, gevent_worker

        from .helpers import Marker

        marker = tmp_path / "ran.txt"
        addr = f"ipc://{tempfile.gettempdir()}/gi-kill-{uuid.uuid4().hex[:8]}.sock"
        worker = gevent.spawn(
            gevent_worker,
            WorkerConfig(
                ipc_addr=addr,
                factory_bytes=dill.dumps(functools.partial(Marker, str(marker))),
                max_concurrency=1,
            ),
            {},
        )

        real_spawn = gevent.pool.Group.spawn
        refuse = [True]
        kills = []

        class EverythingRefused:
            def __init__(self, g):
                self._g = g

            def rawlink(self, _cb):
                raise MemoryError("the link was refused")

            def kill(self, *_a, **_k):
                kills.append(1)
                raise MemoryError("the kill was refused too")

            def __getattr__(self, name):
                return getattr(self._g, name)

        def wrap(group, fn, *args, **kwargs):
            g = real_spawn(group, fn, *args, **kwargs)
            if refuse[0] and getattr(fn, "__name__", "") == "_invoke":
                refuse[0] = False
                return EverythingRefused(g)
            return g

        ctx = zmq.green.Context()
        client = ctx.socket(zmq.DEALER)
        try:
            for _ in range(200):
                if os.path.exists(addr.removeprefix("ipc://")):
                    break
                gevent.sleep(0.05)
            client.connect(addr)
            monkeypatch.setattr(gevent.pool.Group, "spawn", wrap)
            req = SmartPickle.dumps(("mark", (), {}, time.monotonic() + 10))
            client.send_multipart([(1).to_bytes(8), req])
            assert client.poll(10_000), "no reply to the refused call"
            assert client.recv_multipart()[1] == ERR
            monkeypatch.undo()

            # The slot has to be back even though the kill refused.
            req = SmartPickle.dumps(("ping", (), {}, time.monotonic() + 10))
            client.send_multipart([(2).to_bytes(8), req])
            assert client.poll(10_000), "the slot was never given back"
            reply = client.recv_multipart()
            assert reply[1] == OK and SmartPickle.loads(reply[2]) == "pong"

            # The kill really was attempted, and — having refused — the
            # invocation really did run: the slot came back through the
            # rollback, not because the call never happened.
            assert kills, "the rollback never tried the kill"
            gevent.sleep(0.2)
            assert marker.exists(), "the surviving invocation never ran"
        finally:
            client.close(linger=0)
            ctx.term()
            worker.kill(block=True, timeout=5)
            with contextlib.suppress(OSError):
                os.unlink(addr.removeprefix("ipc://"))


class TestAdmissionRefused:
    def test_a_refused_handler_spawn_answers_instead_of_ending_the_worker(
        self, monkeypatch
    ):
        """handlers.spawn(handle, ...) in the request loop had no guard: a
        spawn the hub refuses unwound the worker and closed its transport —
        under serve() a host every attached process shares."""
        import tempfile
        import uuid

        import dill
        import gevent
        import gevent.pool
        import zmq
        import zmq.green

        from gisolate._workers import WorkerConfig, gevent_worker

        from .helpers import adder_factory

        addr = f"ipc://{tempfile.gettempdir()}/gi-adm-{uuid.uuid4().hex[:8]}.sock"
        worker = gevent.spawn(
            gevent_worker,
            WorkerConfig(ipc_addr=addr, factory_bytes=dill.dumps(adder_factory)),
            {},
        )
        real_spawn = gevent.pool.Group.spawn
        refuse = [True]

        def maybe_refuse(group, fn, *args, **kwargs):
            if refuse[0] and getattr(fn, "__name__", "") == "handle":
                refuse[0] = False
                raise MemoryError("the hub refused a greenlet")
            return real_spawn(group, fn, *args, **kwargs)

        ctx = zmq.green.Context()
        client = ctx.socket(zmq.DEALER)
        try:
            for _ in range(200):
                if os.path.exists(addr.removeprefix("ipc://")):
                    break
                gevent.sleep(0.05)
            client.connect(addr)
            monkeypatch.setattr(gevent.pool.Group, "spawn", maybe_refuse)
            req = SmartPickle.dumps(("add", (1, 1), {}, time.monotonic() + 10))
            client.send_multipart([(1).to_bytes(8), req])
            assert client.poll(10_000), "the refused request went unanswered"
            assert client.recv_multipart()[1] == ERR
            monkeypatch.undo()

            # The worker survived to serve the next request.
            req = SmartPickle.dumps(("add", (2, 3), {}, time.monotonic() + 10))
            client.send_multipart([(2).to_bytes(8), req])
            assert client.poll(10_000), "the worker died with the refused spawn"
            reply = client.recv_multipart()
            assert reply[1] == OK and SmartPickle.loads(reply[2]) == 5
        finally:
            client.close(linger=0)
            ctx.term()
            worker.kill(block=True, timeout=5)
            with contextlib.suppress(OSError):
                os.unlink(addr.removeprefix("ipc://"))

    def test_a_refused_task_answers_instead_of_ending_the_asyncio_worker(
        self, monkeypatch
    ):
        """asyncio.create_task in the request loop had the same gap: an
        installed task factory refusing a handler unwound asyncio.run and the
        worker with it."""
        import asyncio
        import tempfile
        import uuid

        import dill
        import gevent
        import gevent.monkey
        import zmq
        import zmq.green

        from gisolate._workers import WorkerConfig, asyncio_worker

        from .helpers import adder_factory

        Thread = gevent.monkey.get_original("threading", "Thread")
        addr = f"ipc://{tempfile.gettempdir()}/gi-atask-{uuid.uuid4().hex[:8]}.sock"
        worker = Thread(
            target=asyncio_worker,
            args=(
                WorkerConfig(ipc_addr=addr, factory_bytes=dill.dumps(adder_factory)),
            ),
            daemon=True,
        )
        worker.start()

        real_create = asyncio.create_task
        refuse = [True]

        def maybe_refuse(coro, **kwargs):
            if refuse[0] and getattr(coro, "__name__", "") == "handle":
                refuse[0] = False
                raise MemoryError("the task factory refused")
            return real_create(coro, **kwargs)

        ctx = zmq.green.Context()
        client = ctx.socket(zmq.DEALER)
        try:
            for _ in range(200):
                if os.path.exists(addr.removeprefix("ipc://")):
                    break
                gevent.sleep(0.05)
            client.connect(addr)
            monkeypatch.setattr(asyncio, "create_task", maybe_refuse)
            req = SmartPickle.dumps(("add", (1, 1), {}, time.monotonic() + 10))
            client.send_multipart([(1).to_bytes(8), req])
            assert client.poll(10_000), "the refused request went unanswered"
            assert client.recv_multipart()[1] == ERR
            monkeypatch.undo()

            req = SmartPickle.dumps(("add", (2, 3), {}, time.monotonic() + 10))
            client.send_multipart([(2).to_bytes(8), req])
            assert client.poll(10_000), "the worker died with the refused task"
            reply = client.recv_multipart()
            assert reply[1] == OK and SmartPickle.loads(reply[2]) == 5

            client.send_multipart([b"0", SHUTDOWN])
            deadline = time.monotonic() + 20
            while worker.is_alive() and time.monotonic() < deadline:
                gevent.sleep(0.1)
            assert not worker.is_alive()
        finally:
            client.close(linger=0)
            ctx.term()
            with contextlib.suppress(OSError):
                os.unlink(addr.removeprefix("ipc://"))
