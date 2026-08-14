"""Tests for gisolate.pubsub (ProcessPublisher / ProcessSubscriber)."""

import asyncio
import contextlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import uuid
from typing import Any

import gevent
import pytest

from gisolate.pubsub import ProcessPublisher, ProcessSubscriber, Runtime


def _make_addr() -> str:
    return f"ipc://{tempfile.gettempdir()}/gisolate-pubsub-{uuid.uuid4().hex}.sock"


class JSONSerializer:
    """Trivial alternate serializer used to verify pluggability."""

    @staticmethod
    def dumps(obj: Any) -> bytes:
        return json.dumps(obj).encode("utf-8")

    @staticmethod
    def loads(data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))


# ---------------------------------------------------------------------------
# Publisher unit tests
# ---------------------------------------------------------------------------


class TestProcessPublisher:
    def test_start_returns_self(self):
        pub = ProcessPublisher(_make_addr())
        assert pub.start() is pub
        pub.close()

    def test_start_idempotent(self):
        pub = ProcessPublisher(_make_addr())
        pub.start()
        pub.start()  # no-op
        pub.close()

    def test_close_idempotent(self):
        pub = ProcessPublisher(_make_addr())
        pub.close()  # never started
        pub.start()
        pub.close()
        pub.close()  # double close

    def test_publish_before_start_raises(self):
        pub = ProcessPublisher(_make_addr())
        with pytest.raises(RuntimeError, match="before start"):
            pub.publish("t", {"x": 1})

    def test_a_start_that_fails_after_binding_leaves_nothing_behind(
        self, monkeypatch
    ):
        """Everything fallible has to come before the bind, or be undone. The
        lock came after it, so a MemoryError there left a bound PUB socket
        reachable only through the traceback — _transport was still None, so
        neither close() nor __del__ could release it, and the socket file
        stayed."""
        import gevent.lock

        def no_room(*_args, **_kwargs):
            raise MemoryError("no room for a lock")

        addr = _make_addr()
        monkeypatch.setattr(gevent.lock, "Semaphore", no_room)
        pub = ProcessPublisher(addr)
        with pytest.raises(MemoryError):
            pub.start()
        assert pub._transport is None
        assert not os.path.exists(addr[6:])

    def test_the_async_start_leaves_nothing_behind_either(self, monkeypatch):
        """Same ordering, same consequence, in the runtime whose lock is
        asyncio's."""

        def no_room(*_args, **_kwargs):
            raise MemoryError("no room for a lock")

        addr = _make_addr()
        monkeypatch.setattr(asyncio, "Lock", no_room)

        async def go():
            pub = ProcessPublisher(addr, runtime=Runtime.ASYNC)
            with pytest.raises(MemoryError):
                pub.start()
            assert pub._transport is None

        asyncio.run(go())
        assert not os.path.exists(addr[6:])

    def test_context_manager(self):
        addr = _make_addr()
        with ProcessPublisher(addr) as pub:
            assert pub.address == addr
            pub.publish("x", 1)


# ---------------------------------------------------------------------------
# Subscriber unit tests (no publisher — start/close/register lifecycle only)
# ---------------------------------------------------------------------------


class TestProcessSubscriberLifecycle:
    def test_subscribe_before_start(self):
        sub = ProcessSubscriber(_make_addr())

        async def handler(_topic, _payload):  # pragma: no cover - never invoked
            pass

        sub.subscribe("a.", handler)
        sub.subscribe("a.", handler)  # second handler same prefix
        sub.subscribe("b.", handler)
        # No exception; internal state populated.
        assert "a." in sub._handlers
        assert "b." in sub._handlers
        assert len(sub._handlers["a."]) == 2

    def test_unsubscribe_specific_handler(self):
        sub = ProcessSubscriber(_make_addr())

        async def h1(_topic, _payload):  # pragma: no cover
            pass

        async def h2(_topic, _payload):  # pragma: no cover
            pass

        sub.subscribe("a.", h1)
        sub.subscribe("a.", h2)
        sub.unsubscribe("a.", h1)
        assert sub._handlers["a."] == [h2]
        sub.unsubscribe("a.")  # remove all
        assert "a." not in sub._handlers

    def test_close_idempotent_before_start(self):
        sub = ProcessSubscriber(_make_addr())
        asyncio.run(sub.close())  # never started
        asyncio.run(sub.close())

    def test_gevent_close_outer_timeout_not_swallowed(self):
        """A caller's enclosing gevent.Timeout firing while close() waits on
        the reader must propagate as their cancellation, not be suppressed."""
        sub = ProcessSubscriber(_make_addr(), runtime=Runtime.GEVENT)
        sub.subscribe("x.", lambda _t, _p: None)
        sub.start()
        sub._reader.join = lambda *_a, **_k: gevent.sleep(5)  # wedge the join
        timer = gevent.Timeout.start_new(0.3)
        try:
            with pytest.raises(gevent.Timeout) as excinfo:
                sub.close()
            assert excinfo.value is timer
        finally:
            timer.close()

    def test_async_close_cancellation_not_swallowed(self):
        """Cancelling the task that runs close() while it waits on the reader
        must actually cancel it — CancelledError must not be suppressed."""
        addr = _make_addr()

        async def main() -> None:
            sub = ProcessSubscriber(addr)
            sub.subscribe("x.", lambda _t, _p: None)
            sub.start()
            wedge = asyncio.create_task(asyncio.sleep(30))
            sub._reader = wedge  # close() will wait on this instead
            closer = asyncio.create_task(sub.close())
            await asyncio.sleep(0.1)
            closer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await closer
            wedge.cancel()

        asyncio.run(main())

    def test_start_without_running_loop_does_not_leak(self):
        """Calling start() outside a running loop must not half-build the subscriber."""
        sub = ProcessSubscriber(_make_addr())
        with pytest.raises(RuntimeError):
            sub.start()
        assert sub._transport is None
        assert sub._reader is None
        assert not sub._started


# ---------------------------------------------------------------------------
# Integration: gevent PUB <-> asyncio SUB across threads
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _subscriber_thread(
    addr: str,
    prefixes_and_buckets: dict[str, list[tuple[str, Any]]],
    serializer=None,
    task_factory=None,
):
    """Run a SUB in its own thread with its own asyncio loop for the block.

    Yields only once the subscriber has started, and stops + joins the thread
    on exit — including when the readiness wait itself fails.
    """
    ready_evt, stop_evt = threading.Event(), threading.Event()

    def runner() -> None:
        async def main() -> None:
            if task_factory is not None:
                asyncio.get_running_loop().set_task_factory(task_factory)
            kwargs = {"serializer": serializer} if serializer else {}
            sub = ProcessSubscriber(addr, **kwargs)
            for prefix, bucket in prefixes_and_buckets.items():

                def make_handler(b):
                    async def handler(topic, payload):
                        b.append((topic, payload))

                    return handler

                sub.subscribe(prefix, make_handler(bucket))
            sub.start()
            ready_evt.set()
            try:
                while not stop_evt.is_set():
                    await asyncio.sleep(0.05)
            finally:
                await sub.close()

        asyncio.run(main())

    t = _RealThread(target=runner, daemon=True)
    t.start()
    try:
        assert ready_evt.wait(2.0)
        yield
    finally:
        stop_evt.set()
        t.join(timeout=3)


def _wait_until(predicate, timeout: float = 5.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        gevent.sleep(interval)
    return predicate()


class TestPubSubIntegration:
    def test_close_inside_eager_start_not_resurrected(self):
        """asyncio.eager_task_factory runs the reader inside create_task; a
        close() executing there (e.g. from a handler) must not be undone by
        start()'s remaining code — the subscriber stays closed."""
        addr = _make_addr()

        async def main() -> None:
            loop = asyncio.get_running_loop()
            loop.set_task_factory(asyncio.eager_task_factory)
            sub = ProcessSubscriber(addr)
            real_read_loop = sub._read_loop_async

            async def hijacked(sock):
                await sub.close()  # handler-close during eager start
                await real_read_loop(sock)

            sub._read_loop_async = hijacked  # type: ignore[method-assign]
            sub.start()
            assert sub._started is False
            assert sub._transport is None

        asyncio.run(main())

    def test_roundtrip_default_serializer(self):
        addr = _make_addr()
        bucket: list[tuple[str, Any]] = []
        with _subscriber_thread(addr, {"v1.snap.": bucket}):
            with ProcessPublisher(addr) as pub:
                # Give SUB a moment to actually establish its connection so
                # the very first message is not lost (PUB drops without peer).
                gevent.sleep(0.2)
                for i in range(5):
                    pub.publish("v1.snap.AAPL", {"i": i})
                assert _wait_until(lambda: len(bucket) >= 5, timeout=3.0)
                topics = [t for t, _ in bucket]
                payloads = [p for _, p in bucket]
                assert all(t == "v1.snap.AAPL" for t in topics)
                assert payloads == [{"i": i} for i in range(5)]

    def test_prefix_routing_and_multiple_handlers(self):
        addr = _make_addr()
        snap: list[tuple[str, Any]] = []
        snap2: list[tuple[str, Any]] = []
        hb: list[tuple[str, Any]] = []
        ready = threading.Event()
        stop = threading.Event()

        # Subscribe two handlers under v1.snap. plus one for v1.heartbeat.
        def runner() -> None:
            async def main() -> None:
                sub = ProcessSubscriber(addr)

                async def on_snap(topic, payload):
                    snap.append((topic, payload))

                async def on_snap2(topic, payload):
                    snap2.append((topic, payload))

                async def on_hb(topic, payload):
                    hb.append((topic, payload))

                sub.subscribe("v1.snap.", on_snap)
                sub.subscribe("v1.snap.", on_snap2)
                sub.subscribe("v1.heartbeat.", on_hb)
                sub.start()
                ready.set()
                try:
                    while not stop.is_set():
                        await asyncio.sleep(0.05)
                finally:
                    await sub.close()

            asyncio.run(main())

        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
        try:
            assert ready.wait(2.0)
            with ProcessPublisher(addr) as pub:
                gevent.sleep(0.2)
                pub.publish("v1.snap.AAPL", {"price": 1})
                pub.publish("v1.heartbeat.gevent", {"ts": 123})
                pub.publish("v1.unmatched.X", {"drop": True})
                assert _wait_until(
                    lambda: len(snap) >= 1 and len(snap2) >= 1 and len(hb) >= 1
                )
                assert snap == [("v1.snap.AAPL", {"price": 1})]
                assert snap2 == [("v1.snap.AAPL", {"price": 1})]
                assert hb == [("v1.heartbeat.gevent", {"ts": 123})]
                # Verify unmatched topic was NOT delivered to any handler.
                gevent.sleep(0.1)
                assert all("unmatched" not in t for t, _ in snap + snap2 + hb)
        finally:
            stop.set()
            thread.join(timeout=3)

    def test_async_subscriber_with_eager_task_factory(self):
        """Regression: with asyncio.eager_task_factory (3.12+) create_task runs
        the reader synchronously inside start(); it must not exit against a
        not-yet-flagged subscriber and silently drop all messages."""
        addr = _make_addr()
        got: list[tuple[str, Any]] = []
        with _subscriber_thread(
            addr, {"v1.": got}, task_factory=asyncio.eager_task_factory
        ):
            with ProcessPublisher(addr) as pub:
                gevent.sleep(0.2)
                pub.publish("v1.snap", {"n": 1})
                assert _wait_until(lambda: len(got) >= 1)
                assert got == [("v1.snap", {"n": 1})]

    def test_handler_exception_does_not_kill_reader(self):
        addr = _make_addr()
        good: list[tuple[str, Any]] = []
        ready = threading.Event()
        stop = threading.Event()

        def runner() -> None:
            async def main() -> None:
                sub = ProcessSubscriber(addr)

                async def bad(_topic, _payload):
                    raise RuntimeError("boom")

                async def fine(topic, payload):
                    good.append((topic, payload))

                sub.subscribe("v1.x.", bad)
                sub.subscribe("v1.x.", fine)
                sub.start()
                ready.set()
                try:
                    while not stop.is_set():
                        await asyncio.sleep(0.05)
                finally:
                    await sub.close()

            asyncio.run(main())

        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
        try:
            assert ready.wait(2.0)
            with ProcessPublisher(addr) as pub:
                gevent.sleep(0.2)
                for i in range(3):
                    pub.publish("v1.x.k", {"i": i})
                assert _wait_until(lambda: len(good) >= 3)
                assert good == [("v1.x.k", {"i": i}) for i in range(3)]
        finally:
            stop.set()
            thread.join(timeout=3)

    def test_unsubscribe_stops_delivery(self):
        addr = _make_addr()
        bucket: list[tuple[str, Any]] = []
        ready = threading.Event()
        stop = threading.Event()
        unsubscribed = threading.Event()
        do_unsubscribe = threading.Event()

        def runner() -> None:
            async def main() -> None:
                sub = ProcessSubscriber(addr)

                async def handler(topic, payload):
                    bucket.append((topic, payload))

                sub.subscribe("v1.x.", handler)
                sub.start()
                ready.set()
                try:
                    while not stop.is_set():
                        if do_unsubscribe.is_set() and not unsubscribed.is_set():
                            sub.unsubscribe("v1.x.", handler)
                            unsubscribed.set()
                        await asyncio.sleep(0.02)
                finally:
                    await sub.close()

            asyncio.run(main())

        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
        try:
            assert ready.wait(2.0)
            with ProcessPublisher(addr) as pub:
                gevent.sleep(0.2)
                pub.publish("v1.x.k", {"i": 0})
                assert _wait_until(lambda: len(bucket) >= 1)
                pre_count = len(bucket)
                do_unsubscribe.set()
                assert _wait_until(unsubscribed.is_set, timeout=2.0)
                # Allow ZMQ to actually drop subscription.
                gevent.sleep(0.2)
                for i in range(5):
                    pub.publish("v1.x.k", {"i": i + 1})
                gevent.sleep(0.3)
                assert len(bucket) == pre_count
        finally:
            stop.set()
            thread.join(timeout=3)

    def test_handler_invoked_close_cleans_up(self):
        """Calling `await sub.close()` from inside a handler must not leak sock/ctx."""
        addr = _make_addr()
        ready = threading.Event()
        stop = threading.Event()
        captured: dict[str, Any] = {}

        def runner() -> None:
            async def main() -> None:
                sub = ProcessSubscriber(addr)

                async def handler(_topic, _payload):
                    captured["entered"] = True
                    await sub.close()
                    captured["after_close_transport"] = sub._transport
                    captured["after_close_started"] = sub._started

                sub.subscribe("v1.x.", handler)
                sub.start()
                ready.set()
                try:
                    while not stop.is_set():
                        await asyncio.sleep(0.05)
                finally:
                    await sub.close()  # second close: idempotent

            asyncio.run(main())

        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
        try:
            assert ready.wait(2.0)
            with ProcessPublisher(addr) as pub:
                gevent.sleep(0.2)

                def offered():
                    # PUB drops everything until the subscription has
                    # propagated, so keep offering rather than betting the test
                    # on one message landing after a fixed sleep.
                    pub.publish("v1.x.k", {"i": 1})
                    return captured.get("entered", False)

                assert _wait_until(offered, timeout=5.0)
                # Give close() a moment to finish cleanup.
                gevent.sleep(0.3)
                assert captured["after_close_transport"] is None
                assert captured["after_close_started"] is False
        finally:
            stop.set()
            thread.join(timeout=3)

    def test_handler_invoked_close_does_not_cancel_siblings(self):
        """`await sub.close()` from one handler must let sibling handlers finish.

        Regression: previously close() cancelled the reader task, which
        propagated through asyncio.gather and tore down still-running
        siblings mid-execution.
        """
        addr = _make_addr()
        ready = threading.Event()
        stop = threading.Event()
        sibling_finished = threading.Event()

        def runner() -> None:
            async def main() -> None:
                sub = ProcessSubscriber(addr)

                async def closing_handler(_topic, _payload):
                    await sub.close()

                async def slow_sibling(_topic, _payload):
                    # Yield several times so closing_handler has plenty of
                    # opportunity to call close() and (if buggy) cancel us.
                    for _ in range(5):
                        await asyncio.sleep(0.02)
                    sibling_finished.set()

                sub.subscribe("v1.x.", closing_handler)
                sub.subscribe("v1.x.", slow_sibling)
                sub.start()
                ready.set()
                try:
                    while not stop.is_set():
                        await asyncio.sleep(0.05)
                finally:
                    await sub.close()

            asyncio.run(main())

        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
        try:
            assert ready.wait(2.0)
            with ProcessPublisher(addr) as pub:
                gevent.sleep(0.2)
                pub.publish("v1.x.k", {"i": 1})
                # Sibling must complete, not get torn down by close()'s cancel.
                assert _wait_until(sibling_finished.is_set, timeout=3.0)
        finally:
            stop.set()
            thread.join(timeout=3)

    def test_handler_cancellederror_does_not_kill_reader(self):
        """A handler raising CancelledError must not kill the reader task."""
        addr = _make_addr()
        good: list[tuple[str, Any]] = []
        ready = threading.Event()
        stop = threading.Event()

        def runner() -> None:
            async def main() -> None:
                sub = ProcessSubscriber(addr)

                async def bad(_topic, _payload):
                    raise asyncio.CancelledError("simulated")

                async def fine(topic, payload):
                    good.append((topic, payload))

                sub.subscribe("v1.x.", bad)
                sub.subscribe("v1.x.", fine)
                sub.start()
                ready.set()
                try:
                    while not stop.is_set():
                        await asyncio.sleep(0.05)
                finally:
                    await sub.close()

            asyncio.run(main())

        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
        try:
            assert ready.wait(2.0)
            with ProcessPublisher(addr) as pub:
                gevent.sleep(0.2)
                for i in range(3):
                    pub.publish("v1.x.k", {"i": i})
                assert _wait_until(lambda: len(good) >= 3, timeout=3.0)
                assert good == [("v1.x.k", {"i": i}) for i in range(3)]
        finally:
            stop.set()
            thread.join(timeout=3)

    def test_subscriber_close_then_restart(self):
        """After close+start, subscriber gets a fresh context and resumes delivery."""
        addr = _make_addr()
        bucket: list[tuple[str, Any]] = []
        ready = threading.Event()
        stop = threading.Event()
        restart_done = threading.Event()
        do_restart = threading.Event()

        def runner() -> None:
            async def main() -> None:
                sub = ProcessSubscriber(addr)

                async def handler(topic, payload):
                    bucket.append((topic, payload))

                sub.subscribe("v1.x.", handler)
                sub.start()
                ready.set()
                try:
                    while not stop.is_set():
                        if do_restart.is_set() and not restart_done.is_set():
                            await sub.close()
                            sub.start()  # re-uses retained handlers
                            restart_done.set()
                        await asyncio.sleep(0.02)
                finally:
                    await sub.close()

            asyncio.run(main())

        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
        try:
            assert ready.wait(2.0)
            with ProcessPublisher(addr) as pub:
                gevent.sleep(0.2)
                pub.publish("v1.x.k", {"i": 1})
                assert _wait_until(lambda: len(bucket) >= 1)
                do_restart.set()
                assert _wait_until(restart_done.is_set, timeout=2.0)
                gevent.sleep(0.3)  # let SUB reconnect
                pub.publish("v1.x.k", {"i": 2})
                assert _wait_until(lambda: len(bucket) >= 2, timeout=3.0)
                assert bucket[0] == ("v1.x.k", {"i": 1})
                assert bucket[1] == ("v1.x.k", {"i": 2})
        finally:
            stop.set()
            thread.join(timeout=3)

    def test_handler_close_then_restart_no_stale_reader(self):
        """close+start from inside a handler must not leave a stale reader live.

        Regression: ``_read_loop`` used to read ``self._transport`` dynamically, so
        after a handler did ``await sub.close(); sub.start()`` the original
        reader resumed against the *new* socket, racing the new reader's
        recv. The reader is now bound to its start-time socket and exits on
        the first failed recv from the closed old socket.
        """
        addr = _make_addr()
        bucket: list[tuple[str, Any]] = []
        ready = threading.Event()
        stop = threading.Event()
        restarted = threading.Event()
        reader_tasks_snapshot: dict[str, Any] = {}

        def runner() -> None:
            async def main() -> None:
                sub = ProcessSubscriber(addr)

                async def handler(topic, payload):
                    bucket.append((topic, payload))
                    if payload == {"i": 0} and not restarted.is_set():
                        old_task = sub._reader
                        await sub.close()
                        sub.start()
                        reader_tasks_snapshot["old"] = old_task
                        reader_tasks_snapshot["new"] = sub._reader
                        restarted.set()

                sub.subscribe("v1.x.", handler)
                sub.start()
                ready.set()
                try:
                    while not stop.is_set():
                        await asyncio.sleep(0.02)
                finally:
                    await sub.close()

            asyncio.run(main())

        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
        try:
            assert ready.wait(2.0)
            with ProcessPublisher(addr) as pub:
                gevent.sleep(0.2)
                pub.publish("v1.x.k", {"i": 0})
                assert _wait_until(restarted.is_set, timeout=3.0)
                # Give the old reader time to wake and (correctly) exit.
                gevent.sleep(0.3)
                old = reader_tasks_snapshot["old"]
                new = reader_tasks_snapshot["new"]
                assert old is not new
                assert old.done(), "stale reader from pre-restart still alive"
                # New reader still receives.
                pub.publish("v1.x.k", {"i": 1})
                assert _wait_until(lambda: len(bucket) >= 2, timeout=3.0)
                assert bucket[1] == ("v1.x.k", {"i": 1})
        finally:
            stop.set()
            thread.join(timeout=3)

    def test_custom_serializer_end_to_end(self):
        addr = _make_addr()
        bucket: list[tuple[str, Any]] = []
        with _subscriber_thread(addr, {"v1.json.": bucket}, serializer=JSONSerializer):
            with ProcessPublisher(addr, serializer=JSONSerializer) as pub:
                gevent.sleep(0.2)
                pub.publish("v1.json.x", {"a": 1, "b": [1, 2, 3]})
                assert _wait_until(lambda: len(bucket) >= 1)
                assert bucket == [("v1.json.x", {"a": 1, "b": [1, 2, 3]})]


# ---------------------------------------------------------------------------
# Runtime matrix: every (pub_runtime, sub_runtime) combination
# ---------------------------------------------------------------------------

# conftest.py runs ``gevent.monkey.patch_all()``. Under it, two
# ``asyncio.run()`` calls in different OS threads collide on asyncio's
# running-loop TLS (Python 3.14 + gevent monkey-patching). For cross-runtime
# cases below we therefore put the gevent side in a *real* OS thread and run
# the asyncio side on the main thread — only one ``asyncio.run`` ever lives.
import gevent.monkey  # noqa: E402

_RealThread = gevent.monkey.get_original("threading", "Thread")


def _gevent_sub_in_thread(addr, prefix, bucket, ready, stop):
    """Run a GEVENT subscriber in a real OS thread (own gevent hub)."""

    def runner():
        sub = ProcessSubscriber(addr, runtime=Runtime.GEVENT)

        def handler(topic, payload):
            bucket.append((topic, payload))

        sub.subscribe(prefix, handler)
        sub.start()
        ready.set()
        try:
            while not stop.is_set():
                gevent.sleep(0.05)
        finally:
            sub.close()

    t = _RealThread(target=runner, daemon=True)
    t.start()
    return t


def _async_sub_in_thread(addr, prefix, bucket, ready, stop):
    """Run an ASYNC subscriber in a real OS thread (own asyncio loop)."""

    def runner():
        async def main():
            sub = ProcessSubscriber(addr, runtime=Runtime.ASYNC)

            async def handler(topic, payload):
                bucket.append((topic, payload))

            sub.subscribe(prefix, handler)
            sub.start()
            ready.set()
            try:
                while not stop.is_set():
                    await asyncio.sleep(0.05)
            finally:
                await sub.close()

        asyncio.run(main())

    t = _RealThread(target=runner, daemon=True)
    t.start()
    return t


class TestRuntimeMatrix:
    """Round-trip a message through every (pub_runtime, sub_runtime) pair."""

    def test_gevent_pub_async_sub(self):
        """The original supported combination."""
        addr = _make_addr()
        bucket: list[tuple[str, Any]] = []
        ready, stop = threading.Event(), threading.Event()

        sub_t = _async_sub_in_thread(addr, "v1.", bucket, ready, stop)
        try:
            assert ready.wait(2.0)
            with ProcessPublisher(addr, runtime=Runtime.GEVENT) as pub:
                for i in range(100):
                    pub.publish("v1.x", {"i": i})
                    gevent.sleep(0.05)
                    if bucket:
                        break
        finally:
            stop.set()
            sub_t.join(3)

        assert bucket, "no messages: gevent -> asyncio"
        assert bucket[0][0] == "v1.x"

    def test_async_pub_gevent_sub(self):
        """Asyncio publisher, gevent subscriber."""
        addr = _make_addr()
        bucket: list[tuple[str, Any]] = []
        ready, stop = threading.Event(), threading.Event()

        sub_t = _gevent_sub_in_thread(addr, "v1.", bucket, ready, stop)
        try:
            assert ready.wait(2.0)

            async def producer():
                async with ProcessPublisher(addr, runtime=Runtime.ASYNC) as pub:
                    for i in range(100):
                        await pub.publish("v1.x", {"i": i})
                        await asyncio.sleep(0.05)
                        if bucket:
                            break

            asyncio.run(producer())
        finally:
            stop.set()
            sub_t.join(3)

        assert bucket, "no messages: asyncio -> gevent"
        assert bucket[0][0] == "v1.x"

    def test_async_pub_async_sub(self):
        """Both ends on the same asyncio loop — no cross-thread coordination."""
        addr = _make_addr()
        bucket: list[tuple[str, Any]] = []

        async def main():
            sub = ProcessSubscriber(addr, runtime=Runtime.ASYNC)

            async def handler(topic, payload):
                bucket.append((topic, payload))

            sub.subscribe("v1.", handler)
            sub.start()
            pub = ProcessPublisher(addr, runtime=Runtime.ASYNC).start()
            try:
                for i in range(100):
                    await pub.publish("v1.x", {"i": i})
                    await asyncio.sleep(0.05)
                    if bucket:
                        break
            finally:
                await pub.close()
                await sub.close()

        asyncio.run(main())

        assert bucket, "no messages: asyncio -> asyncio"
        assert bucket[0][0] == "v1.x"

    def test_gevent_pub_gevent_sub(self):
        """Both ends on the same gevent hub — no cross-thread coordination."""
        addr = _make_addr()
        bucket: list[tuple[str, Any]] = []

        sub = ProcessSubscriber(addr, runtime=Runtime.GEVENT)

        def handler(topic, payload):
            bucket.append((topic, payload))

        sub.subscribe("v1.", handler)
        sub.start()
        try:
            with ProcessPublisher(addr, runtime=Runtime.GEVENT) as pub:
                for i in range(100):
                    pub.publish("v1.x", {"i": i})
                    gevent.sleep(0.05)
                    if bucket:
                        break
        finally:
            sub.close()

        assert bucket, "no messages: gevent -> gevent"
        assert bucket[0][0] == "v1.x"


class TestPublisherAddressOwnership:
    def test_closing_an_old_publisher_leaves_the_live_one_reachable(self):
        """libzmq unlinks an ipc path before binding it, so a restarted
        publisher silently takes the address over. The departing one must not
        remove that file: the survivor kept its current subscribers and went
        invisible to every new connect — a silent half-outage."""
        addr = _make_addr()
        old = ProcessPublisher(addr, runtime=Runtime.GEVENT).start()
        new = ProcessPublisher(addr, runtime=Runtime.GEVENT).start()
        received: list[Any] = []
        sub = ProcessSubscriber(addr, runtime=Runtime.GEVENT)
        sub.subscribe("t", lambda _topic, payload: received.append(payload))
        try:
            old.close()
            sub.start()  # a subscriber connecting AFTER the old one left
            gevent.sleep(0.3)
            new.publish("t", "payload")
            gevent.sleep(0.3)
            assert received == ["payload"]
        finally:
            sub.close()
            new.close()

    def test_a_lone_publisher_removes_its_own_socket(self):
        """The other half of the rule: with nobody having taken the address
        over, the file IS ours and must go — a service churning unique
        addresses would otherwise leave a socket inode behind per publisher."""
        addr = _make_addr()
        pub = ProcessPublisher(addr, runtime=Runtime.GEVENT).start()
        assert os.path.exists(addr.removeprefix("ipc://"))
        pub.close()
        assert not os.path.exists(addr.removeprefix("ipc://"))


class TestGeventReaderBackpressure:
    def test_the_reader_waits_for_its_handlers(self):
        """Draining ahead of the handlers defeats what SUB's receive queue and
        PUB's high-water mark are for — dropping at the publisher — and turns a
        handler slower than the stream into unbounded live greenlets, each
        holding its payload."""
        addr = _make_addr()
        active, peak = [], []

        def slow(_topic, payload):
            active.append(payload)
            peak.append(len(active))
            gevent.sleep(0.05)
            active.pop()

        sub = ProcessSubscriber(addr, runtime=Runtime.GEVENT)
        sub.subscribe("t.", slow)
        pub = ProcessPublisher(addr, runtime=Runtime.GEVENT).start()
        sub.start()
        try:
            gevent.sleep(0.3)  # slow joiner: let the subscription land
            for i in range(20):
                pub.publish("t.x", i)
            gevent.sleep(1.0)
            assert peak, "no message reached the handler"
            assert max(peak) == 1, f"{max(peak)} dispatches ran at once"
        finally:
            sub.close()
            pub.close()


class TestHandlerExitIsolation:
    def test_a_handler_that_exits_does_not_take_the_host_with_it(self):
        """SystemExit raised inside a handler is re-raised into the event loop
        by Task.__step, or forwarded by gevent to the main greenlet — either
        way ending the subscriber host, which is the opposite of what "an
        exception in one handler is logged but does not kill the reader"
        promises. An operator's Ctrl-C does not arrive here: it lands where the
        main thread is, not inside a handler."""

        def exiting(_topic, _payload):
            raise SystemExit(2)

        sub = ProcessSubscriber(_make_addr(), runtime=Runtime.GEVENT)
        worker = gevent.spawn(sub._invoke_gevent, exiting, "t.x", None)
        worker.join(timeout=5)
        assert worker.successful(), worker.exception

    def test_the_async_handler_boundary_holds_too(self):
        async def go():
            async def exiting(_topic, _payload):
                raise SystemExit(2)

            sub = ProcessSubscriber(_make_addr(), runtime=Runtime.ASYNC)
            task = asyncio.ensure_future(sub._invoke_async(exiting, "t.x", None))
            await asyncio.wait({task}, timeout=5)
            assert task.done() and task.exception() is None

        asyncio.run(go())

    def test_the_operators_interrupt_is_not_absorbed(self):
        """Measured, and the opposite of what round 33 assumed: a real SIGINT is
        raised in whatever greenlet is running on the main OS thread, so it
        lands inside the handler — not in the main greenlet. Caught as a client
        failure, Ctrl-C stops working on a subscriber host."""
        script = textwrap.dedent(
            """
            from gevent import monkey

            monkey.patch_all()

            import time

            import gevent

            from gisolate.pubsub import ProcessSubscriber, Runtime

            def busy(_topic, _payload):
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    for _ in range(50000):
                        pass
                    gevent.sleep(0)

            sub = ProcessSubscriber("ipc:///tmp/gi-never", runtime=Runtime.GEVENT)
            print("READY", flush=True)
            worker = gevent.spawn(sub._invoke_gevent, busy, "t.x", None)
            try:
                worker.join(timeout=15)
            except KeyboardInterrupt:
                print("INTERRUPTED", flush=True)
            """
        )
        proc = subprocess.Popen(
            [sys.executable, "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            assert proc.stdout is not None
            assert proc.stdout.readline().strip() == "READY"
            time.sleep(1.0)  # let the handler get going
            proc.send_signal(signal.SIGINT)
            out, _ = proc.communicate(timeout=20)
        finally:
            if proc.poll() is None:
                proc.kill()
        assert "INTERRUPTED" in out, out


class TestSubscriberSpawnRefused:
    def test_a_refused_spawn_leaves_the_subscriber_startable(self, monkeypatch):
        """_started and the transport are published before the reader exists, so
        a spawn that refuses left the subscriber claiming to be up with nothing
        reading — and start() no-ops on the retry. Its async twin already rolled
        this back."""
        real_spawn = gevent.spawn

        def refuse(fn, *args, **kwargs):
            if getattr(fn, "__name__", "") == "_read_loop_gevent":
                raise RuntimeError("the hub refused a greenlet")
            return real_spawn(fn, *args, **kwargs)

        monkeypatch.setattr(gevent, "spawn", refuse)
        sub = ProcessSubscriber(_make_addr(), runtime=Runtime.GEVENT)
        with pytest.raises(RuntimeError, match="refused"):
            sub.start()
        assert not sub._started and sub._transport is None

        monkeypatch.undo()
        sub.start()  # and the retry works
        sub.close()


class TestPublishGenerationBound:
    def test_a_parked_publish_does_not_send_on_the_next_generation(
        self, monkeypatch
    ):
        """A publish parked on generation A's send lock can resume after a
        close+start has published B — and re-reading ``self`` sent A's message
        on B's socket while holding A's lock, beside a publish legitimately
        holding B's, interleaving their multipart frames. Constructed the way
        the bridge's stale-generation test does: the post-race state is
        installed directly, because the wake ordering that produces it live is
        the hub's to choose."""
        import zmq
        import zmq.green

        from gisolate import _internal

        pub = ProcessPublisher(_make_addr()).start()
        sent = []
        real_send = zmq.green.Socket.send_multipart

        def recording(sock_self, frames, *a, **k):
            sent.append((sock_self, frames[0]))
            return real_send(sock_self, frames, *a, **k)

        monkeypatch.setattr(zmq.green.Socket, "send_multipart", recording)

        lock_a = pub._send_lock
        lock_a.acquire()
        stale = gevent.spawn(pub.publish, "stale", 1)
        gevent.sleep(0.05)  # parked on generation A's lock

        # What a completed close+start leaves behind while the publish waits:
        # a NEW started generation, and the old one gone.
        old_transport = pub._transport
        new_transport = _internal.ZmqTransport.open(
            zmq.green.Context, zmq.PUB, _make_addr(), bind=True
        )
        pub._transport = new_transport
        pub._send_lock = type(lock_a)()

        lock_a.release()  # the parked publish resumes, holding A's lock
        stale.join(timeout=2)
        monkeypatch.undo()

        try:
            assert not [1 for s_, t in sent if s_ is new_transport.sock], (
                "a publish accepted on the old generation sent on the new one"
            )
        finally:
            new_transport.close()
            pub._transport = old_transport
            pub._send_lock = lock_a
            pub.close()

    def test_a_stale_closer_leaves_the_restarted_generation_alone(self):
        """Two closers race a restart: the winner closes generation A and
        starts B without yielding — a PUB transport's close has no green op
        to switch on — while the loser, parked on A's lock behind a publisher
        mid-send, is scheduled but not yet run. The loser then wakes holding
        a lock nothing current uses, and used to re-read ``self`` and tear
        down the generation the restart had just published."""
        pub = ProcessPublisher(_make_addr()).start()
        lock_a = pub._send_lock
        lock_a.acquire()  # stand in for a publisher mid-send
        stale = gevent.spawn(pub.close)
        gevent.sleep(0.05)  # the loser passes its check and parks on A's lock
        lock_a.release()  # the sender finishes; the loser is scheduled, not run
        pub.close()  # the winner barges past the parked loser...
        pub.start()  # ...and restarts before the loser ever runs
        fresh = pub._transport
        stale.join(timeout=2)
        try:
            assert pub._started and pub._transport is fresh, (
                "the stale closer tore down the restarted generation"
            )
        finally:
            pub.close()
