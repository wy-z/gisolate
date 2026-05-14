"""Tests for gisolate.pubsub (ProcessPublisher / ProcessSubscriber)."""

import asyncio
import json
import tempfile
import threading
import time
import uuid
from typing import Any

import gevent
import pytest

from gisolate.pubsub import ProcessPublisher, ProcessSubscriber


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

    def test_start_failure_releases_resources(self):
        """If bind fails, ctx/sock must be cleaned up so close() is harmless."""
        # Bogus transport — bind must raise.
        pub = ProcessPublisher("not-a-valid-transport://x")
        with pytest.raises(Exception):
            pub.start()
        assert pub._sock is None
        assert pub._ctx is None
        assert not pub._started
        pub.close()  # idempotent, no-op

    def test_publish_no_subscribers_does_not_block(self):
        """Without a SUB connected, NOBLOCK publish must return immediately."""
        pub = ProcessPublisher(_make_addr()).start()
        try:
            t0 = time.monotonic()
            for i in range(2000):
                pub.publish("v1.fast", {"i": i})
            elapsed = time.monotonic() - t0
            assert elapsed < 2.0  # generous bound; we mostly care it didn't hang
        finally:
            pub.close()

    def test_context_manager(self):
        addr = _make_addr()
        with ProcessPublisher(addr) as pub:
            assert pub.address == addr
            pub.publish("x", 1)

    def test_custom_serializer(self):
        pub = ProcessPublisher(_make_addr(), serializer=JSONSerializer).start()
        try:
            pub.publish("t", {"a": 1})
        finally:
            pub.close()

    def test_concurrent_publish_and_close(self):
        """publish() running concurrently with close() must not crash on a freed socket.

        Regression: close() tore the socket down without taking _send_lock,
        leaving in-flight publish() greenlets racing against a freed sock.

        After close, publish() legitimately raises RuntimeError (publisher
        not started); that's expected and explicitly tolerated here. The
        bug we're guarding against is a ZMQ-level crash from a torn-down
        socket during the in-flight send.
        """
        pub = ProcessPublisher(_make_addr()).start()
        errors: list[BaseException] = []

        def producer():
            for _ in range(500):
                try:
                    pub.publish("v1.race", {"i": 0})
                except RuntimeError:
                    return  # publisher closed after us — expected
                except BaseException as e:  # noqa: BLE001
                    errors.append(e)
                    return
                gevent.sleep(0)

        greenlets = [gevent.spawn(producer) for _ in range(4)]
        gevent.sleep(0.05)  # let producers start
        pub.close()
        gevent.joinall(greenlets, timeout=3)
        assert errors == [], f"publish raised under concurrent close: {errors!r}"


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

    def test_start_without_running_loop_does_not_leak(self):
        """Calling start() outside a running loop must not half-build the subscriber."""
        sub = ProcessSubscriber(_make_addr())
        with pytest.raises(RuntimeError):
            sub.start()
        assert sub._sock is None
        assert sub._ctx is None
        assert sub._reader_task is None
        assert not sub._started


# ---------------------------------------------------------------------------
# Integration: gevent PUB <-> asyncio SUB across threads
# ---------------------------------------------------------------------------


def _run_subscriber_in_thread(
    addr: str,
    prefixes_and_buckets: dict[str, list[tuple[str, Any]]],
    ready_evt: threading.Event,
    stop_evt: threading.Event,
    serializer=None,
) -> threading.Thread:
    """Run a SUB in its own thread with its own asyncio loop."""

    def runner() -> None:
        async def main() -> None:
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

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    return t


def _wait_until(predicate, timeout: float = 5.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        gevent.sleep(interval)
    return predicate()


class TestPubSubIntegration:
    def test_roundtrip_default_serializer(self):
        addr = _make_addr()
        bucket: list[tuple[str, Any]] = []
        ready = threading.Event()
        stop = threading.Event()
        thread = _run_subscriber_in_thread(addr, {"v1.snap.": bucket}, ready, stop)
        try:
            assert ready.wait(2.0)
            pub = ProcessPublisher(addr).start()
            try:
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
            finally:
                pub.close()
        finally:
            stop.set()
            thread.join(timeout=3)

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
            pub = ProcessPublisher(addr).start()
            try:
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
                pub.close()
        finally:
            stop.set()
            thread.join(timeout=3)

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
            pub = ProcessPublisher(addr).start()
            try:
                gevent.sleep(0.2)
                for i in range(3):
                    pub.publish("v1.x.k", {"i": i})
                assert _wait_until(lambda: len(good) >= 3)
                assert good == [("v1.x.k", {"i": i}) for i in range(3)]
            finally:
                pub.close()
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
            pub = ProcessPublisher(addr).start()
            try:
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
                pub.close()
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
                    captured["after_close_sock"] = sub._sock
                    captured["after_close_ctx"] = sub._ctx
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
            pub = ProcessPublisher(addr).start()
            try:
                gevent.sleep(0.2)
                pub.publish("v1.x.k", {"i": 1})
                assert _wait_until(lambda: captured.get("entered", False), timeout=3.0)
                # Give close() a moment to finish cleanup.
                gevent.sleep(0.3)
                assert captured["after_close_sock"] is None
                assert captured["after_close_ctx"] is None
                assert captured["after_close_started"] is False
            finally:
                pub.close()
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
            pub = ProcessPublisher(addr).start()
            try:
                gevent.sleep(0.2)
                pub.publish("v1.x.k", {"i": 1})
                # Sibling must complete, not get torn down by close()'s cancel.
                assert _wait_until(sibling_finished.is_set, timeout=3.0)
            finally:
                pub.close()
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
            pub = ProcessPublisher(addr).start()
            try:
                gevent.sleep(0.2)
                for i in range(3):
                    pub.publish("v1.x.k", {"i": i})
                assert _wait_until(lambda: len(good) >= 3, timeout=3.0)
                assert good == [("v1.x.k", {"i": i}) for i in range(3)]
            finally:
                pub.close()
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
            pub = ProcessPublisher(addr).start()
            try:
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
                pub.close()
        finally:
            stop.set()
            thread.join(timeout=3)

    def test_handler_close_then_restart_no_stale_reader(self):
        """close+start from inside a handler must not leave a stale reader live.

        Regression: ``_read_loop`` used to read ``self._sock`` dynamically, so
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
                        old_task = sub._reader_task
                        await sub.close()
                        sub.start()
                        reader_tasks_snapshot["old"] = old_task
                        reader_tasks_snapshot["new"] = sub._reader_task
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
            pub = ProcessPublisher(addr).start()
            try:
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
                pub.close()
        finally:
            stop.set()
            thread.join(timeout=3)

    def test_custom_serializer_end_to_end(self):
        addr = _make_addr()
        bucket: list[tuple[str, Any]] = []
        ready = threading.Event()
        stop = threading.Event()
        thread = _run_subscriber_in_thread(
            addr,
            {"v1.json.": bucket},
            ready,
            stop,
            serializer=JSONSerializer,
        )
        try:
            assert ready.wait(2.0)
            pub = ProcessPublisher(addr, serializer=JSONSerializer).start()
            try:
                gevent.sleep(0.2)
                pub.publish("v1.json.x", {"a": 1, "b": [1, 2, 3]})
                assert _wait_until(lambda: len(bucket) >= 1)
                assert bucket == [("v1.json.x", {"a": 1, "b": [1, 2, 3]})]
            finally:
                pub.close()
        finally:
            stop.set()
            thread.join(timeout=3)
