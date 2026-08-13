"""Tests for gisolate.hub module."""

import subprocess
import sys
import textwrap

import gevent
import pytest
from gisolate.hub import AsyncResult, run_on_main_hub, spawn_on_main_hub


class TestAsyncResult:
    def test_set_and_get(self):
        ar = AsyncResult()
        ar.set(42)
        assert ar.get(timeout=1) == 42

    def test_set_exception_and_get(self):
        ar = AsyncResult()
        ar.set_exception(ValueError("boom"))
        with pytest.raises(ValueError, match="boom"):
            ar.get(timeout=1)

    def test_get_timeout(self):
        ar = AsyncResult()
        with pytest.raises(TimeoutError):
            ar.get(timeout=0.01)

    def test_set_none(self):
        ar = AsyncResult()
        ar.set(None)
        assert ar.get(timeout=1) is None


class TestMainHub:
    def test_run_on_main_hub_executes_func(self):
        """run_on_main_hub runs the function and waits (returns None on success)."""
        side_effect = []
        run_on_main_hub(lambda: side_effect.append(42))
        assert side_effect == [42]

    def test_run_on_main_hub_propagates_exception(self):
        def fail():
            raise RuntimeError("hub error")

        with pytest.raises(RuntimeError, match="hub error"):
            run_on_main_hub(fail)

    def test_run_on_main_hub_propagates_base_exception(self):
        """A BaseException must complete the result too. Catching only
        Exception left the marshalled greenlet dead with the result never set,
        and the caller — the proxy lifecycle waits with no timeout — blocked on
        an answer that could no longer come."""

        def killed():
            raise gevent.GreenletExit("marshalled work killed")

        with pytest.raises(gevent.GreenletExit, match="marshalled work killed"):
            run_on_main_hub(killed, timeout=3)

    def test_spawn_on_main_hub(self):
        results = []

        def append_value(v):
            results.append(v)

        spawn_on_main_hub(append_value, "hello")
        gevent.sleep(0.1)
        assert "hello" in results


class TestMarshalledInterrupt:
    def test_it_reaches_the_host_not_just_the_waiting_thread(self):
        """Relaying the interrupt to the waiter is not enough: that waiter is a
        native thread, where an uncaught KeyboardInterrupt kills only the thread
        while the host carries on serving. It has to reach the hub as well."""
        script = textwrap.dedent(
            """
            from gevent import monkey

            monkey.patch_all()

            import gevent

            from gisolate import hub

            Thread = monkey.get_original("threading", "Thread")
            real_sleep = monkey.get_original("time", "sleep")

            def interrupting():
                raise KeyboardInterrupt

            box = {}

            def worker():
                try:
                    hub.run_on_main_hub(interrupting, timeout=5)
                except BaseException as exc:
                    box["thread"] = type(exc).__name__

            hub.ensure_hub_started()
            t = Thread(target=worker, daemon=True)
            t.start()
            try:
                for _ in range(200):
                    if not t.is_alive():
                        break
                    gevent.sleep(0.05)
                print("HOST STILL RUNNING", box, flush=True)
            except KeyboardInterrupt:
                print("HOST INTERRUPTED", box, flush=True)
            """
        )
        proc = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=60
        )
        # Only the host is asserted: the waiter is told first — set_exception
        # runs before the raise — but the interrupt reaches the main greenlet
        # before that thread is scheduled again, so its record is a race.
        assert "HOST INTERRUPTED" in proc.stdout, (proc.stdout, proc.stderr[-500:])


class TestSpawnOnMainHubIsolation:
    def test_a_task_that_exits_does_not_end_the_process(self):
        """gevent forwards a greenlet's SystemExit to the main one, which ends
        the process. run_on_main_hub has the boundary its AsyncResult gives it;
        fire-and-forget work had nowhere to record one, and the caller that
        scheduled it is on another thread with nothing waiting on the outcome.

        Checked in a child, because without the boundary this kills whatever
        process runs it — including the test session."""
        script = textwrap.dedent(
            """
            from gevent import monkey

            monkey.patch_all()

            import sys

            import gevent

            from gisolate import hub

            hub.ensure_hub_started()
            hub.spawn_on_main_hub(sys.exit, 2)
            gevent.sleep(0.3)
            print("alive")
            """
        )
        proc = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True
        )
        assert "alive" in proc.stdout, (proc.returncode, proc.stdout, proc.stderr)


class TestScheduleRefused:
    def test_a_refused_spawn_reaches_the_waiter(self, monkeypatch):
        """The loop callback is where a refused gevent.spawn surfaces, and its
        only audience was the hub's error handler: the waiter — usually on the
        default unbounded get() — was never told, and wedged for good."""
        import time

        import gevent.monkey

        run_on_main_hub(lambda: None)  # the hub is up before spawn is broken

        Thread = gevent.monkey.get_original("threading", "Thread")
        outcome = []

        def caller():
            try:
                # The UNBOUNDED wait, because that is the wedge being tested: a
                # sliced wait that only polled when a timeout was supplied
                # would pass a bounded call and still wedge this one.
                outcome.append(("returned", run_on_main_hub(lambda: 42)))
            except BaseException as e:  # noqa: BLE001 — the outcome IS the test
                outcome.append(("raised", e))

        def refuse(*_a, **_k):
            raise MemoryError("the hub refused a greenlet")

        monkeypatch.setattr(gevent, "spawn", refuse)
        try:
            t = Thread(target=caller, daemon=True)  # a wedged waiter must not block exit
            t.start()
            deadline = time.monotonic() + 5
            while t.is_alive() and time.monotonic() < deadline:
                gevent.sleep(0.05)
        finally:
            monkeypatch.undo()
        assert not t.is_alive(), "the waiter is still waiting"
        kind, value = outcome[0]
        assert kind == "raised" and isinstance(value, MemoryError), outcome
