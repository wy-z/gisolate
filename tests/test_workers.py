"""Tests for gisolate._workers module."""

import pytest
import zmq

from gisolate._workers import ERR, OK, SHUTDOWN, bind_or_close, safe_close


class TestBindOrClose:
    def test_a_failed_bind_takes_the_transport_with_it(self):
        """serve() runs the worker in a process that survives the error, so a
        context left open here wedges its next term() — that leak hung a whole
        test session once."""
        closed, termed = [], []

        class Sock:
            def bind(self, addr):
                raise zmq.ZMQError(zmq.EINVAL)

            def close(self, linger=None):
                closed.append(linger)

        class Ctx:
            def term(self):
                termed.append(True)

        with pytest.raises(zmq.ZMQError):
            bind_or_close(Ctx(), Sock(), "ipc:///whatever")
        assert closed == [0]
        assert termed == [True]


class TestMarkers:
    def test_ok_marker(self):
        assert OK == b"\x01"

    def test_err_marker(self):
        assert ERR == b"\x00"

    def test_shutdown_marker(self):
        assert SHUTDOWN == b""


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
