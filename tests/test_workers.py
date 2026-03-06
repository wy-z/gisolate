"""Tests for gisolate._workers module."""

from gisolate._workers import _ERR, _OK, _SHUTDOWN, safe_close


class TestMarkers:
    def test_ok_marker(self):
        assert _OK == b"\x01"

    def test_err_marker(self):
        assert _ERR == b"\x00"

    def test_shutdown_marker(self):
        assert _SHUTDOWN == b""


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
