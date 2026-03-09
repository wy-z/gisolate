"""Tests for gisolate.bridge module."""

import tempfile
import uuid

import pytest

from gisolate.bridge import ProcessBridge


def _make_addr():
    return f"ipc://{tempfile.gettempdir()}/gisolate-test-{uuid.uuid4().hex}.sock"


class TestProcessBridgeServer:
    def test_start_server(self):
        addr = _make_addr()
        bridge = ProcessBridge(addr, mode="server")
        assert bridge.start() is bridge  # returns self for chaining
        bridge.close()

    def test_explicit_server_mode(self):
        addr = _make_addr()
        bridge = ProcessBridge(addr, mode="server").start()
        assert bridge.address == addr
        bridge.close()

    def test_address_is_plain_property(self):
        addr = _make_addr()
        bridge = ProcessBridge(addr, mode="client")
        assert bridge.address == addr  # no side effects
        bridge.close()

    def test_close_idempotent_before_start(self):
        addr = _make_addr()
        bridge = ProcessBridge(addr, mode="server")
        bridge.close()  # never started, should not raise
        bridge.close()  # double close, should not raise

    def test_start_idempotent(self):
        addr = _make_addr()
        bridge = ProcessBridge(addr, mode="server")
        bridge.start()
        bridge.start()  # second start is no-op
        bridge.close()

    def test_server_mode_cannot_call(self):
        addr = _make_addr()
        bridge = ProcessBridge(addr, mode="server")

        async def try_call():
            await bridge.call(lambda: 42)

        import asyncio

        with pytest.raises(RuntimeError, match="server mode"):
            asyncio.run(try_call())

        bridge.close()


class TestProcessBridgeRPC:
    def test_server_client_roundtrip(self):
        """Server handles a function call from an asyncio client."""
        addr = _make_addr()
        server = ProcessBridge(addr, mode="server")
        server.start()

        import asyncio

        async def client_call():
            client = ProcessBridge(addr, mode="client")
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
        server = ProcessBridge(addr, mode="server")
        server.start()

        import asyncio

        async def client_call():
            client = ProcessBridge(addr, mode="client")
            try:

                def fail():
                    raise ValueError("bridge error")

                await client.call(fail, timeout=5)
            finally:
                client.close()

        with pytest.raises(ValueError, match="bridge error"):
            asyncio.run(client_call())

        server.close()

    def test_multiple_calls(self):
        addr = _make_addr()
        server = ProcessBridge(addr, mode="server")
        server.start()

        import asyncio

        async def client_calls():
            client = ProcessBridge(addr, mode="client")
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
        server = ProcessBridge(addr, mode="server")
        server.start()

        import asyncio

        client = ProcessBridge(addr, mode="client")
        for i in range(3):
            result = asyncio.run(client.call(lambda x: x * 2, i, timeout=5))
            assert result == i * 2
        client.close()
        server.close()
