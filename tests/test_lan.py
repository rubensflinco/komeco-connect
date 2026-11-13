"""Tests for experimental LAN transport."""

from __future__ import annotations

import json
import sys
import threading
import types
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from aiohttp import ClientSession


PACKAGE_DIR = Path(__file__).parents[1] / "custom_components" / "komeco_connect"


def _load_lan_module():
    package = types.ModuleType("komeco_connect")
    package.__path__ = [str(PACKAGE_DIR)]
    sys.modules["komeco_connect"] = package

    api_mod = types.ModuleType("komeco_connect.api")

    def _as_int(value):
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            try:
                return int(value.strip())
            except ValueError:
                return None
        return None

    def _as_bool(value):
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return value != 0
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "on", "yes"}:
                return True
            if normalized in {"0", "false", "off", "no"}:
                return False
        return None

    api_mod._as_int = _as_int
    api_mod._as_bool = _as_bool
    sys.modules["komeco_connect.api"] = api_mod

    spec = spec_from_file_location("komeco_connect.lan", PACKAGE_DIR / "lan.py")
    assert spec and spec.loader
    module = module_from_spec(spec)
    sys.modules["komeco_connect.lan"] = module
    spec.loader.exec_module(module)
    return module


lan = _load_lan_module()


class _HeaterHandler(BaseHTTPRequestHandler):
    state = {
        "shadow_reported": {
            "switch": True,
            "temp_set": 42,
            "temp_current_output": 41,
            "connected": True,
        },
        "command_values": {"switch": True, "temp_set": 42},
        "current_temperature": 41,
        "device": {"deviceName": "Mock Komeco", "version": "1.0.36"},
    }
    last_command = None

    def log_message(self, format, *args):  # noqa: A003
        return

    def do_GET(self):  # noqa: N802
        if self.path not in {"/api/v1/state", "/komeco/state"}:
            self.send_response(404)
            self.end_headers()
            return
        body = json.dumps(self.state).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):  # noqa: N802
        if self.path not in {"/api/v1/command", "/komeco/command"}:
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        payload = json.loads(raw.decode("utf-8") or "{}")
        type(self).last_command = payload
        reported = dict(self.state["shadow_reported"])
        command_values = dict(self.state["command_values"])
        for key, value in payload.items():
            reported[key] = value
            command_values[key] = value
        self.state["shadow_reported"] = reported
        self.state["command_values"] = command_values
        if "temp_set" in payload:
            self.state["current_temperature"] = payload["temp_set"]
        body = b'{"ok":true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class LanClientTests(unittest.IsolatedAsyncioTestCase):
    """Exercise HTTP JSON LAN protocol against a local mock heater."""

    async def asyncSetUp(self) -> None:
        _HeaterHandler.state = {
            "shadow_reported": {
                "switch": True,
                "temp_set": 42,
                "temp_current_output": 41,
                "connected": True,
            },
            "command_values": {"switch": True, "temp_set": 42},
            "current_temperature": 41,
            "device": {"deviceName": "Mock Komeco", "version": "1.0.36"},
        }
        _HeaterHandler.last_command = None
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _HeaterHandler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.session = ClientSession()

    async def asyncTearDown(self) -> None:
        await self.session.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    async def test_probe_and_fetch_state(self) -> None:
        client = lan.KomecoLanClient(self.session, host="127.0.0.1", port=self.port)
        probe = await client.async_probe()
        self.assertTrue(probe["tcp_open"])
        self.assertTrue(probe["http_ok"])
        self.assertEqual(probe["protocol"], "http_json")

        state = await client.async_fetch_state()
        self.assertEqual(state["transport"], "lan")
        self.assertEqual(state["command_values"]["temp_set"], 42)
        self.assertEqual(state["current_temperature"], 41)
        self.assertTrue(await client.async_reachable())

    async def test_send_command(self) -> None:
        client = lan.KomecoLanClient(self.session, host="127.0.0.1", port=self.port)
        await client.async_probe()
        await client.async_send_command({"temp_set": 45, "switch": True})
        self.assertEqual(_HeaterHandler.last_command, {"temp_set": 45, "switch": True})
        state = await client.async_fetch_state()
        self.assertEqual(state["command_values"]["temp_set"], 45)

    async def test_unreachable_host(self) -> None:
        client = lan.KomecoLanClient(self.session, host="127.0.0.1", port=1, timeout=0.3)
        with self.assertRaises(lan.KomecoLanError):
            await client.async_fetch_state()


class LanNormalizeTests(unittest.TestCase):
    """Pure normalization helpers."""

    def test_normalize_reported_wrapper(self) -> None:
        client = lan.KomecoLanClient(object(), host="1.2.3.4", port=80)
        state = client._normalize_state(
            {"reported": {"switch": False, "temp_set": 38}},
            transport_detail="http_json",
        )
        self.assertEqual(state["command_values"]["temp_set"], 38)
        self.assertEqual(state["command_values"]["switch"], False)
        self.assertEqual(state["lan_host"], "1.2.3.4")


if __name__ == "__main__":
    unittest.main()
