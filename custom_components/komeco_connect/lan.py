"""Experimental LAN transport for Komeco heaters.

Stock Connect Komeco gas heaters (HF-LPT130B class modules) currently appear to
speak only to AWS IoT over the internet: community packet captures and LAN
port scans have not found a stable inbound HTTP/MQTT control API.

This module implements:

1. A **documented experimental local protocol** (HTTP JSON + TCP JSON lines)
   that mirrors cloud command payloads (`switch`, `temp_set`, …). It is meant
   for a future local bridge / firmware that exposes the same shape on LAN.
2. **Discovery/probe helpers** that scan common Hi-Flying ports and record
   what answered, so reverse engineering can progress from diagnostics.
3. Integration with the coordinator via connection modes ``cloud`` / ``lan`` /
   ``auto`` (prefer LAN, fall back to cloud).

Until a device answers the local protocol, ``auto`` keeps working via cloud and
``lan`` mode surfaces a clear connectivity error.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import socket
from typing import Any

from aiohttp import ClientError, ClientSession, ClientTimeout

from .api import _as_bool, _as_int

_LOGGER = logging.getLogger(__name__)

LAN_HTTP_STATE_PATHS = (
    "/api/v1/state",
    "/komeco/state",
    "/api/state",
    "/state",
)
LAN_HTTP_COMMAND_PATHS = (
    "/api/v1/command",
    "/komeco/command",
    "/api/command",
    "/command",
)

# Common Hi-Flying / IoT listen ports seen on related modules.
LAN_PROBE_PORTS: tuple[int, ...] = (80, 443, 1883, 8883, 8899, 8080, 8000, 502, 10001)

DEFAULT_LAN_PORT = 80
DEFAULT_LAN_TIMEOUT = 3.0


class KomecoLanError(Exception):
    """LAN transport error."""


class KomecoLanClient:
    """Talk to a heater (or local bridge) over the experimental LAN protocol."""

    def __init__(
        self,
        session: ClientSession,
        *,
        host: str,
        port: int = DEFAULT_LAN_PORT,
        timeout: float = DEFAULT_LAN_TIMEOUT,
        use_ssl: bool = False,
    ) -> None:
        self._session = session
        self.host = host.strip()
        self.port = int(port)
        self.timeout = float(timeout)
        self.use_ssl = bool(use_ssl)
        self._http_state_path: str | None = None
        self._http_command_path: str | None = None
        self.last_probe: dict[str, Any] = {}
        self.last_error: str | None = None

    @property
    def base_url(self) -> str:
        scheme = "https" if self.use_ssl else "http"
        return f"{scheme}://{self.host}:{self.port}"

    async def async_probe(self) -> dict[str, Any]:
        """Probe HTTP + TCP and remember which endpoints answered."""
        result: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "tcp_open": False,
            "http_ok": False,
            "protocol": None,
            "paths": {},
            "error": None,
        }
        try:
            result["tcp_open"] = await self._async_tcp_open(self.host, self.port)
        except Exception as err:  # noqa: BLE001 - probe must stay resilient
            result["error"] = f"tcp: {err}"

        if result["tcp_open"]:
            for path in LAN_HTTP_STATE_PATHS:
                try:
                    payload = await self._async_http_get_json(path)
                except KomecoLanError:
                    continue
                if self._looks_like_state(payload):
                    result["http_ok"] = True
                    result["protocol"] = "http_json"
                    result["paths"]["state"] = path
                    self._http_state_path = path
                    break

            for path in LAN_HTTP_COMMAND_PATHS:
                # Command paths are confirmed lazily on first write; remember candidates.
                result["paths"].setdefault("command_candidates", []).append(path)

            if not result["http_ok"] and self.port in {8899, 10001, 502}:
                # Transparent TCP servers often have no HTTP; mark tcp_json candidate.
                result["protocol"] = "tcp_json"
                result["http_ok"] = False

        self.last_probe = result
        self.last_error = result.get("error")
        _LOGGER.debug("LAN probe result=%s", result)
        return result

    async def async_reachable(self) -> bool:
        """Return True when the configured host answers the local protocol."""
        probe = await self.async_probe()
        if probe.get("protocol") == "http_json" and probe.get("http_ok"):
            return True
        if probe.get("protocol") == "tcp_json" and probe.get("tcp_open"):
            # TCP-only is optimistic: require a successful state handshake.
            try:
                await self._async_tcp_json_exchange({"type": "get_state"})
                return True
            except KomecoLanError as err:
                self.last_error = str(err)
                return False
        return False

    async def async_fetch_state(self) -> dict[str, Any]:
        """Fetch heater state via LAN and normalize to coordinator shape."""
        self.last_error = None
        try:
            if self._http_state_path is None:
                await self.async_probe()
            if self._http_state_path:
                raw = await self._async_http_get_json(self._http_state_path)
                return self._normalize_state(raw, transport_detail="http_json")
            # Fall back to TCP JSON lines.
            raw = await self._async_tcp_json_exchange({"type": "get_state"})
            if isinstance(raw, dict) and raw.get("type") == "state":
                return self._normalize_state(raw.get("data") or raw, transport_detail="tcp_json")
            return self._normalize_state(raw, transport_detail="tcp_json")
        except KomecoLanError as err:
            self.last_error = str(err)
            raise

    async def async_send_command(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Send a cloud-shaped command payload over LAN."""
        self.last_error = None
        body = {k: v for k, v in payload.items() if v is not None}
        try:
            if self._http_command_path is None:
                await self.async_probe()
            if self._http_state_path or self._http_command_path:
                paths = (
                    [self._http_command_path]
                    if self._http_command_path
                    else list(LAN_HTTP_COMMAND_PATHS)
                )
                last_err: Exception | None = None
                for path in paths:
                    if not path:
                        continue
                    try:
                        response = await self._async_http_post_json(path, body)
                        self._http_command_path = path
                        return response if isinstance(response, dict) else {"ok": True}
                    except KomecoLanError as err:
                        last_err = err
                        continue
                raise KomecoLanError(str(last_err) if last_err else "No LAN command endpoint")

            response = await self._async_tcp_json_exchange(
                {"type": "command", "payload": body}
            )
            return response if isinstance(response, dict) else {"ok": True}
        except KomecoLanError as err:
            self.last_error = str(err)
            raise

    def _normalize_state(self, raw: Any, *, transport_detail: str) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raise KomecoLanError("LAN state payload is not a JSON object")

        reported = raw.get("shadow_reported")
        if not isinstance(reported, dict):
            reported = raw.get("reported")
        if not isinstance(reported, dict):
            reported = raw.get("state") if isinstance(raw.get("state"), dict) else {}
            if isinstance(reported, dict) and isinstance(reported.get("reported"), dict):
                reported = reported["reported"]

        command_values = raw.get("command_values")
        if not isinstance(command_values, dict):
            command_values = {}
        command_values = dict(command_values)

        for key in ("switch", "temp_set", "zero_cold_water_mode", "zero_cold_water_mode_status"):
            if key in reported and command_values.get(key) is None:
                if key in {"switch", "zero_cold_water_mode_status"}:
                    command_values[key] = _as_bool(reported.get(key))
                else:
                    command_values[key] = _as_int(reported.get(key))
            elif key in raw and command_values.get(key) is None:
                if key in {"switch", "zero_cold_water_mode_status"}:
                    command_values[key] = _as_bool(raw.get(key))
                else:
                    command_values[key] = _as_int(raw.get(key))

        current_temp = _as_int(raw.get("current_temperature"))
        if current_temp is None:
            for key in (
                "current_temp",
                "temp_current_output",
                "temp_current_input",
                "temp",
                "temperature",
            ):
                current_temp = _as_int(reported.get(key)) if isinstance(reported, dict) else None
                if current_temp is not None:
                    break
        if current_temp is None:
            current_temp = command_values.get("temp_set")

        supported = set(raw.get("supported_command_keys") or [])
        supported.update(k for k in command_values if command_values.get(k) is not None)
        supported.add("switch")
        supported.add("temp_set")

        device = raw.get("device") if isinstance(raw.get("device"), dict) else {}

        return {
            "device": device,
            "dashboard": raw.get("dashboard") if isinstance(raw.get("dashboard"), dict) else {},
            "latest_usage": raw.get("latest_usage") if isinstance(raw.get("latest_usage"), dict) else {},
            "today_usage": raw.get("today_usage") if isinstance(raw.get("today_usage"), dict) else {},
            "month_usage": raw.get("month_usage") if isinstance(raw.get("month_usage"), dict) else {},
            "command_values": command_values,
            "supported_command_keys": sorted(supported),
            "current_temperature": current_temp,
            "last_command_at": raw.get("last_command_at"),
            "shadow_reported": reported if isinstance(reported, dict) else {},
            "shadow_raw": raw.get("shadow_raw") if isinstance(raw.get("shadow_raw"), dict) else {},
            "shadow_timestamp": _as_int(raw.get("shadow_timestamp")),
            "shadow_version": _as_int(raw.get("shadow_version")),
            "shadow_thing_name": raw.get("shadow_thing_name"),
            "shadow_error": None,
            "transport": "lan",
            "transport_detail": transport_detail,
            "lan_host": self.host,
            "lan_port": self.port,
            "lan_probe": dict(self.last_probe) if self.last_probe else {},
        }

    async def _async_http_get_json(self, path: str) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        try:
            async with self._session.get(
                url,
                timeout=ClientTimeout(total=self.timeout),
                allow_redirects=False,
            ) as response:
                if response.status >= 400:
                    raise KomecoLanError(f"HTTP GET {path} → {response.status}")
                data = await response.json(content_type=None)
        except (ClientError, asyncio.TimeoutError, json.JSONDecodeError) as err:
            raise KomecoLanError(f"HTTP GET {path} failed: {err}") from err
        if not isinstance(data, dict):
            raise KomecoLanError(f"HTTP GET {path} returned non-object JSON")
        return data

    async def _async_http_post_json(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        try:
            async with self._session.post(
                url,
                json=body,
                timeout=ClientTimeout(total=self.timeout),
                allow_redirects=False,
            ) as response:
                if response.status >= 400:
                    raise KomecoLanError(f"HTTP POST {path} → {response.status}")
                if response.content_length == 0:
                    return {"ok": True}
                try:
                    data = await response.json(content_type=None)
                except json.JSONDecodeError:
                    return {"ok": True, "raw": await response.text()}
        except (ClientError, asyncio.TimeoutError) as err:
            raise KomecoLanError(f"HTTP POST {path} failed: {err}") from err
        return data if isinstance(data, dict) else {"ok": True}

    async def _async_tcp_json_exchange(self, message: dict[str, Any]) -> Any:
        reader: asyncio.StreamReader
        writer: asyncio.StreamWriter
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=self.timeout,
            )
        except (OSError, asyncio.TimeoutError) as err:
            raise KomecoLanError(f"TCP connect {self.host}:{self.port} failed: {err}") from err

        try:
            writer.write((json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8"))
            await writer.drain()
            line = await asyncio.wait_for(reader.readline(), timeout=self.timeout)
            if not line:
                raise KomecoLanError("TCP JSON server closed without reply")
            try:
                return json.loads(line.decode("utf-8"))
            except json.JSONDecodeError as err:
                raise KomecoLanError(f"Invalid TCP JSON reply: {err}") from err
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001
                pass

    @staticmethod
    async def _async_tcp_open(host: str, port: int, timeout: float = 1.0) -> bool:
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout,
            )
        except (OSError, asyncio.TimeoutError):
            return False
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:  # noqa: BLE001
            pass
        return True

    @staticmethod
    def _looks_like_state(payload: dict[str, Any]) -> bool:
        if any(
            key in payload
            for key in (
                "shadow_reported",
                "reported",
                "command_values",
                "temp_set",
                "switch",
                "current_temperature",
            )
        ):
            return True
        state = payload.get("state")
        return isinstance(state, dict) and (
            "reported" in state or "temp_set" in state or "switch" in state
        )


async def async_discover_lan_hosts(
    session: ClientSession,
    *,
    network: str | None = None,
    ports: tuple[int, ...] = LAN_PROBE_PORTS,
    concurrency: int = 64,
) -> list[dict[str, Any]]:
    """Scan a /24 (or given CIDR) for hosts with open probe ports.

    This does **not** claim a host is a Komeco heater — it only lists LAN
    endpoints that accept TCP, optionally marking HTTP JSON protocol hits.
    """
    if network is None:
        network = _guess_local_cidr()
    try:
        net = ipaddress.ip_network(network, strict=False)
    except ValueError as err:
        raise KomecoLanError(f"Invalid network CIDR: {network}") from err

    if net.num_addresses > 512:
        raise KomecoLanError("LAN discovery limited to networks with <= 512 addresses")

    sem = asyncio.Semaphore(concurrency)
    found: list[dict[str, Any]] = []

    async def _check(ip: str, port: int) -> None:
        async with sem:
            open_ = await KomecoLanClient._async_tcp_open(ip, port, timeout=0.35)
            if not open_:
                return
            item: dict[str, Any] = {"host": ip, "port": port, "tcp_open": True}
            if port in {80, 8080, 8000, 443}:
                client = KomecoLanClient(
                    session,
                    host=ip,
                    port=port,
                    use_ssl=port == 443,
                    timeout=1.5,
                )
                try:
                    probe = await client.async_probe()
                    if probe.get("http_ok"):
                        item["protocol"] = probe.get("protocol")
                        item["paths"] = probe.get("paths")
                except KomecoLanError:
                    pass
            found.append(item)

    tasks = [
        asyncio.create_task(_check(str(host), port))
        for host in net.hosts()
        for port in ports
    ]
    if tasks:
        await asyncio.gather(*tasks)
    found.sort(key=lambda row: (row["host"], row["port"]))
    _LOGGER.debug("LAN discovery network=%s hits=%s", network, len(found))
    return found


def _guess_local_cidr() -> str:
    """Best-effort /24 guess from the primary outbound interface."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
    except OSError:
        ip = "192.168.1.1"
    finally:
        sock.close()
    return str(ipaddress.ip_network(f"{ip}/24", strict=False))
