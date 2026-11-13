"""Data update coordinator for Komeco Connect."""

from __future__ import annotations

import logging
from time import monotonic
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_SCAN_INTERVAL
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import KomecoApiClient, KomecoApiError, KomecoAuthError, _as_bool, _as_int
from .const import (
    CONF_ACCESS_TOKEN,
    CONF_CONNECTION_MODE,
    CONF_ID_TOKEN,
    CONF_REFRESH_TOKEN,
    CONF_SUB,
    CONNECTION_MODE_AUTO,
    CONNECTION_MODE_CLOUD,
    CONNECTION_MODE_LAN,
    DEFAULT_CONNECTION_MODE,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
)
from .lan import KomecoLanClient, KomecoLanError

_LOGGER = logging.getLogger(__name__)
COMMAND_OVERRIDE_SECONDS = 180.0


class KomecoDataUpdateCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Coordinator for Komeco API polling with optional LAN transport."""

    def __init__(
        self,
        *,
        hass: HomeAssistant,
        entry: ConfigEntry,
        api: KomecoApiClient,
        lan: KomecoLanClient | None = None,
    ) -> None:
        interval = entry.options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=interval,
        )
        self.entry = entry
        self.api = api
        self.lan = lan
        self._command_overrides: dict[str, tuple[Any, float]] = {}

    @property
    def connection_mode(self) -> str:
        """Return configured transport preference."""
        mode = self.entry.options.get(CONF_CONNECTION_MODE, DEFAULT_CONNECTION_MODE)
        return mode if mode in {CONNECTION_MODE_CLOUD, CONNECTION_MODE_LAN, CONNECTION_MODE_AUTO} else DEFAULT_CONNECTION_MODE

    async def _async_update_data(self) -> dict[str, Any]:
        _LOGGER.debug(
            "Coordinator polling update started entry_id=%s mode=%s lan=%s",
            self.entry.entry_id,
            self.connection_mode,
            bool(self.lan),
        )
        mode = self.connection_mode
        lan_error: str | None = None

        if mode in {CONNECTION_MODE_LAN, CONNECTION_MODE_AUTO} and self.lan is not None:
            try:
                data = await self.lan.async_fetch_state()
                previous = self.data if isinstance(self.data, dict) else {}
                data = self._merge_with_previous(data, previous)
                # Keep last known cloud usage totals when LAN bridge omits them.
                for usage_key in ("latest_usage", "today_usage", "month_usage"):
                    if not data.get(usage_key) and previous.get(usage_key):
                        data[usage_key] = previous.get(usage_key)
                if not data.get("device") and previous.get("device"):
                    data["device"] = previous.get("device")
                self._apply_command_overrides(data)
                self._persist_tokens_if_needed()
                _LOGGER.debug(
                    "Coordinator LAN update complete temp=%s detail=%s",
                    data.get("current_temperature"),
                    data.get("transport_detail"),
                )
                return data
            except KomecoLanError as err:
                lan_error = str(err)
                _LOGGER.debug("LAN fetch failed mode=%s err=%s", mode, err)
                if mode == CONNECTION_MODE_LAN:
                    raise UpdateFailed(f"LAN unavailable: {err}") from err

        try:
            data = await self.api.async_fetch_state()
        except KomecoAuthError as err:
            raise ConfigEntryAuthFailed(str(err)) from err
        except KomecoApiError as err:
            raise UpdateFailed(str(err)) from err

        previous = self.data if isinstance(self.data, dict) else {}
        data = self._merge_with_previous(data, previous)
        data["transport"] = "cloud"
        if lan_error:
            data["lan_error"] = lan_error
            data["transport_detail"] = "cloud_fallback"
        else:
            data["transport_detail"] = "cloud"
        if self.lan is not None:
            data["lan_host"] = self.lan.host
            data["lan_port"] = self.lan.port
            data["lan_probe"] = dict(self.lan.last_probe) if self.lan.last_probe else {}

        self._apply_command_overrides(data)
        self._persist_tokens_if_needed()
        _LOGGER.debug(
            "Coordinator polling update complete temp=%s thing=%s shadow_error=%s transport=%s",
            data.get("current_temperature"),
            data.get("shadow_thing_name"),
            data.get("shadow_error"),
            data.get("transport"),
        )
        return data

    @staticmethod
    def _merge_with_previous(data: dict[str, Any], previous: dict[str, Any]) -> dict[str, Any]:
        previous_values = previous.get("command_values", {})
        new_values = data.get("command_values", {})
        if isinstance(previous_values, dict) and isinstance(new_values, dict):
            for key, old_value in previous_values.items():
                if new_values.get(key) is None and old_value is not None:
                    new_values[key] = old_value
        if data.get("current_temperature") is None and previous.get("current_temperature") is not None:
            data["current_temperature"] = previous.get("current_temperature")
        if data.get("last_command_at") is None and previous.get("last_command_at") is not None:
            data["last_command_at"] = previous.get("last_command_at")
        if not data.get("latest_usage") and previous.get("latest_usage"):
            data["latest_usage"] = previous.get("latest_usage")
        for usage_key in ("today_usage", "month_usage"):
            if not data.get(usage_key) and previous.get(usage_key):
                data[usage_key] = previous.get(usage_key)
        return data

    async def async_send_command(self, payload: dict[str, Any]) -> None:
        """Send device command and refresh data."""
        _LOGGER.debug(
            "Coordinator sending command payload=%s mode=%s",
            payload,
            self.connection_mode,
        )
        mode = self.connection_mode
        sent_via = "cloud"

        if mode in {CONNECTION_MODE_LAN, CONNECTION_MODE_AUTO} and self.lan is not None:
            try:
                await self.lan.async_send_command(payload)
                sent_via = "lan"
            except KomecoLanError as err:
                _LOGGER.debug("LAN command failed mode=%s err=%s", mode, err)
                if mode == CONNECTION_MODE_LAN:
                    raise UpdateFailed(f"LAN unavailable: {err}") from err
                # auto → cloud fallback
                try:
                    await self.api.async_send_command(payload)
                except KomecoAuthError as auth_err:
                    raise ConfigEntryAuthFailed(str(auth_err)) from auth_err
                except KomecoApiError as api_err:
                    raise UpdateFailed(str(api_err)) from api_err
                sent_via = "cloud_fallback"
        else:
            try:
                await self.api.async_send_command(payload)
            except KomecoAuthError as err:
                raise ConfigEntryAuthFailed(str(err)) from err
            except KomecoApiError as err:
                raise UpdateFailed(str(err)) from err

        if isinstance(self.data, dict):
            merged = dict(self.data)
            command_values = dict(merged.get("command_values", {}))
            for key in ("switch", "temp_set", "zero_cold_water_mode", "zero_cold_water_mode_status"):
                if key in payload:
                    command_values[key] = payload[key]
            merged["command_values"] = command_values
            merged["transport"] = "lan" if sent_via == "lan" else "cloud"
            merged["transport_detail"] = sent_via
            self.async_set_updated_data(merged)
        self._set_command_overrides(payload)

        await self.async_request_refresh()
        _LOGGER.debug(
            "Coordinator command flow complete payload=%s sent_via=%s",
            payload,
            sent_via,
        )

    def _set_command_overrides(self, payload: dict[str, Any]) -> None:
        expires_at = monotonic() + COMMAND_OVERRIDE_SECONDS
        for key in ("switch", "temp_set", "zero_cold_water_mode", "zero_cold_water_mode_status"):
            if key in payload:
                self._command_overrides[key] = (payload[key], expires_at)
                _LOGGER.debug("Applied override key=%s value=%s", key, payload[key])

    def _apply_command_overrides(self, data: dict[str, Any]) -> None:
        now = monotonic()
        values = data.get("command_values")
        if not isinstance(values, dict):
            values = {}
            data["command_values"] = values

        for key, (override, expires_at) in list(self._command_overrides.items()):
            if expires_at <= now:
                self._command_overrides.pop(key, None)
                _LOGGER.debug("Expired override key=%s", key)
                continue

            polled_value = values.get(key)
            if polled_value == override:
                self._command_overrides.pop(key, None)
                _LOGGER.debug("Override converged key=%s value=%s", key, override)
                continue

            values[key] = override
            if key == "temp_set":
                current_temp = data.get("current_temperature")
                if current_temp is None or current_temp == polled_value:
                    data["current_temperature"] = override

    def _persist_tokens_if_needed(self) -> None:
        token_data = self.api.token_data
        changed = False
        new_data = dict(self.entry.data)

        for key_map in (
            (CONF_ID_TOKEN, "id_token"),
            (CONF_ACCESS_TOKEN, "access_token"),
            (CONF_REFRESH_TOKEN, "refresh_token"),
            (CONF_SUB, "sub"),
        ):
            config_key, token_key = key_map
            token_value = token_data.get(token_key)
            if token_value and token_value != self.entry.data.get(config_key):
                new_data[config_key] = token_value
                changed = True

        if changed:
            _LOGGER.debug("Persisting updated token fields in config entry entry_id=%s", self.entry.entry_id)
            self.hass.config_entries.async_update_entry(self.entry, data=new_data)

    def handle_realtime_shadow_update(
        self,
        *,
        reported: dict[str, Any],
        raw: dict[str, Any] | None = None,
        thing_name: str | None = None,
    ) -> None:
        """Apply a realtime shadow update from MQTT."""
        if not isinstance(reported, dict) or not reported:
            return
        # Prefer not to clobber a live LAN poll with stale cloud push while in LAN-only mode.
        if self.connection_mode == CONNECTION_MODE_LAN:
            _LOGGER.debug("Ignoring realtime cloud shadow while connection_mode=lan")
            return

        _LOGGER.debug(
            "Realtime shadow update received thing=%s keys=%s",
            thing_name,
            sorted(reported.keys()),
        )

        data = dict(self.data) if isinstance(self.data, dict) else {}

        shadow_reported = data.get("shadow_reported")
        if not isinstance(shadow_reported, dict):
            shadow_reported = {}
        shadow_reported = dict(shadow_reported)
        shadow_reported.update(reported)
        data["shadow_reported"] = shadow_reported

        if isinstance(raw, dict):
            data["shadow_raw"] = raw
            timestamp = _as_int(raw.get("timestamp"))
            if timestamp is not None:
                data["shadow_timestamp"] = timestamp
            version = _as_int(raw.get("version"))
            if version is not None:
                data["shadow_version"] = version

        if thing_name:
            data["shadow_thing_name"] = thing_name

        command_values = data.get("command_values")
        if not isinstance(command_values, dict):
            command_values = {}
        command_values = dict(command_values)

        if "switch" in shadow_reported:
            command_values["switch"] = _as_bool(shadow_reported.get("switch"))
        if "temp_set" in shadow_reported:
            command_values["temp_set"] = _as_int(shadow_reported.get("temp_set"))
        if "zero_cold_water_mode" in shadow_reported:
            command_values["zero_cold_water_mode"] = _as_int(shadow_reported.get("zero_cold_water_mode"))
        if "zero_cold_water_mode_status" in shadow_reported:
            command_values["zero_cold_water_mode_status"] = _as_bool(
                shadow_reported.get("zero_cold_water_mode_status")
            )

        data["command_values"] = command_values

        supported = set(data.get("supported_command_keys", []))
        for key in ("switch", "temp_set", "zero_cold_water_mode", "zero_cold_water_mode_status"):
            if key in shadow_reported:
                supported.add(key)
        supported.add("switch")
        supported.add("temp_set")
        data["supported_command_keys"] = sorted(supported)

        for temp_key in (
            "current_temp",
            "temp_current_output",
            "temp_current_input",
            "temp",
            "temp_current",
            "temperature",
            "water_temp",
        ):
            temp_value = _as_int(shadow_reported.get(temp_key))
            if temp_value is not None:
                data["current_temperature"] = temp_value
                break

        if data.get("transport") != "lan":
            data["transport"] = "cloud"
            data["transport_detail"] = "cloud_mqtt"

        self._apply_command_overrides(data)
        self.async_set_updated_data(data)
