"""Config flow for Komeco Connect."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigFlow, OptionsFlow
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import KomecoApiClient, KomecoApiError, KomecoAuthError
from .const import (
    CONF_ACCESS_TOKEN,
    CONF_CONNECTION_MODE,
    CONF_DEVICE_ID,
    CONF_EMAIL,
    CONF_ID_TOKEN,
    CONF_LAN_HOST,
    CONF_LAN_PORT,
    CONF_LAN_SSL,
    CONF_PASSWORD,
    CONF_PLACE_ID,
    CONF_REFRESH_TOKEN,
    CONF_SUB,
    CONNECTION_MODE_AUTO,
    CONNECTION_MODE_CLOUD,
    CONNECTION_MODE_LAN,
    CONNECTION_MODES,
    DEFAULT_CONNECTION_MODE,
    DEFAULT_LAN_PORT,
    DOMAIN,
)
from .lan import KomecoLanClient, KomecoLanError

_LOGGER = logging.getLogger(__name__)


def _mask_email(email: str) -> str:
    if "@" not in email:
        return "***"
    local, domain = email.split("@", 1)
    return f"{local[:2]}***@{domain}" if len(local) > 2 else f"***@{domain}"


class KomecoConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Komeco Connect."""

    VERSION = 1

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        """Return the options flow handler."""
        return KomecoOptionsFlow()

    async def async_step_user(self, user_input: dict[str, Any] | None = None):
        """Handle the user step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            raw_email = str(user_input.get(CONF_EMAIL, "")).strip()
            _LOGGER.debug("Starting config flow login for email=%s", _mask_email(raw_email))
            try:
                session = async_get_clientsession(self.hass)
                api = KomecoApiClient(
                    session=session,
                    email=user_input[CONF_EMAIL],
                    password=(user_input.get(CONF_PASSWORD) or "").strip(),
                    refresh_token=(user_input.get(CONF_REFRESH_TOKEN) or "").strip(),
                    device_id=None,
                    place_id=None,
                )
                password = (user_input.get(CONF_PASSWORD) or "").strip()
                if password:
                    await api.async_login_with_password(password)
                else:
                    await api.async_authenticate()

                heater_name = "Komeco Device"
                discovered = await api.async_discover_heaters()
                if not discovered:
                    errors["base"] = "no_devices"
                    _LOGGER.debug("Config flow discovered no supported heater devices")
                else:
                    first = discovered[0]
                    device_id = first["device_id"]
                    place_id = first["place_id"]
                    heater_name = first["device_name"] or heater_name

                if not errors:
                    await self.async_set_unique_id(device_id)
                    self._abort_if_unique_id_configured()

                    data = {
                        CONF_EMAIL: user_input[CONF_EMAIL].strip(),
                        CONF_PASSWORD: password,
                        CONF_REFRESH_TOKEN: api.refresh_token,
                        CONF_DEVICE_ID: device_id,
                        CONF_PLACE_ID: place_id,
                    }
                    token_data = api.token_data
                    if token_data.get("id_token"):
                        data[CONF_ID_TOKEN] = token_data["id_token"]
                    if token_data.get("access_token"):
                        data[CONF_ACCESS_TOKEN] = token_data["access_token"]
                    if token_data.get("sub"):
                        data[CONF_SUB] = token_data["sub"]

                    title = heater_name if heater_name else f"Komeco {device_id}"
                    _LOGGER.debug("Config flow succeeded device_id=%s place_id=%s", device_id, place_id)
                    return self.async_create_entry(
                        title=title,
                        data=data,
                        options={
                            CONF_CONNECTION_MODE: DEFAULT_CONNECTION_MODE,
                            CONF_LAN_PORT: DEFAULT_LAN_PORT,
                            CONF_LAN_SSL: False,
                        },
                    )

            except KomecoAuthError:
                _LOGGER.debug("Config flow auth failed", exc_info=True)
                errors["base"] = "invalid_auth"
            except KomecoApiError:
                _LOGGER.debug("Config flow API connectivity failed", exc_info=True)
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.debug("Config flow unexpected error", exc_info=True)
                errors["base"] = "unknown"

        schema = vol.Schema(
            {
                vol.Required(CONF_EMAIL): str,
                vol.Required(CONF_PASSWORD): str,
            }
        )
        return self.async_show_form(step_id="user", data_schema=schema, errors=errors)


class KomecoOptionsFlow(OptionsFlow):
    """Handle Komeco Connect options (LAN / transport)."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None):
        """Manage connection mode and optional LAN host."""
        errors: dict[str, str] = {}
        current = self.config_entry.options

        if user_input is not None:
            mode = user_input.get(CONF_CONNECTION_MODE, DEFAULT_CONNECTION_MODE)
            host = str(user_input.get(CONF_LAN_HOST) or "").strip()
            port = int(user_input.get(CONF_LAN_PORT) or DEFAULT_LAN_PORT)
            use_ssl = bool(user_input.get(CONF_LAN_SSL, False))

            if mode not in CONNECTION_MODES:
                errors["base"] = "unknown"
            elif mode in {CONNECTION_MODE_LAN, CONNECTION_MODE_AUTO} and not host:
                errors[CONF_LAN_HOST] = "lan_host_required"
            elif mode == CONNECTION_MODE_LAN and host:
                session = async_get_clientsession(self.hass)
                client = KomecoLanClient(session, host=host, port=port, use_ssl=use_ssl)
                try:
                    reachable = await client.async_reachable()
                except KomecoLanError:
                    reachable = False
                if not reachable:
                    errors["base"] = "lan_unreachable"
                    _LOGGER.debug(
                        "Options LAN probe failed host=%s port=%s probe=%s",
                        host,
                        port,
                        client.last_probe,
                    )

            if not errors:
                return self.async_create_entry(
                    title="",
                    data={
                        CONF_CONNECTION_MODE: mode,
                        CONF_LAN_HOST: host,
                        CONF_LAN_PORT: port,
                        CONF_LAN_SSL: use_ssl,
                    },
                )

        schema = vol.Schema(
            {
                vol.Required(
                    CONF_CONNECTION_MODE,
                    default=current.get(CONF_CONNECTION_MODE, DEFAULT_CONNECTION_MODE),
                ): vol.In(
                    {
                        CONNECTION_MODE_CLOUD: "Cloud (Connect Komeco / AWS)",
                        CONNECTION_MODE_AUTO: "Auto (prefer LAN, fallback cloud)",
                        CONNECTION_MODE_LAN: "LAN only (experimental)",
                    }
                ),
                vol.Optional(
                    CONF_LAN_HOST,
                    default=current.get(CONF_LAN_HOST, ""),
                ): str,
                vol.Optional(
                    CONF_LAN_PORT,
                    default=int(current.get(CONF_LAN_PORT, DEFAULT_LAN_PORT) or DEFAULT_LAN_PORT),
                ): vol.All(vol.Coerce(int), vol.Range(min=1, max=65535)),
                vol.Optional(
                    CONF_LAN_SSL,
                    default=bool(current.get(CONF_LAN_SSL, False)),
                ): bool,
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema, errors=errors)
