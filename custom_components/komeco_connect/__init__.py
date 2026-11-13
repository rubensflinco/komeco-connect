"""Komeco Connect integration."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import KomecoApiClient
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
    DEFAULT_CONNECTION_MODE,
    DEFAULT_LAN_PORT,
    DOMAIN,
    PLATFORMS,
)
from .coordinator import KomecoDataUpdateCoordinator
from .lan import KomecoLanClient, async_discover_lan_hosts
from .presets import KomecoPresetStore
from .realtime import KomecoRealtimeListener

KomecoConfigEntry = ConfigEntry
_LOGGER = logging.getLogger(__name__)


def _build_lan_client(hass: HomeAssistant, entry: KomecoConfigEntry) -> KomecoLanClient | None:
    host = (entry.options.get(CONF_LAN_HOST) or "").strip()
    if not host:
        return None
    session = async_get_clientsession(hass)
    port = int(entry.options.get(CONF_LAN_PORT, DEFAULT_LAN_PORT) or DEFAULT_LAN_PORT)
    use_ssl = bool(entry.options.get(CONF_LAN_SSL, False))
    return KomecoLanClient(session=session, host=host, port=port, use_ssl=use_ssl)


async def async_setup_entry(hass: HomeAssistant, entry: KomecoConfigEntry) -> bool:
    """Set up Komeco from a config entry."""
    _LOGGER.debug("Setting up Komeco entry_id=%s", entry.entry_id)
    hass.data.setdefault(DOMAIN, {})
    session = async_get_clientsession(hass)

    api = KomecoApiClient(
        session=session,
        email=entry.data[CONF_EMAIL],
        password=entry.data.get(CONF_PASSWORD),
        refresh_token=entry.data[CONF_REFRESH_TOKEN],
        device_id=entry.data.get(CONF_DEVICE_ID),
        place_id=entry.data.get(CONF_PLACE_ID),
        id_token=entry.data.get(CONF_ID_TOKEN),
        access_token=entry.data.get(CONF_ACCESS_TOKEN),
        sub=entry.data.get(CONF_SUB),
    )
    lan = _build_lan_client(hass, entry)
    coordinator = KomecoDataUpdateCoordinator(hass=hass, entry=entry, api=api, lan=lan)
    await coordinator.async_config_entry_first_refresh()
    _LOGGER.debug(
        "Initial refresh completed for entry_id=%s mode=%s lan_host=%s",
        entry.entry_id,
        entry.options.get(CONF_CONNECTION_MODE, DEFAULT_CONNECTION_MODE),
        entry.options.get(CONF_LAN_HOST),
    )

    mode = entry.options.get(CONF_CONNECTION_MODE, DEFAULT_CONNECTION_MODE)
    realtime = None
    if mode != "lan":
        realtime = KomecoRealtimeListener(hass=hass, api=api, coordinator=coordinator)
        await realtime.async_start()
        _LOGGER.debug("Realtime listener started for entry_id=%s", entry.entry_id)
    else:
        _LOGGER.debug("Realtime listener skipped (connection_mode=lan) entry_id=%s", entry.entry_id)

    hass.data[DOMAIN][entry.entry_id] = {
        "api": api,
        "lan": lan,
        "coordinator": coordinator,
        "presets": KomecoPresetStore(hass, entry),
        "realtime": realtime,
    }

    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    if not hass.services.has_service(DOMAIN, "probe_lan"):
        hass.services.async_register(DOMAIN, "probe_lan", _async_service_probe_lan)
    if not hass.services.has_service(DOMAIN, "discover_lan"):
        hass.services.async_register(DOMAIN, "discover_lan", _async_service_discover_lan)

    return True


async def _async_update_listener(hass: HomeAssistant, entry: KomecoConfigEntry) -> None:
    """Reload when options change (LAN host/mode)."""
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: KomecoConfigEntry) -> bool:
    """Unload a config entry."""
    _LOGGER.debug("Unloading Komeco entry_id=%s", entry.entry_id)
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    listener = hass.data.get(DOMAIN, {}).get(entry.entry_id, {}).get("realtime")
    if listener is not None:
        await listener.async_stop()
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
        if not hass.data[DOMAIN]:
            hass.data.pop(DOMAIN)
            if hass.services.has_service(DOMAIN, "probe_lan"):
                hass.services.async_remove(DOMAIN, "probe_lan")
            if hass.services.has_service(DOMAIN, "discover_lan"):
                hass.services.async_remove(DOMAIN, "discover_lan")
    _LOGGER.debug("Unload finished for entry_id=%s unload_ok=%s", entry.entry_id, unload_ok)
    return unload_ok


async def _async_service_probe_lan(call: ServiceCall) -> None:
    """Probe the configured LAN host for the experimental local protocol."""
    hass = call.hass
    entry_id = call.data.get("entry_id")
    targets = []
    if entry_id and entry_id in hass.data.get(DOMAIN, {}):
        targets = [hass.data[DOMAIN][entry_id]]
    else:
        targets = list(hass.data.get(DOMAIN, {}).values())

    for bucket in targets:
        lan: KomecoLanClient | None = bucket.get("lan")
        coordinator: KomecoDataUpdateCoordinator = bucket["coordinator"]
        if lan is None:
            _LOGGER.warning("probe_lan skipped: no lan_host configured for entry %s", coordinator.entry.entry_id)
            continue
        result = await lan.async_probe()
        _LOGGER.info("probe_lan entry=%s result=%s", coordinator.entry.entry_id, result)
        if isinstance(coordinator.data, dict):
            merged = dict(coordinator.data)
            merged["lan_probe"] = result
            coordinator.async_set_updated_data(merged)


async def _async_service_discover_lan(call: ServiceCall) -> None:
    """Scan the local /24 for open probe ports (diagnostic helper)."""
    hass = call.hass
    session = async_get_clientsession(hass)
    network = call.data.get("network")
    hits = await async_discover_lan_hosts(session, network=network)
    _LOGGER.info("discover_lan network=%s hits=%s", network or "auto", hits)
    # Stash on first coordinator for UI/diagnostics visibility.
    for bucket in hass.data.get(DOMAIN, {}).values():
        coordinator: KomecoDataUpdateCoordinator = bucket["coordinator"]
        if isinstance(coordinator.data, dict):
            merged = dict(coordinator.data)
            merged["lan_discovery"] = hits
            coordinator.async_set_updated_data(merged)
        break
