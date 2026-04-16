import argparse

import pytest

from ea_driver.config import (
    ConnectionSettings,
    deep_merge_dicts,
    discover_serial_ports,
    resolve_connection_settings,
    resolve_serial_port,
)


def test_discover_serial_ports_deduplicates_symlink_and_tty(monkeypatch):
    def fake_glob(pattern: str) -> list[str]:
        return {
            "/dev/serial/by-id/*": ["/dev/serial/by-id/ea_psb"],
            "/dev/ttyACM*": ["/dev/ttyACM0"],
            "/dev/ttyUSB*": [],
        }[pattern]

    def fake_realpath(path: str) -> str:
        return {
            "/dev/serial/by-id/ea_psb": "/dev/ttyACM0",
            "/dev/ttyACM0": "/dev/ttyACM0",
        }[path]

    monkeypatch.setattr("ea_driver.config.glob.glob", fake_glob)
    monkeypatch.setattr("ea_driver.config.os.path.realpath", fake_realpath)

    assert discover_serial_ports() == ["/dev/serial/by-id/ea_psb"]


def test_resolve_serial_port_prefers_single_glob_match():
    port = resolve_serial_port(
        serial_glob="*PSB*",
        candidates=["/dev/serial/by-id/ea_psb", "/dev/serial/by-id/ea_el"],
    )

    assert port == "/dev/serial/by-id/ea_psb"


def test_resolve_serial_port_fails_for_multiple_candidates_without_selector():
    with pytest.raises(SystemExit, match="Multiple serial ports found"):
        resolve_serial_port(candidates=["/dev/ttyACM0", "/dev/ttyACM1"])


def test_resolve_connection_settings_prefers_cli_then_env_then_defaults(monkeypatch):
    monkeypatch.setenv("EA_DRIVER_TRANSPORT", "lan-scpi")
    monkeypatch.setenv("EA_DRIVER_HOST", "192.168.0.50")
    args = argparse.Namespace(
        transport=None,
        serial_port="/dev/ttyUSB0",
        serial_glob=None,
        host=None,
        lan_scpi_port=None,
        lan_modbus_port=None,
        baudrate=None,
        unit_id=None,
        timeout_s=None,
    )

    settings = resolve_connection_settings(
        defaults=ConnectionSettings(transport="usb-modbus", serial_glob="*PSB*"),
        args=args,
        env_prefixes=("EA_PSB_EXAMPLE", "EA_DRIVER"),
    )

    assert settings.transport == "lan-scpi"
    assert settings.host == "192.168.0.50"
    assert settings.serial_port == "/dev/ttyUSB0"
    assert settings.serial_glob == "*PSB*"


def test_deep_merge_dicts_merges_nested_mappings():
    merged = deep_merge_dicts(
        {
            "connection_config": {"transport": "usb-scpi", "timeout_s": 1.0},
            "test_config": {"cycle_count": 1, "sample_interval_s": 1.0},
        },
        {
            "connection_config": {"timeout_s": 2.0},
            "test_config": {"cycle_count": 3},
        },
    )

    assert merged == {
        "connection_config": {"transport": "usb-scpi", "timeout_s": 2.0},
        "test_config": {"cycle_count": 3, "sample_interval_s": 1.0},
    }
