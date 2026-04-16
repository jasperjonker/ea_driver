from __future__ import annotations

import argparse
import fnmatch
import glob
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_SERIAL_PORT_PATTERNS = (
    "/dev/serial/by-id/*",
    "/dev/ttyACM*",
    "/dev/ttyUSB*",
)


@dataclass(frozen=True, slots=True)
class ConnectionSettings:
    transport: str
    serial_port: str | None = None
    serial_glob: str | None = None
    host: str | None = None
    lan_scpi_port: int = 5025
    lan_modbus_port: int = 502
    baudrate: int = 115200
    unit_id: int = 0
    timeout_s: float = 1.0

    def resolved_serial_port(self) -> str:
        return resolve_serial_port(port=self.serial_port, serial_glob=self.serial_glob)

    def required_host(self) -> str:
        host = _normalize_optional_text(self.host)
        if host is None:
            raise SystemExit("A host is required for LAN transports. Pass --host or set an environment variable.")
        return host


def add_connection_arguments(
    parser: argparse.ArgumentParser,
    *,
    transport_choices: Sequence[str],
) -> None:
    parser.add_argument("--transport", choices=tuple(transport_choices), help="Transport to use.")
    parser.add_argument(
        "--serial-port",
        help="Serial device path. Use 'auto' or omit it to auto-discover a port.",
    )
    parser.add_argument(
        "--serial-glob",
        help="Glob matched against auto-discovered serial ports when --serial-port is omitted.",
    )
    parser.add_argument("--host", help="Instrument hostname or IP address for LAN transports.")
    parser.add_argument("--lan-scpi-port", type=int, help="TCP port for SCPI over LAN.")
    parser.add_argument("--lan-modbus-port", type=int, help="TCP port for Modbus over LAN.")
    parser.add_argument("--baudrate", type=int, help="Serial baudrate for USB transports.")
    parser.add_argument("--unit-id", type=int, help="Modbus unit id.")
    parser.add_argument("--timeout-s", type=float, help="Transport timeout in seconds.")


def discover_serial_ports(*, patterns: Sequence[str] = DEFAULT_SERIAL_PORT_PATTERNS) -> list[str]:
    candidates: list[str] = []
    seen_targets: set[str] = set()
    for pattern in patterns:
        for path in sorted(glob.glob(pattern)):
            canonical = os.path.realpath(path)
            if canonical in seen_targets:
                continue
            candidates.append(path)
            seen_targets.add(canonical)
    return candidates


def resolve_serial_port(
    port: str | None = None,
    *,
    serial_glob: str | None = None,
    patterns: Sequence[str] = DEFAULT_SERIAL_PORT_PATTERNS,
    candidates: Sequence[str] | None = None,
) -> str:
    explicit_port = _normalize_optional_text(port)
    if explicit_port is not None:
        return explicit_port

    discovered = list(candidates) if candidates is not None else discover_serial_ports(patterns=patterns)
    selector = _normalize_optional_text(serial_glob)
    if selector is not None:
        matches = [candidate for candidate in discovered if _serial_candidate_matches(candidate, selector)]
        if len(matches) == 1:
            return matches[0]
        if not matches:
            raise SystemExit(
                f"No serial ports matched {selector!r}. Pass --serial-port explicitly or connect the device."
            )
        joined = ", ".join(matches)
        raise SystemExit(
            f"Multiple serial ports matched {selector!r} ({joined}). Pass --serial-port to choose one."
        )

    if not discovered:
        raise SystemExit("No serial ports found. Pass --serial-port explicitly or connect the device.")
    if len(discovered) > 1:
        joined = ", ".join(discovered)
        raise SystemExit(
            f"Multiple serial ports found ({joined}). Pass --serial-port or --serial-glob to choose one."
        )
    return discovered[0]


def resolve_connection_settings(
    *,
    defaults: ConnectionSettings,
    args: argparse.Namespace,
    env_prefixes: Sequence[str] = (),
) -> ConnectionSettings:
    return ConnectionSettings(
        transport=_read_setting(args, env_prefixes, "transport", default=defaults.transport, value_type=str),
        serial_port=_read_setting(args, env_prefixes, "serial_port", default=defaults.serial_port, value_type=str),
        serial_glob=_read_setting(args, env_prefixes, "serial_glob", default=defaults.serial_glob, value_type=str),
        host=_read_setting(args, env_prefixes, "host", default=defaults.host, value_type=str),
        lan_scpi_port=_read_setting(
            args,
            env_prefixes,
            "lan_scpi_port",
            default=defaults.lan_scpi_port,
            value_type=int,
        ),
        lan_modbus_port=_read_setting(
            args,
            env_prefixes,
            "lan_modbus_port",
            default=defaults.lan_modbus_port,
            value_type=int,
        ),
        baudrate=_read_setting(args, env_prefixes, "baudrate", default=defaults.baudrate, value_type=int),
        unit_id=_read_setting(args, env_prefixes, "unit_id", default=defaults.unit_id, value_type=int),
        timeout_s=_read_setting(args, env_prefixes, "timeout_s", default=defaults.timeout_s, value_type=float),
    )


def build_device_connection(device_cls: type[Any], settings: ConnectionSettings) -> Any:
    if settings.transport == "usb-modbus":
        return device_cls.modbus_rtu(
            settings.resolved_serial_port(),
            baudrate=settings.baudrate,
            unit_id=settings.unit_id,
            timeout=settings.timeout_s,
        )
    if settings.transport == "usb-scpi":
        return device_cls.scpi_serial(
            settings.resolved_serial_port(),
            baudrate=settings.baudrate,
            timeout=settings.timeout_s,
        )
    if settings.transport == "lan-scpi":
        return device_cls.scpi_tcp(
            settings.required_host(),
            port=settings.lan_scpi_port,
            timeout=settings.timeout_s,
        )
    if settings.transport == "lan-modbus":
        return device_cls.modbus_tcp(
            settings.required_host(),
            port=settings.lan_modbus_port,
            unit_id=settings.unit_id,
            timeout=settings.timeout_s,
        )
    raise ValueError(f"Unsupported transport: {settings.transport}")


def format_connection(settings: ConnectionSettings) -> str:
    if settings.transport == "lan-scpi":
        return f"{settings.transport} host={settings.required_host()}:{settings.lan_scpi_port}"
    if settings.transport == "lan-modbus":
        return f"{settings.transport} host={settings.required_host()}:{settings.lan_modbus_port} unit_id={settings.unit_id}"

    port = settings.resolved_serial_port()
    return f"{settings.transport} port={port} baudrate={settings.baudrate}"


def deep_merge_dicts(base: Mapping[str, Any], overrides: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overrides.items():
        current = merged.get(key)
        if isinstance(current, Mapping) and isinstance(value, Mapping):
            merged[key] = deep_merge_dicts(current, value)
            continue
        merged[key] = value
    return merged


def _read_setting(
    args: argparse.Namespace,
    env_prefixes: Sequence[str],
    attribute: str,
    *,
    default: Any,
    value_type: type[Any],
) -> Any:
    cli_value = getattr(args, attribute, None)
    if cli_value is not None:
        return _coerce_value(cli_value, value_type)

    env_value = _read_environment_value(env_prefixes, attribute)
    if env_value is not None:
        return _coerce_value(env_value, value_type)

    return default


def _read_environment_value(prefixes: Sequence[str], attribute: str) -> str | None:
    key = attribute.upper()
    for prefix in prefixes:
        name = f"{prefix}_{key}".upper()
        value = os.environ.get(name)
        if value is not None:
            return value
    return None


def _coerce_value(value: Any, value_type: type[Any]) -> Any:
    if value_type is str:
        return _normalize_optional_text(str(value))
    return value_type(value)


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped or stripped.lower() == "auto":
        return None
    return stripped


def _serial_candidate_matches(candidate: str, pattern: str) -> bool:
    lowered_pattern = pattern.lower()
    path = Path(candidate)
    realpath = os.path.realpath(candidate)
    return any(
        (
            fnmatch.fnmatch(candidate.lower(), lowered_pattern),
            fnmatch.fnmatch(path.name.lower(), lowered_pattern),
            fnmatch.fnmatch(realpath.lower(), lowered_pattern),
            fnmatch.fnmatch(Path(realpath).name.lower(), lowered_pattern),
        )
    )
