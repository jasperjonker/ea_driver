from __future__ import annotations

"""
Simple EA PSB 10060-60 example.

How to run:

    uv sync
    uv run python examples/psb_complete.py

You can override the connection and the test profile on the command line instead
of editing this file, for example:

    uv run python examples/psb_complete.py --transport lan-scpi --host 192.168.0.50 --mode sink

Connection defaults can also come from `EA_PSB_EXAMPLE_*` or `EA_DRIVER_*`
environment variables, such as `EA_DRIVER_SERIAL_PORT` or `EA_PSB_EXAMPLE_HOST`.
Use `--print-config` to inspect the merged configuration without talking to the device.
"""

import argparse
import csv
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from ea_driver import EAPSB10060_60
from ea_driver.config import (
    ConnectionSettings,
    add_connection_arguments,
    build_device_connection,
    format_connection,
    resolve_connection_settings,
)

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
LOGGER = logging.getLogger("ea_driver.examples.psb_complete")
ENV_PREFIXES = ("EA_PSB_EXAMPLE", "EA_DRIVER")


@dataclass(frozen=True, slots=True)
class PSBExampleConfig:
    connection: ConnectionSettings
    mode: str = "source"
    voltage_v: float = 24.0
    current_a: float = 5.0
    power_limit_w: float | None = 120.0
    resistance_ohm: float | None = None
    samples: int = 20
    interval_s: float = 0.5
    remote_settle_s: float = 0.3
    enable_settle_s: float = 0.5
    require_remote_sensing: bool = True
    csv_path: Path = Path("psb_samples.csv")


DEFAULT_CONNECTION = ConnectionSettings(
    transport="usb-modbus",
    serial_glob="*PSB_10060-60*",
    host="192.168.0.50",
    lan_scpi_port=5025,
    lan_modbus_port=502,
    baudrate=115200,
    unit_id=0,
    timeout_s=1.0,
)
DEFAULT_CONFIG = PSBExampleConfig(connection=DEFAULT_CONNECTION)


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a short EA PSB 10060-60 example.")
    add_connection_arguments(parser, transport_choices=("usb-modbus", "usb-scpi", "lan-scpi", "lan-modbus"))
    parser.add_argument("--mode", choices=("source", "sink"), default=DEFAULT_CONFIG.mode)
    parser.add_argument("--voltage-v", type=float, default=DEFAULT_CONFIG.voltage_v)
    parser.add_argument("--current-a", type=float, default=DEFAULT_CONFIG.current_a)
    parser.add_argument("--power-limit-w", type=float, default=DEFAULT_CONFIG.power_limit_w)
    parser.add_argument("--resistance-ohm", type=float, default=DEFAULT_CONFIG.resistance_ohm)
    parser.add_argument("--samples", type=int, default=DEFAULT_CONFIG.samples)
    parser.add_argument("--interval-s", type=float, default=DEFAULT_CONFIG.interval_s)
    parser.add_argument("--remote-settle-s", type=float, default=DEFAULT_CONFIG.remote_settle_s)
    parser.add_argument("--enable-settle-s", type=float, default=DEFAULT_CONFIG.enable_settle_s)
    parser.add_argument(
        "--require-remote-sensing",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_CONFIG.require_remote_sensing,
        help="Require the Kelvin / remote-sensing status bit when running through Modbus.",
    )
    parser.add_argument("--csv-path", type=Path, default=DEFAULT_CONFIG.csv_path)
    parser.add_argument(
        "--print-config",
        action="store_true",
        help="Print the merged configuration and exit without touching the device.",
    )
    return parser


def load_config(argv: list[str] | None = None) -> tuple[PSBExampleConfig, bool]:
    args = build_parser().parse_args(argv)
    connection = resolve_connection_settings(
        defaults=DEFAULT_CONNECTION,
        args=args,
        env_prefixes=ENV_PREFIXES,
    )
    config = PSBExampleConfig(
        connection=connection,
        mode=args.mode,
        voltage_v=args.voltage_v,
        current_a=args.current_a,
        power_limit_w=args.power_limit_w,
        resistance_ohm=args.resistance_ohm,
        samples=args.samples,
        interval_s=args.interval_s,
        remote_settle_s=args.remote_settle_s,
        enable_settle_s=args.enable_settle_s,
        require_remote_sensing=args.require_remote_sensing,
        csv_path=args.csv_path,
    )
    return config, args.print_config


def build_device(config: PSBExampleConfig):
    return build_device_connection(EAPSB10060_60, config.connection)


def apply_mode(device, config: PSBExampleConfig) -> None:
    if config.mode == "source":
        device.set_source_only_mode()
        device.set_source_current(config.current_a)
        return
    if config.mode == "sink":
        device.set_sink_only_mode()
        device.set_sink_current(config.current_a)
        return
    raise ValueError(f"Unsupported PSB mode: {config.mode}")


def timestamp_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(config: PSBExampleConfig) -> None:
    device = build_device(config)
    config.csv_path.parent.mkdir(parents=True, exist_ok=True)
    LOGGER.info("Using %s", format_connection(config.connection))

    with config.csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "timestamp_utc",
                "transport",
                "sample_index",
                "mode",
                "command_voltage_v",
                "command_current_a",
                "power_limit_w",
                "resistance_ohm",
                "voltage_v",
                "current_a",
                "power_w",
                "output_enabled",
                "remote",
                "control_location",
                "regulation_mode",
                "operation_mode",
                "remote_sensing",
                "alarms_active",
                "remote_owner",
            ],
        )
        writer.writeheader()

        with device:
            is_modbus = hasattr(device, "read_status")
            if is_modbus:
                LOGGER.info("Nominals: %s", device.read_nominals())
                LOGGER.info("Protection thresholds: %s", device.read_protection_thresholds())
                initial_status = device.read_status()
                LOGGER.info("Initial status: %s", initial_status)
                if config.require_remote_sensing and not initial_status.remote_sensing:
                    raise RuntimeError("Kelvin / remote sensing is not active on the PSB")
                if not initial_status.remote_sensing:
                    LOGGER.warning(
                        "Kelvin / remote sensing is not active on the PSB. Wire the rear Sense connector to the load "
                        "or source; the PSB detects it automatically."
                    )
                if config.mode != "source":
                    LOGGER.warning(
                        "EA documents remote sensing as effective only in constant-voltage operation; "
                        "in sink mode this check only confirms that the sense wiring is detected."
                    )
            else:
                LOGGER.info("Connected to %s", device.identify())
                device.clear_status()
                LOGGER.info("Initial remote owner: %s", device.remote_owner())
                if config.require_remote_sensing:
                    LOGGER.warning(
                        "SCPI control works over USB and Ethernet, but the explicit Kelvin / remote-sensing status bit "
                        "is currently only exposed through the Modbus path."
                    )
                if config.mode != "source":
                    LOGGER.warning("EA documents remote sensing as effective only in constant-voltage operation.")

            try:
                device.set_remote(True)
                time.sleep(config.remote_settle_s)

                device.set_voltage(config.voltage_v)
                apply_mode(device, config)
                if config.power_limit_w is not None:
                    if config.mode == "source":
                        device.set_source_power(config.power_limit_w)
                    else:
                        device.set_sink_power(config.power_limit_w)
                if config.resistance_ohm is not None:
                    device.set_resistance_mode_enabled(True)
                    if config.mode == "source":
                        device.set_source_resistance(config.resistance_ohm)
                    else:
                        device.set_sink_resistance(config.resistance_ohm)
                else:
                    device.set_resistance_mode_enabled(False)

                device.set_output_enabled(True)
                time.sleep(config.enable_settle_s)

                for sample_index in range(1, config.samples + 1):
                    measurement = device.read_measurements() if is_modbus else device.measure_all()
                    status = device.read_status() if is_modbus else None
                    output_enabled = status.dc_on if status else device.is_output_enabled()
                    remote_owner = "" if status else device.remote_owner()
                    writer.writerow(
                        {
                            "timestamp_utc": timestamp_utc(),
                            "transport": config.connection.transport,
                            "sample_index": sample_index,
                            "mode": config.mode,
                            "command_voltage_v": config.voltage_v,
                            "command_current_a": config.current_a,
                            "power_limit_w": config.power_limit_w if config.power_limit_w is not None else "",
                            "resistance_ohm": config.resistance_ohm if config.resistance_ohm is not None else "",
                            "voltage_v": measurement.voltage_v,
                            "current_a": measurement.current_a,
                            "power_w": measurement.power_w,
                            "output_enabled": output_enabled,
                            "remote": status.remote if status else "",
                            "control_location": status.control_location if status else "",
                            "regulation_mode": status.regulation_mode if status else "",
                            "operation_mode": status.operation_mode if status else "",
                            "remote_sensing": status.remote_sensing if status else "",
                            "alarms_active": status.alarms_active if status else "",
                            "remote_owner": remote_owner,
                        }
                    )
                    if status is None:
                        LOGGER.info(
                            "Sample %d/%d: %.3f V, %.3f A, %.3f W, output_enabled=%s, owner=%s",
                            sample_index,
                            config.samples,
                            measurement.voltage_v,
                            measurement.current_a,
                            measurement.power_w,
                            output_enabled,
                            remote_owner,
                        )
                    else:
                        LOGGER.info(
                            "Sample %d/%d: %.3f V, %.3f A, %.3f W, mode=%s, operation=%s, kelvin=%s, alarms=%s",
                            sample_index,
                            config.samples,
                            measurement.voltage_v,
                            measurement.current_a,
                            measurement.power_w,
                            status.regulation_mode,
                            status.operation_mode,
                            status.remote_sensing,
                            status.alarms_active,
                        )
                    if sample_index < config.samples:
                        time.sleep(config.interval_s)
            finally:
                try:
                    device.set_output_enabled(False)
                except Exception:
                    LOGGER.exception("Failed to disable PSB output during cleanup")
                try:
                    device.set_remote(False)
                except Exception:
                    LOGGER.exception("Failed to release PSB remote lock during cleanup")
                if hasattr(device, "read_errors"):
                    errors = device.read_errors()
                    if errors and any(not error.startswith("0,") for error in errors):
                        LOGGER.warning("SCPI error queue: %s", errors)

    LOGGER.info("Wrote CSV log to %s", config.csv_path)


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    config, print_config = load_config(argv)
    if print_config:
        print(json.dumps(asdict(config), indent=2, sort_keys=True, default=str))
        return
    run(config)


if __name__ == "__main__":
    main()
