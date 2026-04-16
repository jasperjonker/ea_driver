from __future__ import annotations

"""
Simple EA-EL 9080-60 DT example.

How to run:

    uv sync
    uv run python examples/el_complete.py

You can override the connection and the test profile on the command line instead
of editing this file, for example:

    uv run python examples/el_complete.py --transport lan-scpi --host 192.168.0.42

Connection defaults can also come from `EA_EL_EXAMPLE_*` or `EA_DRIVER_*`
environment variables, such as `EA_DRIVER_SERIAL_PORT` or `EA_EL_EXAMPLE_HOST`.
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

from ea_driver import EAEL9080_60DT
from ea_driver.config import (
    ConnectionSettings,
    add_connection_arguments,
    build_device_connection,
    format_connection,
    resolve_connection_settings,
)

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
LOGGER = logging.getLogger("ea_driver.examples.el_complete")
ENV_PREFIXES = ("EA_EL_EXAMPLE", "EA_DRIVER")


@dataclass(frozen=True, slots=True)
class ELExampleConfig:
    connection: ConnectionSettings
    mode: str = "cc"
    setpoint: float = 1.0
    samples: int = 20
    interval_s: float = 0.5
    remote_settle_s: float = 0.3
    enable_settle_s: float = 0.5
    require_remote_sensing: bool = True
    csv_path: Path = Path("el_samples.csv")


DEFAULT_CONNECTION = ConnectionSettings(
    transport="usb-modbus",
    serial_glob="*EL_9080-60_DT*",
    host="192.168.0.42",
    lan_scpi_port=5025,
    baudrate=115200,
    unit_id=0,
    timeout_s=1.0,
)
DEFAULT_CONFIG = ELExampleConfig(connection=DEFAULT_CONNECTION)


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a short EA-EL 9080-60 DT example.")
    add_connection_arguments(parser, transport_choices=("usb-modbus", "usb-scpi", "lan-scpi"))
    parser.add_argument("--mode", choices=("cc", "cp", "cr"), default=DEFAULT_CONFIG.mode)
    parser.add_argument("--setpoint", type=float, default=DEFAULT_CONFIG.setpoint)
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


def load_config(argv: list[str] | None = None) -> tuple[ELExampleConfig, bool]:
    args = build_parser().parse_args(argv)
    connection = resolve_connection_settings(
        defaults=DEFAULT_CONNECTION,
        args=args,
        env_prefixes=ENV_PREFIXES,
    )
    config = ELExampleConfig(
        connection=connection,
        mode=args.mode,
        setpoint=args.setpoint,
        samples=args.samples,
        interval_s=args.interval_s,
        remote_settle_s=args.remote_settle_s,
        enable_settle_s=args.enable_settle_s,
        require_remote_sensing=args.require_remote_sensing,
        csv_path=args.csv_path,
    )
    return config, args.print_config


def build_device(config: ELExampleConfig):
    return build_device_connection(EAEL9080_60DT, config.connection)


def apply_mode(device, config: ELExampleConfig) -> None:
    if config.mode == "cc":
        device.set_current(config.setpoint)
        return
    if config.mode == "cp":
        device.set_power(config.setpoint)
        return
    if config.mode == "cr":
        device.set_resistance(config.setpoint)
        return
    raise ValueError(f"Unsupported EL mode: {config.mode}")


def timestamp_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(config: ELExampleConfig) -> None:
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
                "setpoint",
                "voltage_v",
                "current_a",
                "power_w",
                "dc_on",
                "remote",
                "regulation_mode",
                "remote_sensing",
                "alarms_active",
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
                    raise RuntimeError("Kelvin / remote sensing is not active on the load")
                if not initial_status.remote_sensing:
                    LOGGER.warning(
                        "Kelvin / remote sensing is not active on the load. Enable 4-wire sensing on the instrument first."
                    )
            else:
                LOGGER.info("Connected to %s", device.identify())
                device.clear_status()
                if config.require_remote_sensing:
                    LOGGER.warning(
                        "SCPI control works over USB and Ethernet, but the explicit Kelvin / remote-sensing status bit "
                        "is currently only exposed through the Modbus path."
                    )

            try:
                device.set_remote(True)
                time.sleep(config.remote_settle_s)

                apply_mode(device, config)
                device.set_input_enabled(True)
                time.sleep(config.enable_settle_s)

                for sample_index in range(1, config.samples + 1):
                    measurement = device.read_measurements() if is_modbus else device.measure_all()
                    status = device.read_status() if is_modbus else None
                    writer.writerow(
                        {
                            "timestamp_utc": timestamp_utc(),
                            "transport": config.connection.transport,
                            "sample_index": sample_index,
                            "mode": config.mode,
                            "setpoint": config.setpoint,
                            "voltage_v": measurement.voltage_v,
                            "current_a": measurement.current_a,
                            "power_w": measurement.power_w,
                            "dc_on": status.dc_on if status else device.is_input_enabled(),
                            "remote": status.remote if status else "",
                            "regulation_mode": status.regulation_mode if status else "",
                            "remote_sensing": status.remote_sensing if status else "",
                            "alarms_active": status.alarms_active if status else "",
                        }
                    )
                    if status is None:
                        LOGGER.info(
                            "Sample %d/%d: %.3f V, %.3f A, %.3f W, input_enabled=%s",
                            sample_index,
                            config.samples,
                            measurement.voltage_v,
                            measurement.current_a,
                            measurement.power_w,
                            device.is_input_enabled(),
                        )
                    else:
                        LOGGER.info(
                            "Sample %d/%d: %.3f V, %.3f A, %.3f W, mode=%s, kelvin=%s, alarms=%s",
                            sample_index,
                            config.samples,
                            measurement.voltage_v,
                            measurement.current_a,
                            measurement.power_w,
                            status.regulation_mode,
                            status.remote_sensing,
                            status.alarms_active,
                        )
                    if sample_index < config.samples:
                        time.sleep(config.interval_s)
            finally:
                try:
                    device.set_input_enabled(False)
                except Exception:
                    LOGGER.exception("Failed to disable EL input during cleanup")
                try:
                    device.set_remote(False)
                except Exception:
                    LOGGER.exception("Failed to release EL remote lock during cleanup")
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
