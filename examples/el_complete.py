from __future__ import annotations

"""
Very simple EA-EL 9080-60 DT discharge example.

Edit the constants below, then run:

    uv sync
    uv run python examples/el_complete.py

The script asks for the battery serial number before it starts, performs a
constant-current discharge, and writes a CSV log to `logging/`.
"""

import csv
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from ea_driver import EAEL9080_60DT
from ea_driver.config import ConnectionSettings, build_device_connection, format_connection

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
LOGGER = logging.getLogger("ea_driver.examples.el_complete")

CSV_FIELDS = [
    "timestamp",
    "battery_serial",
    "sample_index",
    "elapsed_s",
    "command_mode",
    "command_value",
    "cutoff_voltage_v",
    "below_cutoff_samples",
    "stop_reason",
    "voltage_v",
    "current_a",
    "power_w",
    "discharged_ah",
    "discharged_wh",
    "input_enabled",
    "remote_sensing",
    "alarms_active",
]


@dataclass(frozen=True, slots=True)
class SimpleDischargeConfig:
    connection: ConnectionSettings
    current_a: float = 5.0
    cutoff_voltage_v: float = 18.0
    sample_interval_s: float = 1.0
    remote_settle_s: float = 0.3
    enable_settle_s: float = 0.5
    cutoff_confirm_samples: int = 3
    log_directory: Path = Path("logging")


DEFAULT_CONFIG = SimpleDischargeConfig(
    connection=ConnectionSettings(
        transport="usb-modbus",
        serial_glob="*EL_9080-60_DT*",
        host="192.168.0.42",
        lan_scpi_port=5025,
        baudrate=115200,
        unit_id=0,
        timeout_s=1.0,
    )
)


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def prompt_battery_serial() -> str:
    while True:
        battery_serial = input("Battery serial number: ").strip()
        if battery_serial:
            return battery_serial
        print("Please enter a non-empty battery serial number.")


def sanitize_filename_component(value: str) -> str:
    cleaned = "".join(character if character.isalnum() or character in "-_." else "-" for character in value.strip())
    return cleaned.strip("-_.") or "unknown-battery"


def build_log_path(config: SimpleDischargeConfig, battery_serial: str, now: datetime | None = None) -> Path:
    timestamp = (now or datetime.now()).strftime("%Y%m%d_%H%M%S")
    safe_serial = sanitize_filename_component(battery_serial)
    return config.log_directory / f"{timestamp}_{safe_serial}_el_cc_discharge.csv"


def timestamp_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def build_device(config: SimpleDischargeConfig):
    return build_device_connection(EAEL9080_60DT, config.connection)


def read_measurement_and_status(device):
    is_modbus = hasattr(device, "read_status")
    measurement = device.read_measurements() if is_modbus else device.measure_all()
    status = device.read_status() if is_modbus else None
    input_enabled = status.dc_on if status else device.is_input_enabled()
    return measurement, status, input_enabled


def log_device_connection(device) -> None:
    if hasattr(device, "read_status"):
        LOGGER.info("Nominals: %s", device.read_nominals())
        initial_status = device.read_status()
        LOGGER.info("Initial status: %s", initial_status)
        if not initial_status.remote_sensing:
            LOGGER.warning("Kelvin / remote sensing is not active on the EL.")
        return

    LOGGER.info("Connected to %s", device.identify())
    device.clear_status()


def run(config: SimpleDischargeConfig, battery_serial: str) -> Path:
    log_path = build_log_path(config, battery_serial)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    device = build_device(config)
    LOGGER.info("Using %s", format_connection(config.connection))
    LOGGER.info("Battery serial: %s", battery_serial)

    with log_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()

        with device:
            log_device_connection(device)

            start_monotonic = time.monotonic()
            last_sample_monotonic: float | None = None
            discharged_ah = 0.0
            discharged_wh = 0.0
            min_voltage_v = float("inf")

            try:
                device.set_remote(True)
                time.sleep(config.remote_settle_s)

                device.set_current(config.current_a)
                device.set_input_enabled(True)
                time.sleep(config.enable_settle_s)

                sample_index = 0
                below_cutoff_samples = 0

                while True:
                    sample_index += 1
                    measurement, status, input_enabled = read_measurement_and_status(device)
                    sample_monotonic = time.monotonic()
                    elapsed_s = sample_monotonic - start_monotonic

                    if last_sample_monotonic is not None:
                        dt_s = sample_monotonic - last_sample_monotonic
                        if input_enabled:
                            discharged_ah += max(measurement.current_a, 0.0) * dt_s / 3600.0
                            discharged_wh += max(measurement.power_w, 0.0) * dt_s / 3600.0
                    last_sample_monotonic = sample_monotonic

                    min_voltage_v = min(min_voltage_v, measurement.voltage_v)
                    if measurement.voltage_v <= config.cutoff_voltage_v:
                        below_cutoff_samples += 1
                    else:
                        below_cutoff_samples = 0

                    stop_reason = ""
                    if below_cutoff_samples >= config.cutoff_confirm_samples:
                        stop_reason = (
                            f"voltage <= {config.cutoff_voltage_v:.3f} V "
                            f"for {config.cutoff_confirm_samples} samples"
                        )

                    writer.writerow(
                        {
                            "timestamp": timestamp_now(),
                            "battery_serial": battery_serial,
                            "sample_index": sample_index,
                            "elapsed_s": f"{elapsed_s:.3f}",
                            "command_mode": "cc",
                            "command_value": config.current_a,
                            "cutoff_voltage_v": config.cutoff_voltage_v,
                            "below_cutoff_samples": below_cutoff_samples,
                            "stop_reason": stop_reason,
                            "voltage_v": measurement.voltage_v,
                            "current_a": measurement.current_a,
                            "power_w": measurement.power_w,
                            "discharged_ah": discharged_ah,
                            "discharged_wh": discharged_wh,
                            "input_enabled": input_enabled,
                            "remote_sensing": status.remote_sensing if status else "",
                            "alarms_active": status.alarms_active if status else "",
                        }
                    )
                    handle.flush()

                    LOGGER.info(
                        "Sample %d: %.3f V, %.3f A, %.3f W",
                        sample_index,
                        measurement.voltage_v,
                        measurement.current_a,
                        measurement.power_w,
                    )

                    if stop_reason:
                        LOGGER.info("Stopping discharge: %s", stop_reason)
                        break

                    time.sleep(config.sample_interval_s)
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

    lowest_voltage = "n/a" if min_voltage_v == float("inf") else f"{min_voltage_v:.3f} V"
    total_elapsed_s = time.monotonic() - start_monotonic
    LOGGER.info(
        "Finished after %.1f s. Lowest voltage: %s. Discharged %.4f Ah / %.4f Wh.",
        total_elapsed_s,
        lowest_voltage,
        discharged_ah,
        discharged_wh,
    )
    LOGGER.info("Wrote CSV log to %s", log_path)
    return log_path


def main() -> None:
    configure_logging()
    battery_serial = prompt_battery_serial()
    run(DEFAULT_CONFIG, battery_serial)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        LOGGER.info("Stopped by user.")
