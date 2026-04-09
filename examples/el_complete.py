from __future__ import annotations

"""
Simple EA-EL 9080-60 DT example.

How to run:

    uv sync
    uv run python examples/el_complete.py

Or with pip:

    pip install -e .
    python examples/el_complete.py

Edit the configuration block below for your USB serial path or Ethernet host.
Use ``TRANSPORT = "usb-modbus"`` if you want an explicit Kelvin / remote-sensing check.
"""

import csv
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from ea_driver import EAEL9080_60DT

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
LOGGER = logging.getLogger("ea_driver.examples.el_complete")

# Connection configuration.
TRANSPORT = "usb-modbus"
# For Ethernet SCPI instead:
# TRANSPORT = "lan-scpi"
SERIAL_PORT = "/dev/serial/by-id/usb-EA_Elektro-Automatik_GmbH___Co._KG_EL_9080-60_DT_2228100002-if00"
HOST = "192.168.0.42"
LAN_SCPI_PORT = 5025
BAUDRATE = 115200
MODBUS_UNIT_ID = 0
TIMEOUT_S = 1.0

# Test profile.
MODE = "cc"  # "cc", "cp", or "cr"
# Example alternatives:
# MODE = "cp"
# MODE = "cr"
SETPOINT = 1.0
SAMPLES = 20
INTERVAL_S = 0.5
REMOTE_SETTLE_S = 0.3
ENABLE_SETTLE_S = 0.5
REQUIRE_REMOTE_SENSING = True
CSV_PATH = Path("el_samples.csv")


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def build_device():
    if TRANSPORT == "usb-modbus":
        return EAEL9080_60DT.modbus_rtu(
            SERIAL_PORT,
            baudrate=BAUDRATE,
            unit_id=MODBUS_UNIT_ID,
            timeout=TIMEOUT_S,
        )
    if TRANSPORT == "usb-scpi":
        return EAEL9080_60DT.scpi_serial(SERIAL_PORT, baudrate=BAUDRATE, timeout=TIMEOUT_S)
    if TRANSPORT == "lan-scpi":
        return EAEL9080_60DT.scpi_tcp(HOST, port=LAN_SCPI_PORT, timeout=TIMEOUT_S)
    raise ValueError(f"Unsupported transport: {TRANSPORT}")


def apply_mode(device) -> None:
    if MODE == "cc":
        device.set_current(SETPOINT)
        return
    if MODE == "cp":
        device.set_power(SETPOINT)
        return
    if MODE == "cr":
        device.set_resistance(SETPOINT)
        return
    raise ValueError(f"Unsupported EL mode: {MODE}")


def timestamp_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def run() -> None:
    device = build_device()
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    with CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
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
                if REQUIRE_REMOTE_SENSING and not initial_status.remote_sensing:
                    raise RuntimeError("Kelvin / remote sensing is not active on the load")
                if not initial_status.remote_sensing:
                    LOGGER.warning(
                        "Kelvin / remote sensing is not active on the load. Enable 4-wire sensing on the instrument first."
                    )
            else:
                LOGGER.info("Connected to %s", device.identify())
                device.clear_status()
                if REQUIRE_REMOTE_SENSING:
                    LOGGER.warning(
                        "SCPI control works over USB and Ethernet, but the explicit Kelvin / remote-sensing status bit "
                        "is currently only exposed through the Modbus path."
                    )

            try:
                device.set_remote(True)
                time.sleep(REMOTE_SETTLE_S)

                apply_mode(device)
                device.set_input_enabled(True)
                time.sleep(ENABLE_SETTLE_S)

                for sample_index in range(1, SAMPLES + 1):
                    measurement = device.read_measurements() if is_modbus else device.measure_all()
                    status = device.read_status() if is_modbus else None
                    writer.writerow(
                        {
                            "timestamp_utc": timestamp_utc(),
                            "transport": TRANSPORT,
                            "sample_index": sample_index,
                            "mode": MODE,
                            "setpoint": SETPOINT,
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
                            SAMPLES,
                            measurement.voltage_v,
                            measurement.current_a,
                            measurement.power_w,
                            device.is_input_enabled(),
                        )
                    else:
                        LOGGER.info(
                            "Sample %d/%d: %.3f V, %.3f A, %.3f W, mode=%s, kelvin=%s, alarms=%s",
                            sample_index,
                            SAMPLES,
                            measurement.voltage_v,
                            measurement.current_a,
                            measurement.power_w,
                            status.regulation_mode,
                            status.remote_sensing,
                            status.alarms_active,
                        )
                    if sample_index < SAMPLES:
                        time.sleep(INTERVAL_S)
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

    LOGGER.info("Wrote CSV log to %s", CSV_PATH)


if __name__ == "__main__":
    configure_logging()
    run()
