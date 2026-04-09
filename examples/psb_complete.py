from __future__ import annotations

"""
Simple EA PSB 10060-60 example.

How to run:

    uv sync
    uv run python examples/psb_complete.py

Or with pip:

    pip install -e .
    python examples/psb_complete.py

Edit the configuration block below for your USB serial path or Ethernet host.
The same script can demonstrate sourcing or sinking current and stores the sampled data in CSV.
"""

import csv
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from ea_driver import EAPSB10060_60

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
LOGGER = logging.getLogger("ea_driver.examples.psb_complete")

# Connection configuration.
TRANSPORT = "usb-scpi"
# For Ethernet SCPI instead:
# TRANSPORT = "lan-scpi"
SERIAL_PORT = "/dev/serial/by-id/usb-EA_Elektro-Automatik_GmbH___Co._KG_PSB_10060-60_2538170001-if00"
HOST = "192.168.0.50"
LAN_SCPI_PORT = 5025
BAUDRATE = 115200
TIMEOUT_S = 1.0

# Test profile.
MODE = "source"  # "source" or "sink"
# Example alternative:
# MODE = "sink"
VOLTAGE_V = 24.0
CURRENT_A = 5.0
POWER_LIMIT_W = 120.0
RESISTANCE_OHM = None
SAMPLES = 20
INTERVAL_S = 0.5
REMOTE_SETTLE_S = 0.3
ENABLE_SETTLE_S = 0.5
CSV_PATH = Path("psb_samples.csv")


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def build_device():
    if TRANSPORT == "usb-scpi":
        return EAPSB10060_60.scpi_serial(SERIAL_PORT, baudrate=BAUDRATE, timeout=TIMEOUT_S)
    if TRANSPORT == "lan-scpi":
        return EAPSB10060_60.scpi_tcp(HOST, port=LAN_SCPI_PORT, timeout=TIMEOUT_S)
    raise ValueError(f"Unsupported transport: {TRANSPORT}")


def apply_mode(device) -> None:
    if MODE == "source":
        device.set_source_only_mode()
        device.set_source_current(CURRENT_A)
        return
    if MODE == "sink":
        device.set_sink_only_mode()
        device.set_sink_current(CURRENT_A)
        return
    raise ValueError(f"Unsupported PSB mode: {MODE}")


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
                "command_voltage_v",
                "command_current_a",
                "power_limit_w",
                "resistance_ohm",
                "voltage_v",
                "current_a",
                "power_w",
                "output_enabled",
                "remote_owner",
            ],
        )
        writer.writeheader()

        with device:
            LOGGER.info("Connected to %s", device.identify())
            device.clear_status()
            LOGGER.info("Initial remote owner: %s", device.remote_owner())

            try:
                device.set_remote(True)
                time.sleep(REMOTE_SETTLE_S)

                device.set_voltage(VOLTAGE_V)
                apply_mode(device)
                if POWER_LIMIT_W is not None:
                    if MODE == "source":
                        device.set_source_power(POWER_LIMIT_W)
                    else:
                        device.set_sink_power(POWER_LIMIT_W)
                if RESISTANCE_OHM is not None:
                    device.set_resistance_mode_enabled(True)
                    if MODE == "source":
                        device.set_source_resistance(RESISTANCE_OHM)
                    else:
                        device.set_sink_resistance(RESISTANCE_OHM)
                else:
                    device.set_resistance_mode_enabled(False)

                device.set_output_enabled(True)
                time.sleep(ENABLE_SETTLE_S)

                for sample_index in range(1, SAMPLES + 1):
                    measurement = device.measure_all()
                    output_enabled = device.is_output_enabled()
                    remote_owner = device.remote_owner()
                    writer.writerow(
                        {
                            "timestamp_utc": timestamp_utc(),
                            "transport": TRANSPORT,
                            "sample_index": sample_index,
                            "mode": MODE,
                            "command_voltage_v": VOLTAGE_V,
                            "command_current_a": CURRENT_A,
                            "power_limit_w": POWER_LIMIT_W if POWER_LIMIT_W is not None else "",
                            "resistance_ohm": RESISTANCE_OHM if RESISTANCE_OHM is not None else "",
                            "voltage_v": measurement.voltage_v,
                            "current_a": measurement.current_a,
                            "power_w": measurement.power_w,
                            "output_enabled": output_enabled,
                            "remote_owner": remote_owner,
                        }
                    )
                    LOGGER.info(
                        "Sample %d/%d: %.3f V, %.3f A, %.3f W, output_enabled=%s, owner=%s",
                        sample_index,
                        SAMPLES,
                        measurement.voltage_v,
                        measurement.current_a,
                        measurement.power_w,
                        output_enabled,
                        remote_owner,
                    )
                    if sample_index < SAMPLES:
                        time.sleep(INTERVAL_S)
            finally:
                try:
                    device.set_output_enabled(False)
                except Exception:
                    LOGGER.exception("Failed to disable PSB output during cleanup")
                try:
                    device.set_remote(False)
                except Exception:
                    LOGGER.exception("Failed to release PSB remote lock during cleanup")
                errors = device.read_errors()
                if errors and any(not error.startswith("0,") for error in errors):
                    LOGGER.warning("SCPI error queue: %s", errors)

    LOGGER.info("Wrote CSV log to %s", CSV_PATH)


if __name__ == "__main__":
    configure_logging()
    run()
