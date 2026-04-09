from __future__ import annotations

import argparse
import glob
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ea_driver import EAEL9080_60DT, EAPSB10060_60
from ea_driver.core import InstrumentError, Measurement
from ea_driver.ea import EAStatus
from ea_driver.scpi import SCPIDevice, SerialSCPITransport

REMOTE_SETTLE_S = 0.3
VALUE_SETTLE_S = 0.1
OUTPUT_SETTLE_S = 0.5
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

logger = logging.getLogger("ea_driver.verify")


@dataclass(frozen=True, slots=True)
class DeviceSpec:
    key: str
    label: str
    idn_substring: str
    device_cls: type[Any]


SUPPORTED_DEVICES = (
    DeviceSpec(
        key="auto",
        label="Auto detect",
        idn_substring="",
        device_cls=object,
    ),
    DeviceSpec(
        key="el9080-60-dt",
        label="EA-EL 9080-60 DT",
        idn_substring="EL 9080-60 DT",
        device_cls=EAEL9080_60DT,
    ),
    DeviceSpec(
        key="psb10060-60",
        label="EA PSB 10060-60",
        idn_substring="PSB 10060-60",
        device_cls=EAPSB10060_60,
    ),
)

DEVICE_BY_KEY = {spec.key: spec for spec in SUPPORTED_DEVICES}


def discover_ports() -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    for pattern in ("/dev/serial/by-id/*", "/dev/ttyACM*", "/dev/ttyUSB*"):
        for path in sorted(glob.glob(pattern)):
            if path not in seen:
                candidates.append(path)
                seen.add(path)
    return candidates


def resolve_port(port: str | None) -> str:
    if port:
        return port
    candidates = discover_ports()
    if not candidates:
        raise SystemExit("No serial ports found. Expected a device on /dev/ttyACM* or /dev/ttyUSB*.")
    if len(candidates) > 1:
        formatted = ", ".join(candidates)
        raise SystemExit(f"Multiple serial ports found ({formatted}). Pass --port to choose one.")
    return candidates[0]


def format_measurement(measurement: Measurement) -> str:
    return (
        f"{measurement.voltage_v:.3f} V, "
        f"{measurement.current_a:.3f} A, "
        f"{measurement.power_w:.3f} W"
    )


def format_status(status: EAStatus) -> str:
    fields = [
        f"control_location={status.control_location}",
        f"dc_on={status.dc_on}",
        f"regulation_mode={status.regulation_mode}",
        f"remote={status.remote}",
        f"remote_sensing={status.remote_sensing}",
        f"alarms_active={status.alarms_active}",
        f"ovp_active={status.ovp_active}",
        f"ocp_active={status.ocp_active}",
        f"opp_active={status.opp_active}",
        f"over_temperature={status.over_temperature}",
        f"power_fail={status.power_fail}",
        f"rem_sb_inhibiting={status.rem_sb_inhibiting}",
    ]
    return ", ".join(fields)


def configure_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), None)
    if not isinstance(level, int):
        raise SystemExit(f"Invalid log level: {level_name}")
    logging.basicConfig(level=level, format=LOG_FORMAT)


def log_header(title: str) -> None:
    logger.info("== %s ==", title)


def parse_scpi_numeric(response: str) -> float:
    return float(response.strip().split()[0])


def parse_unit_id_selection(selection: str) -> list[int]:
    if selection == "auto":
        return [0, 1]
    unit_id = int(selection)
    if unit_id not in (0, 1):
        raise SystemExit("--unit-id must be 0, 1, or auto")
    return [unit_id]


def identify_device(port: str, baudrate: int, timeout: float) -> str:
    transport = SerialSCPITransport(port=port, baudrate=baudrate, timeout=timeout)
    device = SCPIDevice(transport)
    device.open()
    try:
        return device.identify()
    finally:
        device.close()


def resolve_device_spec(port: str, baudrate: int, timeout: float, device_key: str) -> tuple[DeviceSpec, str]:
    if device_key != "auto":
        spec = DEVICE_BY_KEY[device_key]
        idn = identify_device(port, baudrate, timeout)
        logger.info("SCPI identify: %s", idn)
        return spec, idn

    idn = identify_device(port, baudrate, timeout)
    logger.info("SCPI identify: %s", idn)
    for spec in SUPPORTED_DEVICES:
        if spec.key != "auto" and spec.idn_substring in idn:
            logger.info("Detected device model: %s", spec.label)
            return spec, idn
    raise RuntimeError(f"Unsupported device from *IDN?: {idn}")


def create_scpi_device(spec: DeviceSpec, port: str, baudrate: int, timeout: float) -> SCPIDevice:
    return spec.device_cls.scpi_serial(port, baudrate=baudrate, timeout=timeout)


def create_modbus_device(spec: DeviceSpec, port: str, baudrate: int, unit_id: int, timeout: float) -> Any:
    return spec.device_cls.modbus_rtu(port, baudrate=baudrate, unit_id=unit_id, timeout=timeout)


def summarize_scpi_errors(device: SCPIDevice, *, max_errors: int = 3) -> str:
    errors = [error for error in device.read_errors(max_errors=max_errors) if not error.startswith("0,")]
    if not errors:
        return "no SCPI errors reported"
    return "; ".join(errors)


def ensure_scpi_remote(device: SCPIDevice) -> None:
    owner = device.remote_owner()
    if owner.strip().upper() != "NONE":
        return
    errors = summarize_scpi_errors(device)
    raise RuntimeError(f"Remote control was not accepted by the device (owner={owner}, errors={errors})")


def ensure_output_enabled(device: SCPIDevice) -> None:
    if device.is_output_enabled():
        return
    errors = summarize_scpi_errors(device)
    raise RuntimeError(f"Output/input enable was not accepted by the device (errors={errors})")


def annotate_modbus_remote_error(exc: Exception) -> RuntimeError:
    text = str(exc)
    if "exception 7" in text:
        return RuntimeError(
            "Modbus remote-control request was rejected by the device. "
            "On EA 10000-series devices this usually means Local mode is active, "
            "remote control is disabled on the HMI, the setup menu is open, or another interface owns remote control."
        )
    return RuntimeError(text)


def resolve_modbus_unit_id(spec: DeviceSpec, port: str, baudrate: int, timeout: float, selection: str) -> int:
    errors: list[str] = []
    for unit_id in parse_unit_id_selection(selection):
        load = create_modbus_device(spec, port, baudrate, unit_id, timeout)
        load.open()
        try:
            load.read_status()
            logger.info("Selected Modbus unit id %d", unit_id)
            return unit_id
        except Exception as exc:
            errors.append(f"unit_id={unit_id}: {type(exc).__name__}: {exc}")
            logger.debug("Modbus unit id %d probe failed: %s: %s", unit_id, type(exc).__name__, exc)
        finally:
            load.close()
    joined = "; ".join(errors)
    raise RuntimeError(f"Unable to communicate over Modbus RTU with any candidate unit id ({joined})")


def probe_scpi(spec: DeviceSpec, port: str, baudrate: int, timeout: float) -> None:
    log_header("SCPI Probe")
    device = create_scpi_device(spec, port, baudrate, timeout)
    device.open()
    try:
        logger.info("Device: %s", spec.label)
        logger.info("IDN: %s", device.identify())
        logger.info("Lock owner: %s", device.remote_owner())
        logger.info("Output enabled: %s", device.is_output_enabled())
        logger.info("Configured current: %s", device.query("CURR?"))
        logger.info("Measurements: %s", format_measurement(device.measure_all()))
    finally:
        device.close()


def probe_modbus(
    spec: DeviceSpec,
    port: str,
    baudrate: int,
    unit_id: int,
    timeout: float,
    require_remote_sensing: bool,
) -> None:
    log_header("Modbus Probe")
    logger.info("Device: %s", spec.label)
    logger.info("Using Modbus unit id: %d", unit_id)
    device = create_modbus_device(spec, port, baudrate, unit_id, timeout)
    device.open()
    try:
        status = device.read_status()
        logger.info("Nominals: %s", device.read_nominals())
        logger.info("Status: %s", format_status(status))
        logger.info("Measurements: %s", format_measurement(device.read_measurements()))
        logger.info("Protection thresholds: %s", format_measurement(device.read_protection_thresholds()))
        if require_remote_sensing and not status.remote_sensing:
            raise RuntimeError("Remote sensing is not active on the device")
    finally:
        device.close()


def exercise_scpi(
    spec: DeviceSpec,
    port: str,
    baudrate: int,
    timeout: float,
    current_a: float,
    duration_s: float,
) -> None:
    if current_a <= 0:
        raise SystemExit("--exercise-current-a must be greater than 0")
    if duration_s <= 0:
        raise SystemExit("--exercise-duration-s must be greater than 0")

    log_header("SCPI Live Test")
    device = create_scpi_device(spec, port, baudrate, timeout)
    device.open()
    previous_current_a = None
    try:
        if device.is_output_enabled():
            raise SystemExit("The device output/input is already enabled. Turn it off before running --exercise-scpi.")

        baseline = device.measure_all()
        previous_current_a = parse_scpi_numeric(device.query("CURR?"))
        logger.info("Baseline: %s", format_measurement(baseline))
        logger.info("Arming remote control and setting %.3f A for %.2f s", current_a, duration_s)

        device.set_remote(True)
        time.sleep(REMOTE_SETTLE_S)
        ensure_scpi_remote(device)
        device.set_current(current_a)
        time.sleep(VALUE_SETTLE_S)
        logger.info("Configured current after write: %s", device.query("CURR?"))
        device.set_output_enabled(True)
        time.sleep(OUTPUT_SETTLE_S)
        ensure_output_enabled(device)
        time.sleep(duration_s)

        under_load = device.measure_all()
        logger.info("Under load: %s", format_measurement(under_load))
        logger.info("Lock owner: %s", device.remote_owner())
    finally:
        cleanup_errors: list[str] = []
        for label, action in (
            ("disable output", lambda: device.set_output_enabled(False)),
            (
                "restore current",
                (lambda: device.set_current(previous_current_a)) if previous_current_a is not None else (lambda: None),
            ),
            ("release remote", lambda: device.set_remote(False)),
            ("close transport", device.close),
        ):
            try:
                action()
            except Exception as exc:  # pragma: no cover - cleanup path
                cleanup_errors.append(f"{label}: {type(exc).__name__}: {exc}")
        for error in cleanup_errors:
            logger.warning("Cleanup issue: %s", error)


def exercise_modbus(
    spec: DeviceSpec,
    port: str,
    baudrate: int,
    unit_id: int,
    timeout: float,
    current_a: float,
    duration_s: float,
    require_remote_sensing: bool,
) -> None:
    if current_a <= 0:
        raise SystemExit("--exercise-current-a must be greater than 0")
    if duration_s <= 0:
        raise SystemExit("--exercise-duration-s must be greater than 0")

    log_header("Modbus Live Test")
    logger.info("Device: %s", spec.label)
    logger.info("Using Modbus unit id: %d", unit_id)
    scpi = create_scpi_device(spec, port, baudrate, timeout=1.0)
    device = create_modbus_device(spec, port, baudrate, unit_id, timeout)
    scpi.open()
    device.open()
    previous_current_a = None
    try:
        previous_current_a = parse_scpi_numeric(scpi.query("CURR?"))
        initial_status = device.read_status()
        if initial_status.dc_on:
            raise SystemExit("The device output/input is already enabled. Turn it off before running --exercise-modbus.")
        if require_remote_sensing and not initial_status.remote_sensing:
            raise RuntimeError("Remote sensing is not active on the device")

        baseline = device.read_measurements()
        logger.info("Baseline: %s", format_measurement(baseline))
        logger.info("Arming remote control and setting %.3f A for %.2f s", current_a, duration_s)

        try:
            device.set_remote(True)
        except InstrumentError as exc:
            raise annotate_modbus_remote_error(exc) from exc
        time.sleep(REMOTE_SETTLE_S)
        device.set_current(current_a)
        time.sleep(VALUE_SETTLE_S)
        logger.info("Configured current after write: %s", scpi.query("CURR?"))
        device.set_output_enabled(True)
        time.sleep(OUTPUT_SETTLE_S)
        ensure_output_enabled(scpi)
        time.sleep(duration_s)

        under_load = device.read_measurements()
        under_load_status = device.read_status()
        logger.info("Under load: %s", format_measurement(under_load))
        logger.info("Status under load: %s", format_status(under_load_status))
    finally:
        cleanup_errors: list[str] = []
        for label, action in (
            ("disable output", lambda: device.set_output_enabled(False)),
            (
                "restore current",
                (lambda: scpi.write(f"CURR {previous_current_a}")) if previous_current_a is not None else (lambda: None),
            ),
            ("release remote", lambda: device.set_remote(False)),
            ("close transport", device.close),
            ("close scpi transport", scpi.close),
        ):
            try:
                action()
            except Exception as exc:  # pragma: no cover - cleanup path
                cleanup_errors.append(f"{label}: {type(exc).__name__}: {exc}")
        for error in cleanup_errors:
            logger.warning("Cleanup issue: %s", error)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify supported EA devices against a live serial connection.")
    parser.add_argument("--port", help="Serial port for the instrument, for example /dev/ttyACM0")
    parser.add_argument(
        "--device",
        choices=[spec.key for spec in SUPPORTED_DEVICES],
        default="auto",
        help="Device model to use, or auto-detect from *IDN?",
    )
    parser.add_argument("--baudrate", type=int, default=115200)
    parser.add_argument("--unit-id", default="auto", help="Modbus unit id: 0, 1, or auto")
    parser.add_argument("--log-level", default="INFO", help="Python log level, for example INFO or DEBUG")
    parser.add_argument("--scpi-timeout", type=float, default=1.0)
    parser.add_argument("--modbus-timeout", type=float, default=0.5)
    parser.add_argument("--skip-scpi", action="store_true", help="Skip the SCPI probe")
    parser.add_argument("--skip-modbus", action="store_true", help="Skip the Modbus probe")
    parser.add_argument(
        "--require-remote-sensing",
        action="store_true",
        help="Fail the probe or live test unless remote sensing is active.",
    )
    parser.add_argument(
        "--exercise-scpi",
        action="store_true",
        help="Run a short live test using SCPI writes. Requires a real source or load connected appropriately.",
    )
    parser.add_argument(
        "--exercise-modbus",
        action="store_true",
        help="Run a short live test using Modbus writes. Requires a real source or load connected appropriately.",
    )
    parser.add_argument("--exercise-current-a", type=float, default=0.5)
    parser.add_argument("--exercise-duration-s", type=float, default=2.0)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(args.log_level)

    port = resolve_port(args.port)
    logger.info("Using serial port: %s", port)
    if Path(port).exists():
        logger.info("Port present: yes")
    else:
        raise SystemExit(f"Serial port not present: {port}")

    spec, _idn = resolve_device_spec(port, args.baudrate, args.scpi_timeout, args.device)

    modbus_unit_id = None
    if not args.skip_modbus or args.exercise_modbus:
        modbus_unit_id = resolve_modbus_unit_id(
            spec=spec,
            port=port,
            baudrate=args.baudrate,
            timeout=args.modbus_timeout,
            selection=args.unit_id,
        )

    failures: list[str] = []
    if not args.skip_scpi:
        try:
            probe_scpi(spec, port=port, baudrate=args.baudrate, timeout=args.scpi_timeout)
        except Exception as exc:
            failures.append(f"SCPI probe failed: {type(exc).__name__}: {exc}")
            logger.error("%s", failures[-1])
    if not args.skip_modbus:
        try:
            probe_modbus(
                spec,
                port=port,
                baudrate=args.baudrate,
                unit_id=modbus_unit_id,
                timeout=args.modbus_timeout,
                require_remote_sensing=args.require_remote_sensing,
            )
        except Exception as exc:
            failures.append(f"Modbus probe failed: {type(exc).__name__}: {exc}")
            logger.error("%s", failures[-1])
    if args.exercise_scpi:
        try:
            exercise_scpi(
                spec,
                port=port,
                baudrate=args.baudrate,
                timeout=args.scpi_timeout,
                current_a=args.exercise_current_a,
                duration_s=args.exercise_duration_s,
            )
        except Exception as exc:
            failures.append(f"SCPI live test failed: {type(exc).__name__}: {exc}")
            logger.error("%s", failures[-1])
    if args.exercise_modbus:
        try:
            exercise_modbus(
                spec,
                port=port,
                baudrate=args.baudrate,
                unit_id=modbus_unit_id,
                timeout=args.modbus_timeout,
                current_a=args.exercise_current_a,
                duration_s=args.exercise_duration_s,
                require_remote_sensing=args.require_remote_sensing,
            )
        except Exception as exc:
            failures.append(f"Modbus live test failed: {type(exc).__name__}: {exc}")
            logger.error("%s", failures[-1])
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
