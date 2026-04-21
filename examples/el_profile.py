from __future__ import annotations

"""
EA-EL 9080-60 DT profile runner driven entirely by a sibling YAML file.

Edit `examples/el_profile.yaml`, then run:

    uv sync
    uv run python examples/el_profile.py

The script asks for the battery serial number before it starts and writes a CSV
log to `logging/` while the profile is running.
"""

import csv
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml

from ea_driver import EAEL9080_60DT
from ea_driver.config import ConnectionSettings, build_device_connection, deep_merge_dicts, format_connection

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
LOGGER = logging.getLogger("ea_driver.examples.el_profile")
DEFAULT_PROFILE_PATH = Path(__file__).with_suffix(".yaml")
ALLOWED_TRANSPORTS = {"usb-modbus", "usb-scpi", "lan-scpi"}
ALLOWED_STAGE_MODES = {"off", "cv", "cc", "cp", "cr"}
DEVICE_RATINGS = EAEL9080_60DT.RATINGS

CSV_FIELDS = [
    "timestamp",
    "battery_serial",
    "sample_index",
    "stage_index",
    "stage_name",
    "stage_mode",
    "stage_sample_index",
    "elapsed_s",
    "stage_elapsed_s",
    "stage_duration_s",
    "stage_cutoff_voltage_v",
    "stage_setpoint",
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

DEFAULT_PROFILE = {
    "connection": {
        "transport": "usb-modbus",
        "serial_port": None,
        "serial_glob": "*EL_9080-60_DT*",
        "host": "192.168.0.42",
        "lan_scpi_port": 5025,
        "baudrate": 115200,
        "unit_id": 0,
        "timeout_s": 1.0,
    },
    "run": {
        "log_directory": "logging",
        "sample_interval_s": 1.0,
        "remote_settle_s": 0.3,
        "enable_settle_s": 0.5,
        "stage_settle_s": 0.3,
    },
    "protections": {},
    "limits": {},
}


@dataclass(frozen=True, slots=True)
class RunSettings:
    log_directory: Path = Path("logging")
    sample_interval_s: float = 1.0
    remote_settle_s: float = 0.3
    enable_settle_s: float = 0.5
    stage_settle_s: float = 0.3


@dataclass(frozen=True, slots=True)
class ProtectionSettings:
    ovp_v: float | None = None
    ocp_a: float | None = None
    opp_w: float | None = None


@dataclass(frozen=True, slots=True)
class AdjustmentLimits:
    voltage_min_v: float | None = None
    voltage_max_v: float | None = None
    current_min_a: float | None = None
    current_max_a: float | None = None
    power_max_w: float | None = None
    resistance_max_ohm: float | None = None


@dataclass(frozen=True, slots=True)
class ProfileStage:
    name: str
    mode: str
    setpoint: float | None
    duration_s: float | None
    cutoff_voltage_v: float | None
    cutoff_confirm_samples: int


@dataclass(frozen=True, slots=True)
class ProfileConfig:
    connection: ConnectionSettings
    run: RunSettings
    protections: ProtectionSettings
    limits: AdjustmentLimits
    stages: list[ProfileStage]


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


def build_log_path(run_settings: RunSettings, battery_serial: str, now: datetime | None = None) -> Path:
    timestamp = (now or datetime.now()).strftime("%Y%m%d_%H%M%S")
    safe_serial = sanitize_filename_component(battery_serial)
    return run_settings.log_directory / f"{timestamp}_{safe_serial}_el_profile.csv"


def timestamp_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def normalize_optional_text(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized or normalized.lower() == "auto":
        return None
    return normalized


def load_profile(profile_path: Path = DEFAULT_PROFILE_PATH) -> ProfileConfig:
    if not profile_path.exists():
        raise SystemExit(f"Profile file not found: {profile_path}")

    with profile_path.open("r", encoding="utf-8") as handle:
        loaded_profile = yaml.safe_load(handle) or {}

    if not isinstance(loaded_profile, dict):
        raise SystemExit(f"Profile file must contain a YAML object: {profile_path}")

    merged_profile = deep_merge_dicts(DEFAULT_PROFILE, loaded_profile)
    return normalize_profile(merged_profile)


def normalize_profile(raw_profile: dict[str, object]) -> ProfileConfig:
    connection_data = raw_profile.get("connection", {})
    run_data = raw_profile.get("run", {})
    protections_data = raw_profile.get("protections", {})
    limits_data = raw_profile.get("limits", {})
    stages_data = raw_profile.get("stages")

    if not isinstance(connection_data, dict):
        raise SystemExit("'connection' must be a YAML object.")
    if not isinstance(run_data, dict):
        raise SystemExit("'run' must be a YAML object.")
    if not isinstance(protections_data, dict):
        raise SystemExit("'protections' must be a YAML object.")
    if not isinstance(limits_data, dict):
        raise SystemExit("'limits' must be a YAML object.")
    if not isinstance(stages_data, list):
        raise SystemExit("'stages' must be a YAML list and must be defined in the YAML profile.")

    connection = normalize_connection(connection_data)
    run_settings = normalize_run_settings(run_data)
    protections = normalize_protections(protections_data)
    limits = normalize_limits(limits_data)
    stages = [normalize_stage(stage_data, index=index, run_settings=run_settings, limits=limits) for index, stage_data in enumerate(stages_data, start=1)]
    if not stages:
        raise SystemExit("Profile must contain at least one stage.")
    validate_profile_consistency(run_settings=run_settings, protections=protections, limits=limits, stages=stages)

    return ProfileConfig(
        connection=connection,
        run=run_settings,
        protections=protections,
        limits=limits,
        stages=stages,
    )


def normalize_connection(raw_connection: dict[str, object]) -> ConnectionSettings:
    defaults = DEFAULT_PROFILE["connection"]
    transport = str(raw_connection.get("transport", defaults["transport"])).strip()
    if transport not in ALLOWED_TRANSPORTS:
        supported = ", ".join(sorted(ALLOWED_TRANSPORTS))
        raise SystemExit(f"Unsupported EL transport {transport!r}. Use one of: {supported}.")

    return ConnectionSettings(
        transport=transport,
        serial_port=normalize_optional_text(raw_connection.get("serial_port")),
        serial_glob=normalize_optional_text(raw_connection.get("serial_glob")),
        host=normalize_optional_text(raw_connection.get("host")),
        lan_scpi_port=int(raw_connection.get("lan_scpi_port", defaults["lan_scpi_port"])),
        baudrate=int(raw_connection.get("baudrate", defaults["baudrate"])),
        unit_id=int(raw_connection.get("unit_id", defaults["unit_id"])),
        timeout_s=float(raw_connection.get("timeout_s", defaults["timeout_s"])),
    )


def normalize_run_settings(raw_run_settings: dict[str, object]) -> RunSettings:
    defaults = DEFAULT_PROFILE["run"]
    run_settings = RunSettings(
        log_directory=Path(str(raw_run_settings.get("log_directory", defaults["log_directory"]))),
        sample_interval_s=float(raw_run_settings.get("sample_interval_s", defaults["sample_interval_s"])),
        remote_settle_s=float(raw_run_settings.get("remote_settle_s", defaults["remote_settle_s"])),
        enable_settle_s=float(raw_run_settings.get("enable_settle_s", defaults["enable_settle_s"])),
        stage_settle_s=float(raw_run_settings.get("stage_settle_s", defaults["stage_settle_s"])),
    )
    if run_settings.sample_interval_s <= 0.0:
        raise SystemExit("'run.sample_interval_s' must be greater than zero.")
    if run_settings.remote_settle_s < 0.0:
        raise SystemExit("'run.remote_settle_s' must be zero or greater.")
    if run_settings.enable_settle_s < 0.0:
        raise SystemExit("'run.enable_settle_s' must be zero or greater.")
    if run_settings.stage_settle_s < 0.0:
        raise SystemExit("'run.stage_settle_s' must be zero or greater.")
    return run_settings


def normalize_protections(raw_protections: dict[str, object]) -> ProtectionSettings:
    protections = ProtectionSettings(
        ovp_v=optional_float(raw_protections.get("ovp_v")),
        ocp_a=optional_float(raw_protections.get("ocp_a")),
        opp_w=optional_float(raw_protections.get("opp_w")),
    )
    validate_positive_or_none("protections.ovp_v", protections.ovp_v)
    validate_positive_or_none("protections.ocp_a", protections.ocp_a)
    validate_positive_or_none("protections.opp_w", protections.opp_w)
    validate_not_above("protections.ovp_v", protections.ovp_v, DEVICE_RATINGS.voltage_v * 1.10)
    validate_not_above("protections.ocp_a", protections.ocp_a, DEVICE_RATINGS.current_a * 1.10)
    validate_not_above("protections.opp_w", protections.opp_w, DEVICE_RATINGS.power_w * 1.10)
    return protections


def normalize_limits(raw_limits: dict[str, object]) -> AdjustmentLimits:
    limits = AdjustmentLimits(
        voltage_min_v=optional_float(raw_limits.get("voltage_min_v")),
        voltage_max_v=optional_float(raw_limits.get("voltage_max_v")),
        current_min_a=optional_float(raw_limits.get("current_min_a")),
        current_max_a=optional_float(raw_limits.get("current_max_a")),
        power_max_w=optional_float(raw_limits.get("power_max_w")),
        resistance_max_ohm=optional_float(raw_limits.get("resistance_max_ohm")),
    )

    validate_non_negative_or_none("limits.voltage_min_v", limits.voltage_min_v)
    validate_positive_or_none("limits.voltage_max_v", limits.voltage_max_v)
    validate_non_negative_or_none("limits.current_min_a", limits.current_min_a)
    validate_positive_or_none("limits.current_max_a", limits.current_max_a)
    validate_positive_or_none("limits.power_max_w", limits.power_max_w)
    validate_positive_or_none("limits.resistance_max_ohm", limits.resistance_max_ohm)

    validate_min_max_pair("limits.voltage_min_v", limits.voltage_min_v, "limits.voltage_max_v", limits.voltage_max_v)
    validate_min_max_pair("limits.current_min_a", limits.current_min_a, "limits.current_max_a", limits.current_max_a)

    validate_not_above("limits.voltage_max_v", limits.voltage_max_v, DEVICE_RATINGS.voltage_v * 1.02)
    validate_not_above("limits.current_max_a", limits.current_max_a, DEVICE_RATINGS.current_a * 1.02)
    validate_not_above("limits.power_max_w", limits.power_max_w, DEVICE_RATINGS.power_w * 1.02)
    validate_not_above("limits.resistance_max_ohm", limits.resistance_max_ohm, DEVICE_RATINGS.resistance_ohm_max)

    return limits


def normalize_stage(
    raw_stage: object,
    *,
    index: int,
    run_settings: RunSettings,
    limits: AdjustmentLimits,
) -> ProfileStage:
    if not isinstance(raw_stage, dict):
        raise SystemExit(f"Stage {index} must be a YAML object.")

    name = str(raw_stage.get("name", f"stage_{index}")).strip() or f"stage_{index}"
    raw_mode = raw_stage.get("mode", "")
    mode = "off" if raw_mode is False else str(raw_mode).strip().lower()
    if mode not in ALLOWED_STAGE_MODES:
        supported = ", ".join(sorted(ALLOWED_STAGE_MODES))
        raise SystemExit(f"Stage {name!r} uses unsupported mode {mode!r}. Use one of: {supported}.")

    setpoint = optional_float(raw_stage.get("setpoint"))
    duration_s = optional_float(raw_stage.get("duration_s"))
    cutoff_voltage_v = optional_float(raw_stage.get("cutoff_voltage_v"))
    cutoff_confirm_samples = int(raw_stage.get("cutoff_confirm_samples", 3))

    if mode == "off":
        if setpoint is not None:
            raise SystemExit(f"Stage {name!r} uses mode 'off' and must not define 'setpoint'.")
    elif setpoint is None:
        raise SystemExit(f"Stage {name!r} must define 'setpoint' for mode {mode!r}.")

    if duration_s is None and cutoff_voltage_v is None:
        raise SystemExit(f"Stage {name!r} must define 'duration_s' and/or 'cutoff_voltage_v'.")
    if duration_s is not None and duration_s <= 0.0:
        raise SystemExit(f"Stage {name!r} has invalid 'duration_s': {duration_s}.")
    if cutoff_voltage_v is not None and cutoff_voltage_v <= 0.0:
        raise SystemExit(f"Stage {name!r} has invalid 'cutoff_voltage_v': {cutoff_voltage_v}.")
    if cutoff_confirm_samples < 1:
        raise SystemExit(f"Stage {name!r} has invalid 'cutoff_confirm_samples': {cutoff_confirm_samples}.")

    validate_stage_against_limits(name=name, mode=mode, setpoint=setpoint, cutoff_voltage_v=cutoff_voltage_v, limits=limits)

    return ProfileStage(
        name=name,
        mode=mode,
        setpoint=setpoint,
        duration_s=duration_s,
        cutoff_voltage_v=cutoff_voltage_v,
        cutoff_confirm_samples=cutoff_confirm_samples,
    )


def validate_stage_against_limits(
    *,
    name: str,
    mode: str,
    setpoint: float | None,
    cutoff_voltage_v: float | None,
    limits: AdjustmentLimits,
) -> None:
    validate_not_above(f"Stage {name!r} cutoff_voltage_v", cutoff_voltage_v, DEVICE_RATINGS.voltage_v)

    if mode == "cv":
        validate_positive_or_none(f"Stage {name!r} voltage setpoint", setpoint)
        validate_not_below(f"Stage {name!r} voltage setpoint", setpoint, limits.voltage_min_v)
        validate_not_above(f"Stage {name!r} voltage setpoint", setpoint, DEVICE_RATINGS.voltage_v)
        validate_not_above(f"Stage {name!r} voltage setpoint", setpoint, limits.voltage_max_v)
        return

    if mode == "cc":
        validate_positive_or_none(f"Stage {name!r} current setpoint", setpoint)
        validate_not_below(f"Stage {name!r} cutoff_voltage_v", cutoff_voltage_v, limits.voltage_min_v)
        validate_not_below(f"Stage {name!r} current setpoint", setpoint, limits.current_min_a)
        validate_not_above(f"Stage {name!r} current setpoint", setpoint, DEVICE_RATINGS.current_a)
        validate_not_above(f"Stage {name!r} current setpoint", setpoint, limits.current_max_a)
        return

    if mode == "cp":
        validate_positive_or_none(f"Stage {name!r} power setpoint", setpoint)
        validate_not_below(f"Stage {name!r} cutoff_voltage_v", cutoff_voltage_v, limits.voltage_min_v)
        validate_not_above(f"Stage {name!r} power setpoint", setpoint, DEVICE_RATINGS.power_w)
        validate_not_above(f"Stage {name!r} power setpoint", setpoint, limits.power_max_w)
        return

    if mode == "cr":
        validate_positive_or_none(f"Stage {name!r} resistance setpoint", setpoint)
        validate_not_below(f"Stage {name!r} cutoff_voltage_v", cutoff_voltage_v, limits.voltage_min_v)
        validate_not_below(f"Stage {name!r} resistance setpoint", setpoint, DEVICE_RATINGS.resistance_ohm_min)
        validate_not_above(f"Stage {name!r} resistance setpoint", setpoint, DEVICE_RATINGS.resistance_ohm_max)
        validate_not_above(f"Stage {name!r} resistance setpoint", setpoint, limits.resistance_max_ohm)


def validate_profile_consistency(
    *,
    run_settings: RunSettings,
    protections: ProtectionSettings,
    limits: AdjustmentLimits,
    stages: list[ProfileStage],
) -> None:
    for stage in stages:
        if stage.mode == "cv":
            validate_not_above(f"Stage {stage.name!r} voltage setpoint", stage.setpoint, protections.ovp_v)

        if stage.mode in {"cc", "cp", "cr"}:
            validate_not_above(f"Stage {stage.name!r} cutoff_voltage_v", stage.cutoff_voltage_v, limits.voltage_max_v)
            validate_not_above(f"Stage {stage.name!r} cutoff_voltage_v", stage.cutoff_voltage_v, protections.ovp_v)


def validate_positive_or_none(name: str, value: float | None) -> None:
    if value is not None and value <= 0.0:
        raise SystemExit(f"{name} must be greater than zero.")


def validate_non_negative_or_none(name: str, value: float | None) -> None:
    if value is not None and value < 0.0:
        raise SystemExit(f"{name} must be zero or greater.")


def validate_min_max_pair(min_name: str, min_value: float | None, max_name: str, max_value: float | None) -> None:
    if min_value is None or max_value is None:
        return
    if min_value > max_value:
        raise SystemExit(f"{min_name} must be less than or equal to {max_name}.")


def validate_not_above(name: str, value: float | None, maximum: float | None) -> None:
    if value is None or maximum is None:
        return
    if value > maximum:
        raise SystemExit(f"{name}={value} exceeds the allowed maximum of {maximum}.")


def validate_not_below(name: str, value: float | None, minimum: float | None) -> None:
    if value is None or minimum is None:
        return
    if value < minimum:
        raise SystemExit(f"{name}={value} is below the allowed minimum of {minimum}.")


def build_device(config: ProfileConfig):
    return build_device_connection(EAEL9080_60DT, config.connection)


def read_measurement_and_status(device):
    is_modbus = hasattr(device, "read_status")
    measurement = device.read_measurements() if is_modbus else device.measure_all()
    status = device.read_status() if is_modbus else None
    input_enabled = status.dc_on if status else device.is_input_enabled()
    return measurement, status, input_enabled


def read_protection_settings(device) -> dict[str, float]:
    settings: dict[str, float] = {}
    if hasattr(device, "get_source_voltage_protection"):
        settings["ovp_v"] = device.get_source_voltage_protection()
    if hasattr(device, "get_source_current_protection"):
        settings["ocp_a"] = device.get_source_current_protection()
    if hasattr(device, "get_source_power_protection"):
        settings["opp_w"] = device.get_source_power_protection()
    return settings


def read_adjustment_limits(device) -> dict[str, float]:
    limits: dict[str, float] = {}
    if hasattr(device, "get_voltage_limit_low"):
        try:
            limits["voltage_min_v"] = device.get_voltage_limit_low()
        except Exception as exc:
            LOGGER.warning("Skipping unsupported limit read voltage_min_v: %s", exc)
    if hasattr(device, "get_voltage_limit_high"):
        try:
            limits["voltage_max_v"] = device.get_voltage_limit_high()
        except Exception as exc:
            LOGGER.warning("Skipping unsupported limit read voltage_max_v: %s", exc)
    if hasattr(device, "get_current_limit_low"):
        try:
            limits["current_min_a"] = device.get_current_limit_low()
        except Exception as exc:
            LOGGER.warning("Skipping unsupported limit read current_min_a: %s", exc)
    if hasattr(device, "get_current_limit_high"):
        try:
            limits["current_max_a"] = device.get_current_limit_high()
        except Exception as exc:
            LOGGER.warning("Skipping unsupported limit read current_max_a: %s", exc)
    if hasattr(device, "get_power_limit_high"):
        try:
            limits["power_max_w"] = device.get_power_limit_high()
        except Exception as exc:
            LOGGER.warning("Skipping unsupported limit read power_max_w: %s", exc)
    if hasattr(device, "get_resistance_limit_high"):
        try:
            limits["resistance_max_ohm"] = device.get_resistance_limit_high()
        except Exception as exc:
            LOGGER.warning("Skipping unsupported limit read resistance_max_ohm: %s", exc)
    return limits


def log_device_connection(device) -> None:
    if hasattr(device, "read_status"):
        LOGGER.info("Nominals: %s", device.read_nominals())
        LOGGER.info("Initial protections: %s", read_protection_settings(device))
        LOGGER.info("Initial limits: %s", read_adjustment_limits(device))
        initial_status = device.read_status()
        LOGGER.info("Initial status: %s", initial_status)
        if not initial_status.remote_sensing:
            LOGGER.warning("Kelvin / remote sensing is not active on the EL.")
        return

    LOGGER.info("Connected to %s", device.identify())
    device.clear_status()
    LOGGER.info("Nominals: %s", device.read_nominals())
    LOGGER.info("Initial protections: %s", read_protection_settings(device))
    LOGGER.info("Initial limits: %s", read_adjustment_limits(device))


def prepare_setpoint_for_limits(device, getter_name: str, setter_name: str, low: float | None, high: float | None) -> None:
    if not hasattr(device, getter_name) or not hasattr(device, setter_name):
        return

    try:
        current_value = getattr(device, getter_name)()
    except Exception as exc:
        LOGGER.warning("Skipping setpoint preparation for %s: %s", getter_name, exc)
        return
    target_value = current_value

    if high is not None and target_value > high:
        target_value = high
    if low is not None and target_value < low:
        target_value = low

    if target_value != current_value:
        try:
            getattr(device, setter_name)(target_value)
        except Exception as exc:
            LOGGER.warning("Failed to adjust setpoint with %s: %s", setter_name, exc)


def apply_adjustment_limits(device, limits: AdjustmentLimits) -> None:
    configured = {}

    prepare_setpoint_for_limits(device, "get_voltage_setpoint", "set_voltage", limits.voltage_min_v, limits.voltage_max_v)
    if limits.voltage_min_v is not None:
        try:
            device.set_voltage_limit_low(limits.voltage_min_v)
            configured["voltage_min_v"] = limits.voltage_min_v
        except Exception as exc:
            LOGGER.warning("Skipping unsupported limit write voltage_min_v: %s", exc)
    if limits.voltage_max_v is not None:
        try:
            device.set_voltage_limit_high(limits.voltage_max_v)
            configured["voltage_max_v"] = limits.voltage_max_v
        except Exception as exc:
            LOGGER.warning("Skipping unsupported limit write voltage_max_v: %s", exc)

    prepare_setpoint_for_limits(
        device,
        "get_source_current_setpoint",
        "set_current",
        limits.current_min_a,
        limits.current_max_a,
    )
    if limits.current_min_a is not None:
        try:
            device.set_current_limit_low(limits.current_min_a)
            configured["current_min_a"] = limits.current_min_a
        except Exception as exc:
            LOGGER.warning("Skipping unsupported limit write current_min_a: %s", exc)
    if limits.current_max_a is not None:
        try:
            device.set_current_limit_high(limits.current_max_a)
            configured["current_max_a"] = limits.current_max_a
        except Exception as exc:
            LOGGER.warning("Skipping unsupported limit write current_max_a: %s", exc)

    prepare_setpoint_for_limits(device, "get_source_power_setpoint", "set_power", None, limits.power_max_w)
    if limits.power_max_w is not None:
        try:
            device.set_power_limit_high(limits.power_max_w)
            configured["power_max_w"] = limits.power_max_w
        except Exception as exc:
            LOGGER.warning("Skipping unsupported limit write power_max_w: %s", exc)

    prepare_setpoint_for_limits(device, "get_source_resistance_setpoint", "set_resistance", None, limits.resistance_max_ohm)
    if limits.resistance_max_ohm is not None:
        try:
            device.set_resistance_limit_high(limits.resistance_max_ohm)
            configured["resistance_max_ohm"] = limits.resistance_max_ohm
        except Exception as exc:
            LOGGER.warning("Skipping unsupported limit write resistance_max_ohm: %s", exc)

    if configured:
        LOGGER.info("Applied adjustment limits: %s", configured)
        LOGGER.info("Adjustment limits now: %s", read_adjustment_limits(device))


def apply_protection_settings(device, protections: ProtectionSettings) -> None:
    configured = {}

    if protections.ovp_v is not None:
        device.set_source_voltage_protection(protections.ovp_v)
        configured["ovp_v"] = protections.ovp_v
    if protections.ocp_a is not None:
        device.set_source_current_protection(protections.ocp_a)
        configured["ocp_a"] = protections.ocp_a
    if protections.opp_w is not None:
        device.set_source_power_protection(protections.opp_w)
        configured["opp_w"] = protections.opp_w

    if configured:
        LOGGER.info("Applied protections: %s", configured)
        LOGGER.info("Protections now: %s", read_protection_settings(device))


def effective_stage_voltage_ceiling(limits: AdjustmentLimits) -> float:
    return limits.voltage_max_v if limits.voltage_max_v is not None else DEVICE_RATINGS.voltage_v


def effective_stage_voltage_floor(stage: ProfileStage, limits: AdjustmentLimits) -> float:
    if stage.cutoff_voltage_v is not None:
        return stage.cutoff_voltage_v
    if limits.voltage_min_v is not None:
        return limits.voltage_min_v
    return 0.0


def effective_stage_current_ceiling(limits: AdjustmentLimits) -> float:
    return limits.current_max_a if limits.current_max_a is not None else DEVICE_RATINGS.current_a


def effective_stage_power_ceiling(limits: AdjustmentLimits) -> float:
    return limits.power_max_w if limits.power_max_w is not None else DEVICE_RATINGS.power_w


def apply_stage(device, stage: ProfileStage, run_settings: RunSettings, limits: AdjustmentLimits) -> None:
    device.set_input_enabled(False)

    if stage.mode == "cv":
        device.set_current(effective_stage_current_ceiling(limits))
        device.set_power(effective_stage_power_ceiling(limits))
        device.set_voltage(float(stage.setpoint))
        device.set_input_enabled(True)
        time.sleep(run_settings.enable_settle_s)
    elif stage.mode == "cc":
        device.set_voltage(effective_stage_voltage_floor(stage, limits))
        device.set_power(effective_stage_power_ceiling(limits))
        device.set_current(float(stage.setpoint))
        device.set_input_enabled(True)
        time.sleep(run_settings.enable_settle_s)
    elif stage.mode == "cp":
        device.set_voltage(effective_stage_voltage_floor(stage, limits))
        device.set_current(effective_stage_current_ceiling(limits))
        device.set_power(float(stage.setpoint))
        device.set_input_enabled(True)
        time.sleep(run_settings.enable_settle_s)
    elif stage.mode == "cr":
        device.set_voltage(effective_stage_voltage_floor(stage, limits))
        device.set_current(effective_stage_current_ceiling(limits))
        device.set_power(effective_stage_power_ceiling(limits))
        device.set_resistance(float(stage.setpoint))
        device.set_input_enabled(True)
        time.sleep(run_settings.enable_settle_s)

    time.sleep(run_settings.stage_settle_s)


def cleanup_device(device) -> None:
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


def run_profile(config: ProfileConfig, battery_serial: str) -> Path:
    log_path = build_log_path(config.run, battery_serial)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    device = build_device(config)
    LOGGER.info("Using %s", format_connection(config.connection))
    LOGGER.info("Battery serial: %s", battery_serial)

    with log_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()

        with device:
            log_device_connection(device)
            preflight_measurement = device.read_measurements() if hasattr(device, "read_measurements") else device.measure_all()
            if config.protections.ovp_v is not None and preflight_measurement.voltage_v >= config.protections.ovp_v:
                raise RuntimeError(
                    f"Measured battery voltage {preflight_measurement.voltage_v:.3f} V is already at or above "
                    f"configured OVP {config.protections.ovp_v:.3f} V. Raise OVP or lower the pack voltage first."
                )

            start_monotonic = time.monotonic()
            last_sample_monotonic: float | None = None
            discharged_ah = 0.0
            discharged_wh = 0.0
            min_voltage_v = float("inf")

            try:
                device.set_remote(True)
                time.sleep(config.run.remote_settle_s)
                apply_adjustment_limits(device, config.limits)
                apply_protection_settings(device, config.protections)

                sample_index = 0

                for stage_index, stage in enumerate(config.stages, start=1):
                    LOGGER.info("Starting stage %d/%d: %s", stage_index, len(config.stages), stage)
                    apply_stage(device, stage, config.run, config.limits)
                    stage_start_monotonic = time.monotonic()
                    stage_sample_index = 0
                    below_cutoff_samples = 0

                    while True:
                        sample_index += 1
                        stage_sample_index += 1
                        measurement, status, input_enabled = read_measurement_and_status(device)
                        sample_monotonic = time.monotonic()
                        elapsed_s = sample_monotonic - start_monotonic
                        stage_elapsed_s = sample_monotonic - stage_start_monotonic

                        if last_sample_monotonic is not None:
                            dt_s = sample_monotonic - last_sample_monotonic
                            if input_enabled:
                                discharged_ah += max(measurement.current_a, 0.0) * dt_s / 3600.0
                                discharged_wh += max(measurement.power_w, 0.0) * dt_s / 3600.0
                        last_sample_monotonic = sample_monotonic

                        min_voltage_v = min(min_voltage_v, measurement.voltage_v)
                        if stage.cutoff_voltage_v is not None and measurement.voltage_v <= stage.cutoff_voltage_v:
                            below_cutoff_samples += 1
                        else:
                            below_cutoff_samples = 0

                        stop_reason = ""
                        if stage.duration_s is not None and stage_elapsed_s >= stage.duration_s:
                            stop_reason = f"duration reached ({stage.duration_s:.1f} s)"
                        if not stop_reason and stage.cutoff_voltage_v is not None:
                            if below_cutoff_samples >= stage.cutoff_confirm_samples:
                                stop_reason = (
                                    f"voltage <= {stage.cutoff_voltage_v:.3f} V "
                                    f"for {stage.cutoff_confirm_samples} samples"
                                )

                        writer.writerow(
                            {
                                "timestamp": timestamp_now(),
                                "battery_serial": battery_serial,
                                "sample_index": sample_index,
                                "stage_index": stage_index,
                                "stage_name": stage.name,
                                "stage_mode": stage.mode,
                                "stage_sample_index": stage_sample_index,
                                "elapsed_s": f"{elapsed_s:.3f}",
                                "stage_elapsed_s": f"{stage_elapsed_s:.3f}",
                                "stage_duration_s": "" if stage.duration_s is None else stage.duration_s,
                                "stage_cutoff_voltage_v": "" if stage.cutoff_voltage_v is None else stage.cutoff_voltage_v,
                                "stage_setpoint": "" if stage.setpoint is None else stage.setpoint,
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
                            "Stage %s sample %d: %.3f V, %.3f A, %.3f W",
                            stage.name,
                            stage_sample_index,
                            measurement.voltage_v,
                            measurement.current_a,
                            measurement.power_w,
                        )

                        if stop_reason:
                            LOGGER.info("Finished stage %s: %s", stage.name, stop_reason)
                            break

                        time.sleep(config.run.sample_interval_s)
            finally:
                cleanup_device(device)

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
    profile = load_profile()
    LOGGER.info("Loaded profile from %s", DEFAULT_PROFILE_PATH)
    battery_serial = prompt_battery_serial()
    run_profile(profile, battery_serial)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        LOGGER.info("Stopped by user.")
