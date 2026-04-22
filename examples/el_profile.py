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
import socket
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TextIO

import yaml

from ea_driver import EAEL9080_60DT
from ea_driver.config import ConnectionSettings, build_device_connection, deep_merge_dicts, format_connection
from ea_driver.ea import EA_SET_VALUE_FULL_SCALE

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
LOGGER = logging.getLogger("ea_driver.examples.el_profile")
DEFAULT_PROFILE_PATH = Path(__file__).with_suffix(".yaml")
ALLOWED_TRANSPORTS = {"usb-modbus", "usb-scpi", "lan-scpi"}
ALLOWED_STAGE_MODES = {"off", "cv", "cc", "cp", "cr"}
DEVICE_RATINGS = EAEL9080_60DT.RATINGS
_CONSOLE_HANDLER: logging.Handler | None = None
_FILE_HANDLER: logging.Handler | None = None

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
class StageSetpoints:
    voltage_v: float | None = None
    current_a: float | None = None
    power_w: float | None = None
    resistance_ohm: float | None = None


@dataclass(frozen=True, slots=True)
class ProfileConfig:
    connection: ConnectionSettings
    run: RunSettings
    protections: ProtectionSettings
    limits: AdjustmentLimits
    stages: list[ProfileStage]


def configure_logging() -> None:
    configure_logging_handlers()


def configure_logging_handlers(log_file_path: Path | None = None) -> None:
    global _CONSOLE_HANDLER, _FILE_HANDLER

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    if _CONSOLE_HANDLER is None:
        _CONSOLE_HANDLER = logging.StreamHandler()
        _CONSOLE_HANDLER.setLevel(logging.WARNING)
        _CONSOLE_HANDLER.setFormatter(logging.Formatter(LOG_FORMAT))
        root_logger.addHandler(_CONSOLE_HANDLER)
    elif _CONSOLE_HANDLER not in root_logger.handlers:
        root_logger.addHandler(_CONSOLE_HANDLER)

    if log_file_path is None:
        return

    resolved_path = log_file_path.resolve()
    current_path = None
    if isinstance(_FILE_HANDLER, logging.FileHandler):
        current_path = Path(_FILE_HANDLER.baseFilename)
    if current_path == resolved_path:
        if _FILE_HANDLER not in root_logger.handlers:
            root_logger.addHandler(_FILE_HANDLER)
        return

    if _FILE_HANDLER is not None:
        root_logger.removeHandler(_FILE_HANDLER)
        _FILE_HANDLER.close()

    file_handler = logging.FileHandler(resolved_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root_logger.addHandler(file_handler)
    _FILE_HANDLER = file_handler


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


def build_text_log_path(csv_log_path: Path) -> Path:
    return csv_log_path.with_suffix(".log")


def serialize_profile_for_log(config: ProfileConfig) -> dict[str, object]:
    return {
        "connection": {
            "transport": config.connection.transport,
            "serial_port": config.connection.serial_port,
            "serial_glob": config.connection.serial_glob,
            "host": config.connection.host,
            "lan_scpi_port": config.connection.lan_scpi_port,
            "baudrate": config.connection.baudrate,
            "unit_id": config.connection.unit_id,
            "timeout_s": config.connection.timeout_s,
        },
        "run": {
            "log_directory": str(config.run.log_directory),
            "sample_interval_s": config.run.sample_interval_s,
            "remote_settle_s": config.run.remote_settle_s,
            "enable_settle_s": config.run.enable_settle_s,
            "stage_settle_s": config.run.stage_settle_s,
        },
        "protections": {
            "ovp_v": config.protections.ovp_v,
            "ocp_a": config.protections.ocp_a,
            "opp_w": config.protections.opp_w,
        },
        "limits": {
            "voltage_min_v": config.limits.voltage_min_v,
            "voltage_max_v": config.limits.voltage_max_v,
            "current_min_a": config.limits.current_min_a,
            "current_max_a": config.limits.current_max_a,
            "power_max_w": config.limits.power_max_w,
            "resistance_max_ohm": config.limits.resistance_max_ohm,
        },
        "stages": [
            {
                "name": stage.name,
                "mode": stage.mode,
                "setpoint": stage.setpoint,
                "duration_s": stage.duration_s,
                "cutoff_voltage_v": stage.cutoff_voltage_v,
                "cutoff_confirm_samples": stage.cutoff_confirm_samples,
            }
            for stage in config.stages
        ],
    }


def build_csv_metadata_lines(
    *,
    config: ProfileConfig,
    battery_serial: str,
    profile_path: Path,
    started_at: datetime,
    hostname: str | None = None,
) -> list[str]:
    lines = [
        f"# host: {hostname or socket.gethostname()}",
        f"# started_at: {started_at.isoformat(timespec='seconds')}",
        f"# profile_path: {profile_path.resolve()}",
        f"# battery_serial: {battery_serial}",
        "# timestamp_column: seconds since run start (time.monotonic)",
        "# profile:",
    ]
    profile_yaml = yaml.safe_dump(serialize_profile_for_log(config), sort_keys=False).rstrip()
    lines.extend(f"#   {line}" for line in profile_yaml.splitlines())
    return lines


def write_csv_metadata_lines(
    handle: TextIO,
    *,
    config: ProfileConfig,
    battery_serial: str,
    profile_path: Path,
    started_at: datetime,
) -> None:
    for line in build_csv_metadata_lines(
        config=config,
        battery_serial=battery_serial,
        profile_path=profile_path,
        started_at=started_at,
    ):
        handle.write(f"{line}\n")


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


def quantize_set_value(value: float | None, nominal: float | None) -> int | None:
    if value is None or nominal in {None, 0.0}:
        return None
    normalized = min(max(value / nominal, 0.0), 1.02)
    return round((normalized / 1.02) * EA_SET_VALUE_FULL_SCALE)


def validate_not_above_set_value_limit(
    name: str,
    value: float | None,
    maximum: float | None,
    nominal: float | None,
) -> None:
    if value is None or maximum is None:
        return
    value_raw = quantize_set_value(value, nominal)
    maximum_raw = quantize_set_value(maximum, nominal)
    if value_raw is None or maximum_raw is None:
        validate_not_above(name, value, maximum)
        return
    if value_raw > maximum_raw:
        raise SystemExit(f"{name}={value} exceeds the allowed maximum of {maximum}.")


def validate_not_below_set_value_limit(
    name: str,
    value: float | None,
    minimum: float | None,
    nominal: float | None,
) -> None:
    if value is None or minimum is None:
        return
    value_raw = quantize_set_value(value, nominal)
    minimum_raw = quantize_set_value(minimum, nominal)
    if value_raw is None or minimum_raw is None:
        validate_not_below(name, value, minimum)
        return
    if value_raw < minimum_raw:
        raise SystemExit(f"{name}={value} is below the allowed minimum of {minimum}.")


def validate_stage_against_active_limits(
    *,
    name: str,
    mode: str,
    setpoint: float | None,
    cutoff_voltage_v: float | None,
    limits: AdjustmentLimits,
) -> None:
    validate_not_above_set_value_limit(
        f"Stage {name!r} cutoff_voltage_v",
        cutoff_voltage_v,
        DEVICE_RATINGS.voltage_v,
        DEVICE_RATINGS.voltage_v,
    )

    if mode == "cv":
        validate_positive_or_none(f"Stage {name!r} voltage setpoint", setpoint)
        validate_not_below_set_value_limit(
            f"Stage {name!r} voltage setpoint",
            setpoint,
            limits.voltage_min_v,
            DEVICE_RATINGS.voltage_v,
        )
        validate_not_above_set_value_limit(
            f"Stage {name!r} voltage setpoint",
            setpoint,
            DEVICE_RATINGS.voltage_v,
            DEVICE_RATINGS.voltage_v,
        )
        validate_not_above_set_value_limit(
            f"Stage {name!r} voltage setpoint",
            setpoint,
            limits.voltage_max_v,
            DEVICE_RATINGS.voltage_v,
        )
        return

    if mode == "cc":
        validate_positive_or_none(f"Stage {name!r} current setpoint", setpoint)
        validate_not_below_set_value_limit(
            f"Stage {name!r} cutoff_voltage_v",
            cutoff_voltage_v,
            limits.voltage_min_v,
            DEVICE_RATINGS.voltage_v,
        )
        validate_not_below_set_value_limit(
            f"Stage {name!r} current setpoint",
            setpoint,
            limits.current_min_a,
            DEVICE_RATINGS.current_a,
        )
        validate_not_above_set_value_limit(
            f"Stage {name!r} current setpoint",
            setpoint,
            DEVICE_RATINGS.current_a,
            DEVICE_RATINGS.current_a,
        )
        validate_not_above_set_value_limit(
            f"Stage {name!r} current setpoint",
            setpoint,
            limits.current_max_a,
            DEVICE_RATINGS.current_a,
        )
        return

    if mode == "cp":
        validate_positive_or_none(f"Stage {name!r} power setpoint", setpoint)
        validate_not_below_set_value_limit(
            f"Stage {name!r} cutoff_voltage_v",
            cutoff_voltage_v,
            limits.voltage_min_v,
            DEVICE_RATINGS.voltage_v,
        )
        validate_not_above_set_value_limit(
            f"Stage {name!r} power setpoint",
            setpoint,
            DEVICE_RATINGS.power_w,
            DEVICE_RATINGS.power_w,
        )
        validate_not_above_set_value_limit(
            f"Stage {name!r} power setpoint",
            setpoint,
            limits.power_max_w,
            DEVICE_RATINGS.power_w,
        )
        return

    if mode == "cr":
        validate_positive_or_none(f"Stage {name!r} resistance setpoint", setpoint)
        validate_not_below_set_value_limit(
            f"Stage {name!r} cutoff_voltage_v",
            cutoff_voltage_v,
            limits.voltage_min_v,
            DEVICE_RATINGS.voltage_v,
        )
        validate_not_below_set_value_limit(
            f"Stage {name!r} resistance setpoint",
            setpoint,
            DEVICE_RATINGS.resistance_ohm_min,
            DEVICE_RATINGS.resistance_ohm_max,
        )
        validate_not_above_set_value_limit(
            f"Stage {name!r} resistance setpoint",
            setpoint,
            DEVICE_RATINGS.resistance_ohm_max,
            DEVICE_RATINGS.resistance_ohm_max,
        )
        validate_not_above_set_value_limit(
            f"Stage {name!r} resistance setpoint",
            setpoint,
            limits.resistance_max_ohm,
            DEVICE_RATINGS.resistance_ohm_max,
        )


def validate_profile_consistency_against_active_limits(
    *,
    protections: ProtectionSettings,
    limits: AdjustmentLimits,
    stages: list[ProfileStage],
) -> None:
    for stage in stages:
        if stage.mode == "cv":
            validate_not_above(f"Stage {stage.name!r} voltage setpoint", stage.setpoint, protections.ovp_v)

        if stage.mode in {"cc", "cp", "cr"}:
            validate_not_above_set_value_limit(
                f"Stage {stage.name!r} cutoff_voltage_v",
                stage.cutoff_voltage_v,
                limits.voltage_max_v,
                DEVICE_RATINGS.voltage_v,
            )
            validate_not_above(f"Stage {stage.name!r} cutoff_voltage_v", stage.cutoff_voltage_v, protections.ovp_v)


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


def resolve_effective_protections(
    configured: ProtectionSettings,
    observed: dict[str, float],
) -> ProtectionSettings:
    return ProtectionSettings(
        ovp_v=configured.ovp_v if configured.ovp_v is not None else observed.get("ovp_v"),
        ocp_a=configured.ocp_a if configured.ocp_a is not None else observed.get("ocp_a"),
        opp_w=configured.opp_w if configured.opp_w is not None else observed.get("opp_w"),
    )


def resolve_effective_limits(
    configured: AdjustmentLimits,
    observed: dict[str, float],
) -> AdjustmentLimits:
    return AdjustmentLimits(
        voltage_min_v=configured.voltage_min_v if configured.voltage_min_v is not None else observed.get("voltage_min_v"),
        voltage_max_v=configured.voltage_max_v if configured.voltage_max_v is not None else observed.get("voltage_max_v"),
        current_min_a=configured.current_min_a if configured.current_min_a is not None else observed.get("current_min_a"),
        current_max_a=configured.current_max_a if configured.current_max_a is not None else observed.get("current_max_a"),
        power_max_w=configured.power_max_w if configured.power_max_w is not None else observed.get("power_max_w"),
        resistance_max_ohm=(
            configured.resistance_max_ohm
            if configured.resistance_max_ohm is not None
            else observed.get("resistance_max_ohm")
        ),
    )


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


def resolve_stage_setpoints(stage: ProfileStage, limits: AdjustmentLimits) -> StageSetpoints:
    if stage.mode == "off":
        return StageSetpoints()
    if stage.mode == "cv":
        return StageSetpoints(
            voltage_v=float(stage.setpoint),
            current_a=effective_stage_current_ceiling(limits),
            power_w=effective_stage_power_ceiling(limits),
        )
    if stage.mode == "cc":
        return StageSetpoints(
            voltage_v=effective_stage_voltage_floor(stage, limits),
            current_a=float(stage.setpoint),
            power_w=effective_stage_power_ceiling(limits),
        )
    if stage.mode == "cp":
        return StageSetpoints(
            voltage_v=effective_stage_voltage_floor(stage, limits),
            current_a=effective_stage_current_ceiling(limits),
            power_w=float(stage.setpoint),
        )
    if stage.mode == "cr":
        return StageSetpoints(
            voltage_v=effective_stage_voltage_floor(stage, limits),
            current_a=effective_stage_current_ceiling(limits),
            power_w=effective_stage_power_ceiling(limits),
            resistance_ohm=float(stage.setpoint),
        )
    raise ValueError(f"Unsupported stage mode: {stage.mode}")


def ensure_below_active_threshold(
    name: str,
    value: float | None,
    threshold: float | None,
    threshold_name: str,
) -> None:
    if value is None or threshold is None:
        return
    if value >= threshold:
        raise RuntimeError(
            f"{name}={value} reaches or exceeds active {threshold_name} threshold {threshold}. "
            f"Raise {threshold_name} or lower the relevant stage setpoint or limit."
        )


def validate_profile_against_active_device(
    *,
    measurement,
    protections: ProtectionSettings,
    limits: AdjustmentLimits,
    stages: list[ProfileStage],
) -> None:
    if protections.ovp_v is not None and measurement.voltage_v >= protections.ovp_v:
        raise RuntimeError(
            f"Measured battery voltage {measurement.voltage_v:.3f} V is already at or above "
            f"active OVP {protections.ovp_v:.3f} V. Raise OVP or lower the pack voltage first."
        )

    try:
        validate_profile_consistency_against_active_limits(
            protections=protections,
            limits=limits,
            stages=stages,
        )
    except SystemExit as exc:
        raise RuntimeError(str(exc)) from exc

    for stage in stages:
        try:
            validate_stage_against_active_limits(
                name=stage.name,
                mode=stage.mode,
                setpoint=stage.setpoint,
                cutoff_voltage_v=stage.cutoff_voltage_v,
                limits=limits,
            )
        except SystemExit as exc:
            raise RuntimeError(str(exc)) from exc

        programmed = resolve_stage_setpoints(stage, limits)
        if stage.mode == "cv":
            ensure_below_active_threshold(
                f"Stage {stage.name!r} voltage setpoint",
                programmed.voltage_v,
                protections.ovp_v,
                "OVP",
            )

        current_label = "current setpoint" if stage.mode == "cc" else "current ceiling"
        power_label = "power setpoint" if stage.mode == "cp" else "power ceiling"

        ensure_below_active_threshold(
            f"Stage {stage.name!r} {current_label}",
            programmed.current_a,
            protections.ocp_a,
            "OCP",
        )
        ensure_below_active_threshold(
            f"Stage {stage.name!r} {power_label}",
            programmed.power_w,
            protections.opp_w,
            "OPP",
        )


def apply_stage(device, stage: ProfileStage, run_settings: RunSettings, limits: AdjustmentLimits) -> None:
    device.set_input_enabled(False)
    programmed = resolve_stage_setpoints(stage, limits)

    if stage.mode == "cv":
        device.set_current(programmed.current_a)
        device.set_power(programmed.power_w)
        device.set_voltage(programmed.voltage_v)
        device.set_input_enabled(True)
        time.sleep(run_settings.enable_settle_s)
    elif stage.mode == "cc":
        device.set_voltage(programmed.voltage_v)
        device.set_power(programmed.power_w)
        device.set_current(programmed.current_a)
        device.set_input_enabled(True)
        time.sleep(run_settings.enable_settle_s)
    elif stage.mode == "cp":
        device.set_voltage(programmed.voltage_v)
        device.set_current(programmed.current_a)
        device.set_power(programmed.power_w)
        device.set_input_enabled(True)
        time.sleep(run_settings.enable_settle_s)
    elif stage.mode == "cr":
        device.set_voltage(programmed.voltage_v)
        device.set_current(programmed.current_a)
        device.set_power(programmed.power_w)
        device.set_resistance(programmed.resistance_ohm)
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


def run_profile(config: ProfileConfig, battery_serial: str, profile_path: Path = DEFAULT_PROFILE_PATH) -> Path:
    log_path = build_log_path(config.run, battery_serial)
    text_log_path = build_text_log_path(log_path)
    started_at = datetime.now().astimezone()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    configure_logging_handlers(text_log_path)

    device = build_device(config)
    LOGGER.info("Loaded profile from %s", profile_path.resolve())
    LOGGER.info("Using %s", format_connection(config.connection))
    LOGGER.info("Battery serial: %s", battery_serial)

    with log_path.open("w", newline="", encoding="utf-8") as handle:
        write_csv_metadata_lines(
            handle,
            config=config,
            battery_serial=battery_serial,
            profile_path=profile_path,
            started_at=started_at,
        )
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()

        with device:
            log_device_connection(device)
            preflight_measurement = device.read_measurements() if hasattr(device, "read_measurements") else device.measure_all()

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
                active_limits = resolve_effective_limits(config.limits, read_adjustment_limits(device))
                active_protections = resolve_effective_protections(config.protections, read_protection_settings(device))
                LOGGER.info("Active protections for profile: %s", active_protections)
                LOGGER.info("Active limits for profile: %s", active_limits)
                validate_profile_against_active_device(
                    measurement=preflight_measurement,
                    protections=active_protections,
                    limits=active_limits,
                    stages=config.stages,
                )

                sample_index = 0

                for stage_index, stage in enumerate(config.stages, start=1):
                    LOGGER.info("Starting stage %d/%d: %s", stage_index, len(config.stages), stage)
                    apply_stage(device, stage, config.run, active_limits)
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
                                "timestamp": f"{elapsed_s:.3f}",
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
    LOGGER.info("Wrote text log to %s", text_log_path)
    return log_path


def main() -> None:
    configure_logging()
    profile_path = DEFAULT_PROFILE_PATH
    profile = load_profile(profile_path)
    battery_serial = prompt_battery_serial()
    run_profile(profile, battery_serial, profile_path)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        LOGGER.info("Stopped by user.")
