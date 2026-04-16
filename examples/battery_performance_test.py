from __future__ import annotations

"""
Battery performance and cycle test for the EA PSB 10060-60.

How to run:

    uv sync
    uv run python examples/battery_performance_test.py

This example is intended to be configured from the sibling YAML file instead of
editing Python code:

    uv run python examples/battery_performance_test.py

You can still override the connection on the command line or point at another
config file:

    uv run python examples/battery_performance_test.py --transport lan-scpi --host 192.168.0.50
    uv run python examples/battery_performance_test.py --config /tmp/my_profile.yaml
    uv run python examples/battery_performance_test.py --print-config > /tmp/expanded_profile.yaml

Battery-dependent defaults such as the charge voltage and source voltage
protections are derived from `battery_config` at runtime. To adapt the default
sequence without copying the full `stages` list, override stages by name under
`stage_overrides`.

The same script can:
- discharge a battery with a staged load profile
- rest between stages with output disabled
- recharge the battery with a CC/CV source profile
- repeat the full sequence for multiple cycles

Every sample is written to a timestamped CSV file under `logs/`.
"""

import argparse
import copy
import csv
import json
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import tomllib

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

YAML_ERROR = yaml.YAMLError if yaml is not None else ValueError  # type: ignore[union-attr]

from ea_driver import EAPSB10060_60
from ea_driver.config import (
    ConnectionSettings,
    add_connection_arguments,
    build_device_connection,
    deep_merge_dicts,
    format_connection,
    resolve_connection_settings,
)

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
LOGGER = logging.getLogger("battery_performance_test")
SECONDS_PER_MINUTE = 60.0
ENV_PREFIXES = ("EA_PSB_BATTERY", "EA_DRIVER")
DEFAULT_CONFIG_PATH = Path(__file__).with_suffix(".yaml")


# Default config blocks used when no external config file is supplied.
OUTPUT_CONFIG = {
    "run_name": "drone_battery_cycle_test",
    "log_directory": Path("logs"),
}

CONNECTION_CONFIG = {
    "transport": "usb-scpi",
    # For Ethernet SCPI instead:
    # "transport": "lan-scpi",
    "serial_port": None,
    "serial_glob": "*PSB_10060-60*",
    "host": "192.168.0.50",
    "lan_scpi_port": 5025,
    "baudrate": 115200,
    "timeout_s": 1.0,
}

BATTERY_CONFIG = {
    "series_cells": 6,
    "cutoff_per_cell_v": 3.30,
    "max_per_cell_v": 4.20,
    "discharge_cutoff_confirm_samples": 3,
    "charge_termination_confirm_samples": 5,
}

TEST_CONFIG = {
    "cycle_count": 1,
    "max_sink_power_w": 1500.0,
    "max_source_power_w": 300.0,
    "initial_settle_s": 1.0,
    "mode_switch_settle_s": 1.0,
    "ramp_step_a": 2.0,
    "ramp_step_settle_s": 1.0,
    "sample_interval_s": 1.0,
    "rest_sample_interval_s": 2.0,
    "log_every_n_samples": 10,
    "max_total_test_duration_s": 8.0 * 60.0 * 60.0,
}

# Hard protections use the instrument's dedicated OVP/OCP/OPP style thresholds.
# Supervision uses the configurable event system from the EA manual:
# UVD/UCD/OVD/OCD/OPD with actions like NONE, SIGNAL, WARNING, ALARM.
# For PSB sink mode the manual exposes UCD/OCD/OPD supervision, but not sink-side
# UVD/OVD. This script's discharge cutoff remains the main low-voltage protection.
#
# Defaults are derived from `battery_config` at runtime, so changing `series_cells`
# or `max_per_cell_v` also updates the default charge stage and source-side voltage
# protections.

# Supported stage kinds:
# - discharge: sink current until `duration_s` expires or cutoff is reached
# - charge: source CC/CV until charge termination criteria are met
# - rest: output off, observe open-circuit recovery for `duration_s`
#
# You can tweak a default stage without copying the full list by using:
# {
#   "stage_overrides": {
#     "sanity_1a": {"current_a": 0.5, "duration_s": 5.0},
#     "burst_50a": {"enabled": false}
#   }
# }

CSV_FIELDS = [
    "timestamp_utc",
    "cycle_index",
    "sample_index",
    "elapsed_test_s",
    "stage_kind",
    "stage_name",
    "stage_elapsed_s",
    "stage_duration_target_s",
    "command_current_a",
    "command_voltage_v",
    "charge_termination_current_a",
    "discharge_cutoff_voltage_v",
    "voltage_v",
    "current_a",
    "power_w",
    "discharge_current_a",
    "charge_current_a",
    "discharge_power_w",
    "charge_power_w",
    "discharged_ah",
    "charged_ah",
    "discharged_wh",
    "charged_wh",
    "net_ah_into_battery",
    "net_wh_into_battery",
    "voltage_sag_from_cycle_idle_v",
    "apparent_resistance_mohm_from_cycle_idle",
    "output_enabled",
    "remote_owner",
    "note",
]


def pack_cutoff_voltage_v(battery_config: dict[str, object]) -> float:
    return float(battery_config["series_cells"]) * float(battery_config["cutoff_per_cell_v"])


def pack_max_voltage_v(battery_config: dict[str, object]) -> float:
    return float(battery_config["series_cells"]) * float(battery_config["max_per_cell_v"])


def build_default_protection_config(battery_config: dict[str, object]) -> dict[str, object]:
    pack_max_v = pack_max_voltage_v(battery_config)
    return {
        "enabled": True,
        "source_hard_limits": {
            "voltage_protection_v": pack_max_v + 0.15,
            "current_protection_a": 6.0,
            "power_protection_w": 330.0,
        },
        "sink_hard_limits": {
            "current_protection_a": 55.0,
            "power_protection_w": 1500.0,
        },
        "source_supervision": {
            "OVD": {
                "threshold": pack_max_v + 0.10,
                "action": "ALARM",
            },
            "OCD": {"threshold": 6.0, "action": "ALARM"},
            "OPD": {"threshold": 330.0, "action": "ALARM"},
        },
        "sink_supervision": {
            "UCD": {"threshold": 0.5, "action": "WARNING"},
            "OCD": {"threshold": 55.0, "action": "ALARM"},
            "OPD": {"threshold": 1500.0, "action": "ALARM"},
        },
    }


def build_default_stages(battery_config: dict[str, object]) -> list[dict[str, object]]:
    return [
        {"kind": "discharge", "name": "sanity_1a", "current_a": 1.0, "duration_s": 30.0},
        {"kind": "discharge", "name": "burst_50a", "current_a": 50.0, "duration_s": 2.0 * SECONDS_PER_MINUTE},
        {"kind": "discharge", "name": "sustained_30a", "current_a": 30.0, "duration_s": 5.0 * SECONDS_PER_MINUTE},
        {"kind": "discharge", "name": "cruise_15a_to_cutoff", "current_a": 15.0, "duration_s": None},
        {"kind": "rest", "name": "post_discharge_recovery", "duration_s": 60.0},
        {
            "kind": "charge",
            "name": "recharge_cc_cv",
            "voltage_v": pack_max_voltage_v(battery_config),
            "current_a": 5.0,
            "termination_current_a": 1.0,
            "termination_voltage_margin_v": 0.05,
            "max_duration_s": 90.0 * SECONDS_PER_MINUTE,
        },
        {"kind": "rest", "name": "post_charge_recovery", "duration_s": 60.0},
    ]


def apply_stage_overrides(stages: list[dict[str, object]], stage_overrides: dict[str, object]) -> list[dict[str, object]]:
    known_names = {str(stage.get("name", "")) for stage in stages}
    unknown_names = sorted(set(stage_overrides) - known_names)
    if unknown_names:
        joined = ", ".join(unknown_names)
        raise SystemExit(f"Unknown stage overrides: {joined}")

    overridden_stages: list[dict[str, object]] = []
    for stage in stages:
        name = str(stage.get("name", ""))
        override = stage_overrides.get(name, {})
        if not isinstance(override, dict):
            raise SystemExit(f"Stage override for {name!r} must be an object / table.")

        enabled = override.get("enabled", True)
        if not enabled:
            continue

        merged_stage = copy.deepcopy(stage)
        override_values = {key: value for key, value in override.items() if key != "enabled"}
        if override_values:
            merged_stage = deep_merge_dicts(merged_stage, override_values)
        overridden_stages.append(merged_stage)

    return overridden_stages


def default_runtime_config(user_overrides: dict[str, object] | None = None) -> dict[str, object]:
    user_overrides = dict(user_overrides or {})
    runtime_config = {
        "output_config": copy.deepcopy(OUTPUT_CONFIG),
        "connection_config": copy.deepcopy(CONNECTION_CONFIG),
        "battery_config": copy.deepcopy(BATTERY_CONFIG),
        "test_config": copy.deepcopy(TEST_CONFIG),
        "stage_overrides": {},
    }

    plain_overrides = {key: value for key, value in user_overrides.items() if key not in {"protection_config", "stages"}}
    runtime_config = deep_merge_dicts(runtime_config, plain_overrides)

    runtime_config["protection_config"] = build_default_protection_config(runtime_config["battery_config"])
    if "protection_config" in user_overrides:
        runtime_config["protection_config"] = deep_merge_dicts(
            runtime_config["protection_config"],
            user_overrides["protection_config"],
        )

    stage_overrides = runtime_config["stage_overrides"]
    if not isinstance(stage_overrides, dict):
        raise SystemExit("stage_overrides must be an object / table.")

    base_stages = copy.deepcopy(user_overrides["stages"]) if "stages" in user_overrides else build_default_stages(runtime_config["battery_config"])
    runtime_config["stages"] = apply_stage_overrides(base_stages, stage_overrides)
    return {
        "output_config": runtime_config["output_config"],
        "connection_config": runtime_config["connection_config"],
        "battery_config": runtime_config["battery_config"],
        "test_config": runtime_config["test_config"],
        "protection_config": runtime_config["protection_config"],
        "stages": runtime_config["stages"],
        "stage_overrides": runtime_config["stage_overrides"],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a PSB battery charge / discharge cycle test.")
    parser.add_argument(
        "--config",
        type=Path,
        help="Optional YAML, JSON, or TOML file that overrides the runtime configuration. Defaults to the sibling YAML example file.",
    )
    add_connection_arguments(parser, transport_choices=("usb-scpi", "lan-scpi"))
    parser.add_argument("--run-name", help="Override output_config.run_name.")
    parser.add_argument("--log-directory", type=Path, help="Override output_config.log_directory.")
    parser.add_argument("--cycle-count", type=int, help="Override test_config.cycle_count.")
    parser.add_argument(
        "--print-config",
        action="store_true",
        help="Print the merged runtime configuration and exit without touching the device.",
    )
    parser.add_argument(
        "--print-config-format",
        choices=("yaml", "json"),
        default="yaml",
        help="Serialization format used by --print-config.",
    )
    return parser


def load_config_file(path: Path) -> dict[str, object]:
    text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    try:
        if suffix in {".yaml", ".yml"}:
            if yaml is None:
                raise SystemExit("YAML config support requires PyYAML. Install ea_driver with PyYAML available.")
            data = yaml.safe_load(text)
        elif suffix == ".json":
            data = json.loads(text)
        elif suffix == ".toml":
            data = tomllib.loads(text)
        else:
            raise SystemExit(f"Unsupported config format for {path}. Use .yaml, .yml, .json, or .toml.")
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON config in {path}: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise SystemExit(f"Invalid TOML config in {path}: {exc}") from exc
    except YAML_ERROR as exc:
        raise SystemExit(f"Invalid YAML config in {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise SystemExit(f"Config file {path} must contain a top-level object / table.")
    return data


def serialize_for_output(value: object) -> object:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): serialize_for_output(item) for key, item in value.items()}
    if isinstance(value, list):
        return [serialize_for_output(item) for item in value]
    return value


def render_config(runtime_config: dict[str, object], format_name: str) -> str:
    plain_config = serialize_for_output(runtime_config)
    if format_name == "json":
        return json.dumps(plain_config, indent=2, sort_keys=True) + "\n"
    if format_name == "yaml":
        if yaml is None:
            raise SystemExit("YAML config rendering requires PyYAML. Install ea_driver with PyYAML available.")
        return yaml.safe_dump(plain_config, sort_keys=False)
    raise ValueError(f"Unsupported config output format: {format_name}")


def connection_settings_from_mapping(values: dict) -> ConnectionSettings:
    return ConnectionSettings(
        transport=str(values.get("transport", "usb-scpi")),
        serial_port=values.get("serial_port"),
        serial_glob=values.get("serial_glob"),
        host=values.get("host"),
        lan_scpi_port=int(values.get("lan_scpi_port", 5025)),
        lan_modbus_port=int(values.get("lan_modbus_port", 502)),
        baudrate=int(values.get("baudrate", 115200)),
        unit_id=int(values.get("unit_id", 0)),
        timeout_s=float(values.get("timeout_s", 1.0)),
    )


def connection_settings_to_dict(settings: ConnectionSettings) -> dict[str, object]:
    return {
        "transport": settings.transport,
        "serial_port": settings.serial_port,
        "serial_glob": settings.serial_glob,
        "host": settings.host,
        "lan_scpi_port": settings.lan_scpi_port,
        "lan_modbus_port": settings.lan_modbus_port,
        "baudrate": settings.baudrate,
        "unit_id": settings.unit_id,
        "timeout_s": settings.timeout_s,
    }


def load_runtime_config(argv: list[str] | None = None) -> tuple[dict[str, object], bool, str]:
    args = build_parser().parse_args(argv)
    config_path = args.config or (DEFAULT_CONFIG_PATH if DEFAULT_CONFIG_PATH.exists() else None)
    config_overrides = load_config_file(config_path) if config_path is not None else None
    runtime_config = default_runtime_config(config_overrides)

    connection_defaults = connection_settings_from_mapping(dict(runtime_config["connection_config"]))
    runtime_config["connection_config"] = connection_settings_to_dict(
        resolve_connection_settings(
            defaults=connection_defaults,
            args=args,
            env_prefixes=ENV_PREFIXES,
        )
    )

    if args.run_name is not None:
        runtime_config["output_config"]["run_name"] = args.run_name
    if args.log_directory is not None:
        runtime_config["output_config"]["log_directory"] = args.log_directory
    if args.cycle_count is not None:
        runtime_config["test_config"]["cycle_count"] = args.cycle_count

    return runtime_config, args.print_config, args.print_config_format


@dataclass(slots=True)
class RunningStats:
    """Cumulative metrics collected across the entire run."""

    started_monotonic: float
    sample_index: int = 0
    discharged_ah: float = 0.0
    charged_ah: float = 0.0
    discharged_wh: float = 0.0
    charged_wh: float = 0.0
    last_sample_monotonic: float | None = None
    min_voltage_v: float = math.inf
    max_voltage_v: float = 0.0
    max_discharge_current_a: float = 0.0
    max_charge_current_a: float = 0.0
    max_discharge_power_w: float = 0.0
    max_charge_power_w: float = 0.0


class BatteryPerformanceTest:
    """Runs a configurable charge/discharge cycle test using the EA PSB."""

    def __init__(
        self,
        connection_config: dict,
        battery_config: dict,
        test_config: dict,
        stages: list[dict],
        protection_config: dict,
        output_config: dict,
    ) -> None:
        self.connection_config = dict(connection_config)
        self.battery_config = dict(battery_config)
        self.test_config = dict(test_config)
        self.output_config = dict(output_config)
        self.protection_config = dict(protection_config)
        self.stages = self.normalize_stages(stages)

        self.cutoff_voltage_v = (
            float(self.battery_config["series_cells"]) * float(self.battery_config["cutoff_per_cell_v"])
        )
        self.pack_max_voltage_v = (
            float(self.battery_config["series_cells"]) * float(self.battery_config["max_per_cell_v"])
        )
        self.discharge_cutoff_confirm_samples = int(self.battery_config["discharge_cutoff_confirm_samples"])
        self.charge_termination_confirm_samples = int(self.battery_config["charge_termination_confirm_samples"])
        self.max_total_test_duration_s = self.optional_float(self.test_config.get("max_total_test_duration_s"))
        self.csv_path = self.build_csv_path()

        self.device = None
        self.csv_handle = None
        self.csv_writer: csv.DictWriter | None = None
        self.initial_state: dict[str, float | str | bool] | None = None
        self.initial_protection_state: dict[str, object] | None = None
        self.stats: RunningStats | None = None

        self.cycle_index = 0
        self.cycle_idle_voltage_v = 0.0
        self.active_mode = "idle"
        self.command_current_a = 0.0
        self.command_voltage_v = 0.0
        self.end_reason = "completed all configured cycles"

    def normalize_stages(self, stages: list[dict]) -> list[dict[str, float | str | None]]:
        """Validate and normalize the editable stage configuration."""

        normalized: list[dict[str, float | str | None]] = []
        for index, stage in enumerate(stages, start=1):
            kind = str(stage.get("kind", "")).strip().lower()
            name = str(stage.get("name", f"stage_{index}"))

            if kind == "discharge":
                if "current_a" not in stage:
                    raise ValueError(f"Stage {index} is discharge but is missing 'current_a'")
                normalized.append(
                    {
                        "kind": kind,
                        "name": name,
                        "current_a": float(stage["current_a"]),
                        "duration_s": self.optional_float(stage.get("duration_s")),
                        "power_limit_w": self.optional_float(stage.get("power_limit_w"))
                        or float(self.test_config["max_sink_power_w"]),
                    }
                )
                continue

            if kind == "charge":
                for key in ["voltage_v", "current_a", "termination_current_a", "termination_voltage_margin_v"]:
                    if key not in stage:
                        raise ValueError(f"Stage {index} is charge but is missing '{key}'")
                normalized.append(
                    {
                        "kind": kind,
                        "name": name,
                        "voltage_v": float(stage["voltage_v"]),
                        "current_a": float(stage["current_a"]),
                        "termination_current_a": float(stage["termination_current_a"]),
                        "termination_voltage_margin_v": float(stage["termination_voltage_margin_v"]),
                        "max_duration_s": self.optional_float(stage.get("max_duration_s")),
                        "power_limit_w": self.optional_float(stage.get("power_limit_w"))
                        or float(self.test_config["max_source_power_w"]),
                    }
                )
                continue

            if kind == "rest":
                if "duration_s" not in stage:
                    raise ValueError(f"Stage {index} is rest but is missing 'duration_s'")
                normalized.append(
                    {
                        "kind": kind,
                        "name": name,
                        "duration_s": float(stage["duration_s"]),
                    }
                )
                continue

            raise ValueError(f"Unsupported stage kind for stage {index}: {kind!r}")

        return normalized

    def optional_float(self, value: object) -> float | None:
        if value is None:
            return None
        return float(value)

    def build_csv_path(self) -> Path:
        """Create a timestamped CSV filename in the configured log directory."""

        log_directory = Path(self.output_config["log_directory"])
        log_directory.mkdir(parents=True, exist_ok=True)
        run_name = self.sanitize_filename(str(self.output_config.get("run_name", "battery_performance_test")))
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return log_directory / f"{timestamp}_{run_name}.csv"

    def sanitize_filename(self, value: str) -> str:
        safe = "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in value)
        while "__" in safe:
            safe = safe.replace("__", "_")
        return safe.strip("_") or "battery_performance_test"

    def connection_settings(self) -> ConnectionSettings:
        return connection_settings_from_mapping(self.connection_config)

    def build_device(self):
        """Construct the PSB transport from the runtime connection config."""

        return build_device_connection(EAPSB10060_60, self.connection_settings())

    def timestamp_utc(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def has_exceeded_total_duration(self) -> bool:
        if self.max_total_test_duration_s is None or self.stats is None:
            return False
        return time.monotonic() - self.stats.started_monotonic >= self.max_total_test_duration_s

    def capture_initial_state(self) -> dict[str, float | str | bool]:
        return {
            "remote_owner": self.device.remote_owner(),
            "output_enabled": self.device.is_output_enabled(),
            "power_stage_mode": self.device.power_stage_mode(),
            "voltage_v": self.device.get_voltage_setpoint(),
            "source_current_a": self.device.get_source_current_setpoint(),
            "source_power_w": self.device.get_source_power_setpoint(),
            "source_resistance_ohm": self.device.get_source_resistance_setpoint(),
            "sink_current_a": self.device.get_sink_current_setpoint(),
            "sink_power_w": self.device.get_sink_power_setpoint(),
            "sink_resistance_ohm": self.device.get_sink_resistance_setpoint(),
        }

    def capture_initial_protection_state(self) -> dict[str, object]:
        """Capture the protection and supervision settings this script may overwrite."""

        if not self.protection_config.get("enabled", False):
            return {}

        state: dict[str, object] = {
            "source_hard_limits": {},
            "sink_hard_limits": {},
            "source_supervision": {},
            "sink_supervision": {},
        }

        source_hard_limits = self.protection_config.get("source_hard_limits", {})
        if source_hard_limits.get("voltage_protection_v") is not None:
            state["source_hard_limits"]["voltage_protection_v"] = self.device.get_source_voltage_protection()
        if source_hard_limits.get("current_protection_a") is not None:
            state["source_hard_limits"]["current_protection_a"] = self.device.get_source_current_protection()
        if source_hard_limits.get("power_protection_w") is not None:
            state["source_hard_limits"]["power_protection_w"] = self.device.get_source_power_protection()

        sink_hard_limits = self.protection_config.get("sink_hard_limits", {})
        if sink_hard_limits.get("current_protection_a") is not None:
            state["sink_hard_limits"]["current_protection_a"] = self.device.get_sink_current_protection()
        if sink_hard_limits.get("power_protection_w") is not None:
            state["sink_hard_limits"]["power_protection_w"] = self.device.get_sink_power_protection()

        for event in self.protection_config.get("source_supervision", {}):
            threshold, action = self.device.read_source_supervision(event)
            state["source_supervision"][event] = {"threshold": threshold, "action": action}

        for event in self.protection_config.get("sink_supervision", {}):
            threshold, action = self.device.read_sink_supervision(event)
            state["sink_supervision"][event] = {"threshold": threshold, "action": action}

        return state

    def apply_protection_config(self) -> None:
        """Apply the configured hard protections and supervision thresholds."""

        if not self.protection_config.get("enabled", False):
            LOGGER.info("Remote protection configuration is disabled")
            return

        source_hard_limits = self.protection_config.get("source_hard_limits", {})
        if source_hard_limits.get("voltage_protection_v") is not None:
            self.device.set_source_voltage_protection(float(source_hard_limits["voltage_protection_v"]))
        if source_hard_limits.get("current_protection_a") is not None:
            self.device.set_source_current_protection(float(source_hard_limits["current_protection_a"]))
        if source_hard_limits.get("power_protection_w") is not None:
            self.device.set_source_power_protection(float(source_hard_limits["power_protection_w"]))

        sink_hard_limits = self.protection_config.get("sink_hard_limits", {})
        if sink_hard_limits.get("current_protection_a") is not None:
            self.device.set_sink_current_protection(float(sink_hard_limits["current_protection_a"]))
        if sink_hard_limits.get("power_protection_w") is not None:
            self.device.set_sink_power_protection(float(sink_hard_limits["power_protection_w"]))

        for event, supervision in self.protection_config.get("source_supervision", {}).items():
            self.device.configure_source_supervision(
                event,
                threshold=float(supervision["threshold"]),
                action=str(supervision["action"]),
            )
        for event, supervision in self.protection_config.get("sink_supervision", {}).items():
            self.device.configure_sink_supervision(
                event,
                threshold=float(supervision["threshold"]),
                action=str(supervision["action"]),
            )

        LOGGER.info("Applied configured hard protections and supervision thresholds")

    def restore_initial_protection_state(self) -> list[str]:
        """Restore the protection and supervision settings captured before the run."""

        if not self.initial_protection_state:
            return []

        cleanup_errors: list[str] = []
        try:
            source_hard_limits = self.initial_protection_state.get("source_hard_limits", {})
            if "voltage_protection_v" in source_hard_limits:
                self.device.set_source_voltage_protection(float(source_hard_limits["voltage_protection_v"]))
            if "current_protection_a" in source_hard_limits:
                self.device.set_source_current_protection(float(source_hard_limits["current_protection_a"]))
            if "power_protection_w" in source_hard_limits:
                self.device.set_source_power_protection(float(source_hard_limits["power_protection_w"]))

            sink_hard_limits = self.initial_protection_state.get("sink_hard_limits", {})
            if "current_protection_a" in sink_hard_limits:
                self.device.set_sink_current_protection(float(sink_hard_limits["current_protection_a"]))
            if "power_protection_w" in sink_hard_limits:
                self.device.set_sink_power_protection(float(sink_hard_limits["power_protection_w"]))

            for event, supervision in self.initial_protection_state.get("source_supervision", {}).items():
                self.device.configure_source_supervision(
                    event,
                    threshold=float(supervision["threshold"]),
                    action=str(supervision["action"]),
                )
            for event, supervision in self.initial_protection_state.get("sink_supervision", {}).items():
                self.device.configure_sink_supervision(
                    event,
                    threshold=float(supervision["threshold"]),
                    action=str(supervision["action"]),
                )
        except Exception as exc:  # pragma: no cover - cleanup path
            cleanup_errors.append(f"restore_protections: {exc!r}")

        return cleanup_errors

    def restore_initial_state(self) -> list[str]:
        """Restore the instrument configuration that existed before the script ran."""

        cleanup_errors: list[str] = []
        if self.initial_state is None:
            return cleanup_errors

        for label, action in [
            ("disable_output", lambda: self.device.set_output_enabled(False)),
            ("restore_mode", lambda: self.device.set_power_stage_mode(str(self.initial_state["power_stage_mode"]))),
            ("restore_voltage", lambda: self.device.set_voltage(float(self.initial_state["voltage_v"]))),
            ("restore_source_current", lambda: self.device.set_source_current(float(self.initial_state["source_current_a"]))),
            ("restore_source_power", lambda: self.device.set_source_power(float(self.initial_state["source_power_w"]))),
            (
                "restore_source_resistance",
                lambda: self.device.set_source_resistance(float(self.initial_state["source_resistance_ohm"])),
            ),
            ("restore_sink_current", lambda: self.device.set_sink_current(float(self.initial_state["sink_current_a"]))),
            ("restore_sink_power", lambda: self.device.set_sink_power(float(self.initial_state["sink_power_w"]))),
            (
                "restore_sink_resistance",
                lambda: self.device.set_sink_resistance(float(self.initial_state["sink_resistance_ohm"])),
            ),
            ("release_remote", lambda: self.device.set_remote(False)),
        ]:
            try:
                action()
            except Exception as exc:  # pragma: no cover - cleanup path
                cleanup_errors.append(f"{label}: {exc!r}")

        return cleanup_errors

    def open_csv_log(self) -> None:
        self.csv_handle = self.csv_path.open("w", newline="", encoding="utf-8")
        self.csv_writer = csv.DictWriter(self.csv_handle, fieldnames=CSV_FIELDS)
        self.csv_writer.writeheader()

    def close_csv_log(self) -> None:
        if self.csv_handle is not None:
            self.csv_handle.close()
            self.csv_handle = None
            self.csv_writer = None

    def validate_pack_voltage_range(self, voltage_v: float) -> None:
        if voltage_v > self.pack_max_voltage_v + 0.5:
            raise RuntimeError(
                f"Measured pack voltage {voltage_v:.3f} V exceeds the expected "
                f"{int(self.battery_config['series_cells'])}S range"
            )

    def discharge_current_from_measurement(self, measurement, stage_kind: str) -> float:
        if stage_kind != "discharge":
            return 0.0
        return abs(measurement.current_a)

    def charge_current_from_measurement(self, measurement, stage_kind: str) -> float:
        if stage_kind != "charge":
            return 0.0
        return abs(measurement.current_a)

    def discharge_power_from_measurement(self, measurement, stage_kind: str) -> float:
        if stage_kind != "discharge":
            return 0.0
        return abs(measurement.power_w)

    def charge_power_from_measurement(self, measurement, stage_kind: str) -> float:
        if stage_kind != "charge":
            return 0.0
        return abs(measurement.power_w)

    def stage_target_duration(self, stage: dict[str, float | str | None]) -> float | str:
        if stage["kind"] == "charge":
            return "" if stage["max_duration_s"] is None else float(stage["max_duration_s"])
        if stage["kind"] in {"discharge", "rest"}:
            return "" if stage["duration_s"] is None else float(stage["duration_s"])
        return ""

    def stage_charge_termination_current(self, stage: dict[str, float | str | None]) -> float | str:
        if stage["kind"] != "charge":
            return ""
        return float(stage["termination_current_a"])

    def record_sample(self, stage: dict[str, float | str | None], phase_started_monotonic: float, note: str):
        """Measure the battery, update cumulative metrics, and append one CSV row."""

        measurement = self.device.measure_all()
        now = time.monotonic()
        dt_s = 0.0 if self.stats.last_sample_monotonic is None else now - self.stats.last_sample_monotonic

        stage_kind = str(stage["kind"])
        discharge_current_a = self.discharge_current_from_measurement(measurement, stage_kind)
        charge_current_a = self.charge_current_from_measurement(measurement, stage_kind)
        discharge_power_w = self.discharge_power_from_measurement(measurement, stage_kind)
        charge_power_w = self.charge_power_from_measurement(measurement, stage_kind)

        self.stats.discharged_ah += discharge_current_a * dt_s / 3600.0
        self.stats.charged_ah += charge_current_a * dt_s / 3600.0
        self.stats.discharged_wh += discharge_power_w * dt_s / 3600.0
        self.stats.charged_wh += charge_power_w * dt_s / 3600.0
        self.stats.last_sample_monotonic = now
        self.stats.sample_index += 1
        self.stats.min_voltage_v = min(self.stats.min_voltage_v, measurement.voltage_v)
        self.stats.max_voltage_v = max(self.stats.max_voltage_v, measurement.voltage_v)
        self.stats.max_discharge_current_a = max(self.stats.max_discharge_current_a, discharge_current_a)
        self.stats.max_charge_current_a = max(self.stats.max_charge_current_a, charge_current_a)
        self.stats.max_discharge_power_w = max(self.stats.max_discharge_power_w, discharge_power_w)
        self.stats.max_charge_power_w = max(self.stats.max_charge_power_w, charge_power_w)

        voltage_sag_from_cycle_idle_v = self.cycle_idle_voltage_v - measurement.voltage_v
        apparent_resistance_mohm = ""
        if discharge_current_a >= 0.5 and voltage_sag_from_cycle_idle_v > 0.0:
            apparent_resistance_mohm = 1000.0 * voltage_sag_from_cycle_idle_v / discharge_current_a

        self.csv_writer.writerow(
            {
                "timestamp_utc": self.timestamp_utc(),
                "cycle_index": self.cycle_index,
                "sample_index": self.stats.sample_index,
                "elapsed_test_s": now - self.stats.started_monotonic,
                "stage_kind": stage["kind"],
                "stage_name": stage["name"],
                "stage_elapsed_s": now - phase_started_monotonic,
                "stage_duration_target_s": self.stage_target_duration(stage),
                "command_current_a": self.command_current_a,
                "command_voltage_v": self.command_voltage_v,
                "charge_termination_current_a": self.stage_charge_termination_current(stage),
                "discharge_cutoff_voltage_v": self.cutoff_voltage_v,
                "voltage_v": measurement.voltage_v,
                "current_a": measurement.current_a,
                "power_w": measurement.power_w,
                "discharge_current_a": discharge_current_a,
                "charge_current_a": charge_current_a,
                "discharge_power_w": discharge_power_w,
                "charge_power_w": charge_power_w,
                "discharged_ah": self.stats.discharged_ah,
                "charged_ah": self.stats.charged_ah,
                "discharged_wh": self.stats.discharged_wh,
                "charged_wh": self.stats.charged_wh,
                "net_ah_into_battery": self.stats.charged_ah - self.stats.discharged_ah,
                "net_wh_into_battery": self.stats.charged_wh - self.stats.discharged_wh,
                "voltage_sag_from_cycle_idle_v": voltage_sag_from_cycle_idle_v,
                "apparent_resistance_mohm_from_cycle_idle": apparent_resistance_mohm,
                "output_enabled": self.device.is_output_enabled(),
                "remote_owner": self.device.remote_owner(),
                "note": note,
            }
        )
        self.csv_handle.flush()

        log_every = int(self.test_config["log_every_n_samples"])
        if self.stats.sample_index == 1 or self.stats.sample_index % log_every == 0 or note != "hold":
            LOGGER.info(
                "Cycle %d sample %d stage=%s note=%s set=%.1f A target_v=%.2f V measured=%.3f V %.3f A %.3f W",
                self.cycle_index,
                self.stats.sample_index,
                stage["name"],
                note,
                self.command_current_a,
                self.command_voltage_v,
                measurement.voltage_v,
                measurement.current_a,
                measurement.power_w,
            )

        return measurement

    def set_remote_control(self) -> None:
        self.device.clear_status()
        if self.device.is_output_enabled():
            self.device.set_output_enabled(False)
            time.sleep(0.5)
        self.device.set_remote(True)
        time.sleep(0.3)

    def ramp_active_current_to_zero(self, stage: dict[str, float | str | None], note: str) -> None:
        """Ramp the active source or sink current back to zero before mode changes."""

        if self.command_current_a <= 0.0:
            return

        phase_started_monotonic = time.monotonic()
        ramp_step_a = float(self.test_config["ramp_step_a"])
        settle_s = float(self.test_config["ramp_step_settle_s"])

        while self.command_current_a > 0.0:
            self.command_current_a = max(0.0, self.command_current_a - ramp_step_a)
            if self.active_mode == "discharge":
                self.device.set_sink_current(self.command_current_a)
            elif self.active_mode == "charge":
                self.device.set_source_current(self.command_current_a)
            else:
                break
            time.sleep(settle_s)
            self.record_sample(stage, phase_started_monotonic, note)

    def prepare_idle_mode(self, stage: dict[str, float | str | None]) -> None:
        """Disable output and leave the battery at open circuit for a rest stage."""

        self.ramp_active_current_to_zero(stage, "ramp_down")
        if self.device.is_output_enabled():
            self.device.set_output_enabled(False)
            time.sleep(float(self.test_config["mode_switch_settle_s"]))
        self.active_mode = "idle"
        self.command_current_a = 0.0
        self.command_voltage_v = 0.0

    def prepare_discharge_mode(self, stage: dict[str, float | str | None]) -> None:
        """Configure the PSB as a sink for the requested discharge stage."""

        if self.active_mode != "discharge":
            self.ramp_active_current_to_zero(stage, "ramp_down_for_mode_switch")
            if self.device.is_output_enabled():
                self.device.set_output_enabled(False)
                time.sleep(float(self.test_config["mode_switch_settle_s"]))
            self.device.set_resistance_mode_enabled(False)
            self.device.set_sink_only_mode()
            self.device.set_sink_current(0.0)
            self.command_current_a = 0.0
            self.command_voltage_v = 0.0
            self.active_mode = "discharge"

        self.device.set_sink_power(float(stage["power_limit_w"]))
        if not self.device.is_output_enabled():
            self.device.set_output_enabled(True)
            time.sleep(float(self.test_config["mode_switch_settle_s"]))

    def prepare_charge_mode(self, stage: dict[str, float | str | None]) -> None:
        """Configure the PSB as a source for the requested CC/CV charge stage."""

        if self.active_mode != "charge":
            self.ramp_active_current_to_zero(stage, "ramp_down_for_mode_switch")
            if self.device.is_output_enabled():
                self.device.set_output_enabled(False)
                time.sleep(float(self.test_config["mode_switch_settle_s"]))
            self.device.set_resistance_mode_enabled(False)
            self.device.set_source_only_mode()
            self.device.set_source_current(0.0)
            self.command_current_a = 0.0
            self.active_mode = "charge"

        self.command_voltage_v = float(stage["voltage_v"])
        self.device.set_voltage(self.command_voltage_v)
        self.device.set_source_power(float(stage["power_limit_w"]))
        if not self.device.is_output_enabled():
            self.device.set_output_enabled(True)
            time.sleep(float(self.test_config["mode_switch_settle_s"]))

    def ramp_current_to_target(self, stage: dict[str, float | str | None], target_current_a: float, setter_name: str) -> None:
        """Ramp source or sink current gradually to reduce abrupt current steps."""

        phase_started_monotonic = time.monotonic()
        ramp_step_a = float(self.test_config["ramp_step_a"])
        settle_s = float(self.test_config["ramp_step_settle_s"])

        while not math.isclose(self.command_current_a, target_current_a, abs_tol=1e-6):
            direction = 1.0 if target_current_a > self.command_current_a else -1.0
            next_current_a = self.command_current_a + direction * ramp_step_a
            if direction > 0.0:
                next_current_a = min(next_current_a, target_current_a)
            else:
                next_current_a = max(next_current_a, target_current_a)

            self.command_current_a = next_current_a
            if setter_name == "sink":
                self.device.set_sink_current(self.command_current_a)
            else:
                self.device.set_source_current(self.command_current_a)
            time.sleep(settle_s)
            self.record_sample(stage, phase_started_monotonic, "ramp")

            if self.has_exceeded_total_duration():
                raise RuntimeError("max total test duration reached")

    def record_cycle_idle(self) -> float:
        """Measure and log the open-circuit voltage at the start of a cycle."""

        idle_stage = {"kind": "idle", "name": "cycle_idle", "duration_s": 0.0}
        self.prepare_idle_mode(idle_stage)
        measurement = self.device.measure_all()
        self.validate_pack_voltage_range(measurement.voltage_v)
        self.cycle_idle_voltage_v = measurement.voltage_v
        self.record_sample(idle_stage, time.monotonic(), "cycle_start")
        return measurement.voltage_v

    def run_discharge_stage(self, stage: dict[str, float | str | None]) -> str | None:
        """Run one discharge stage until timeout or low-voltage cutoff."""

        self.prepare_discharge_mode(stage)
        self.ramp_current_to_target(stage, float(stage["current_a"]), "sink")
        phase_started_monotonic = time.monotonic()
        cutoff_counter = 0
        duration_s = self.optional_float(stage["duration_s"])

        while True:
            measurement = self.record_sample(stage, phase_started_monotonic, "hold")
            if measurement.voltage_v <= self.cutoff_voltage_v:
                cutoff_counter += 1
            else:
                cutoff_counter = 0
            if cutoff_counter >= self.discharge_cutoff_confirm_samples:
                return "discharge_cutoff"
            if duration_s is not None and time.monotonic() - phase_started_monotonic >= duration_s:
                return None
            if self.has_exceeded_total_duration():
                return "max total test duration reached"
            time.sleep(float(self.test_config["sample_interval_s"]))

    def run_charge_stage(self, stage: dict[str, float | str | None]) -> str | None:
        """Run one CC/CV charge stage until taper-current termination or timeout."""

        self.prepare_charge_mode(stage)
        self.ramp_current_to_target(stage, float(stage["current_a"]), "source")
        phase_started_monotonic = time.monotonic()
        charge_done_counter = 0
        target_voltage_v = float(stage["voltage_v"])
        termination_current_a = float(stage["termination_current_a"])
        voltage_margin_v = float(stage["termination_voltage_margin_v"])
        max_duration_s = self.optional_float(stage["max_duration_s"])

        while True:
            measurement = self.record_sample(stage, phase_started_monotonic, "hold")
            if (
                measurement.voltage_v >= target_voltage_v - voltage_margin_v
                and measurement.current_a >= 0.0
                and measurement.current_a <= termination_current_a
            ):
                charge_done_counter += 1
            else:
                charge_done_counter = 0

            if charge_done_counter >= self.charge_termination_confirm_samples:
                return None
            if max_duration_s is not None and time.monotonic() - phase_started_monotonic >= max_duration_s:
                return "charge stage hit max_duration_s before termination"
            if self.has_exceeded_total_duration():
                return "max total test duration reached"
            time.sleep(float(self.test_config["sample_interval_s"]))

    def run_rest_stage(self, stage: dict[str, float | str | None]) -> str | None:
        """Run one rest stage with output disabled to observe recovery."""

        self.prepare_idle_mode(stage)
        phase_started_monotonic = time.monotonic()
        duration_s = float(stage["duration_s"])
        rest_sample_interval_s = float(self.test_config["rest_sample_interval_s"])

        while True:
            self.record_sample(stage, phase_started_monotonic, "rest")
            if time.monotonic() - phase_started_monotonic >= duration_s:
                return None
            if self.has_exceeded_total_duration():
                return "max total test duration reached"
            time.sleep(rest_sample_interval_s)

    def run_stage(self, stage: dict[str, float | str | None]) -> str | None:
        """Dispatch one normalized stage to the appropriate runner."""

        LOGGER.info("Cycle %d starting %s stage %s", self.cycle_index, stage["kind"], stage["name"])
        if stage["kind"] == "discharge":
            return self.run_discharge_stage(stage)
        if stage["kind"] == "charge":
            return self.run_charge_stage(stage)
        if stage["kind"] == "rest":
            return self.run_rest_stage(stage)
        raise ValueError(f"Unsupported stage kind: {stage['kind']}")

    def cleanup_device(self) -> None:
        """Ramp down current, disable output, restore settings, and report instrument errors."""

        cleanup_stage = {"kind": "cleanup", "name": "cleanup", "duration_s": 0.0}
        try:
            self.prepare_idle_mode(cleanup_stage)
        except Exception:  # pragma: no cover - cleanup path
            LOGGER.exception("Failed to bring the PSB back to idle before cleanup")

        protection_cleanup_errors = self.restore_initial_protection_state()
        state_cleanup_errors = self.restore_initial_state()
        cleanup_errors = protection_cleanup_errors + state_cleanup_errors
        if cleanup_errors:
            LOGGER.warning("Cleanup issues: %s", cleanup_errors)

        errors = [error for error in self.device.read_errors(max_errors=10) if not error.startswith("0,")]
        if errors:
            LOGGER.warning("SCPI error queue: %s", errors)

    def log_summary(self) -> None:
        if self.stats is None:
            return
        LOGGER.info(
            "Finished battery cycle test: reason=%s, samples=%d, discharged=%.3f Ah %.3f Wh, charged=%.3f Ah %.3f Wh, min_voltage=%.3f V, max_voltage=%.3f V, max_discharge=%.3f A %.3f W, max_charge=%.3f A %.3f W",
            self.end_reason,
            self.stats.sample_index,
            self.stats.discharged_ah,
            self.stats.discharged_wh,
            self.stats.charged_ah,
            self.stats.charged_wh,
            self.stats.min_voltage_v,
            self.stats.max_voltage_v,
            self.stats.max_discharge_current_a,
            self.stats.max_discharge_power_w,
            self.stats.max_charge_current_a,
            self.stats.max_charge_power_w,
        )
        LOGGER.info("CSV log written to %s", self.csv_path)

    def run_cycle(self, cycle_index: int) -> str | None:
        """Run one configured sequence of stages."""

        self.cycle_index = cycle_index
        LOGGER.info("Starting cycle %d/%d", cycle_index, int(self.test_config["cycle_count"]))
        cycle_idle_voltage_v = self.record_cycle_idle()
        discharge_blocked = cycle_idle_voltage_v <= self.cutoff_voltage_v
        if discharge_blocked:
            LOGGER.warning(
                "Cycle %d starts at %.3f V which is at or below the discharge cutoff %.3f V; discharge stages will be skipped until a charge stage runs",
                cycle_index,
                cycle_idle_voltage_v,
                self.cutoff_voltage_v,
            )

        for stage in self.stages:
            if discharge_blocked and stage["kind"] == "discharge":
                LOGGER.info("Skipping discharge stage %s in cycle %d because the pack is already at cutoff", stage["name"], cycle_index)
                continue

            stage_result = self.run_stage(stage)
            if stage_result == "discharge_cutoff":
                LOGGER.info(
                    "Discharge cutoff reached in cycle %d during stage %s; remaining discharge stages in this cycle will be skipped",
                    cycle_index,
                    stage["name"],
                )
                discharge_blocked = True
                continue
            if stage_result is not None:
                return stage_result

            if stage["kind"] == "charge":
                discharge_blocked = False

        return None

    def run(self) -> None:
        """Execute the configured number of charge/discharge cycles."""

        self.open_csv_log()
        try:
            LOGGER.info("Using %s", format_connection(self.connection_settings()))
            with self.build_device() as device:
                self.device = device
                LOGGER.info("Connected to %s", self.device.identify())
                self.initial_state = self.capture_initial_state()
                self.stats = RunningStats(started_monotonic=time.monotonic())

                try:
                    self.set_remote_control()
                    self.initial_protection_state = self.capture_initial_protection_state()
                    self.apply_protection_config()
                    initial_measurement = self.device.measure_all()
                    self.validate_pack_voltage_range(initial_measurement.voltage_v)

                    for cycle_index in range(1, int(self.test_config["cycle_count"]) + 1):
                        cycle_result = self.run_cycle(cycle_index)
                        if cycle_result is not None:
                            self.end_reason = cycle_result
                            break
                finally:
                    self.cleanup_device()

                self.log_summary()
        finally:
            self.close_csv_log()


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    runtime_config, print_config, print_config_format = load_runtime_config(argv)
    if print_config:
        print(render_config(runtime_config, print_config_format), end="")
        return

    tester = BatteryPerformanceTest(
        connection_config=runtime_config["connection_config"],
        battery_config=runtime_config["battery_config"],
        test_config=runtime_config["test_config"],
        stages=runtime_config["stages"],
        protection_config=runtime_config["protection_config"],
        output_config=runtime_config["output_config"],
    )
    tester.run()


if __name__ == "__main__":
    main()
