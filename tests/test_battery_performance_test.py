import json
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

from ea_driver.core import Measurement


def load_battery_module():
    script_path = Path(__file__).resolve().parent.parent / "examples" / "battery_performance_test.py"
    spec = spec_from_file_location("battery_performance_test", script_path)
    module = module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_load_runtime_config_merges_file_and_cli_overrides(tmp_path):
    battery_module = load_battery_module()
    config_path = tmp_path / "battery.json"
    config_path.write_text(
        json.dumps(
            {
                "output_config": {"run_name": "from-file"},
                "connection_config": {"transport": "lan-scpi", "host": "10.0.0.2"},
                "test_config": {"cycle_count": 4},
            }
        ),
        encoding="utf-8",
    )

    runtime_config, print_config, print_format = battery_module.load_runtime_config(
        [
            "--config",
            str(config_path),
            "--cycle-count",
            "2",
            "--run-name",
            "from-cli",
        ]
    )

    assert print_config is False
    assert print_format == "yaml"
    assert runtime_config["output_config"]["run_name"] == "from-cli"
    assert runtime_config["connection_config"]["transport"] == "lan-scpi"
    assert runtime_config["connection_config"]["host"] == "10.0.0.2"
    assert runtime_config["test_config"]["cycle_count"] == 2


def test_load_runtime_config_recomputes_derived_defaults_from_battery_config(tmp_path):
    battery_module = load_battery_module()
    config_path = tmp_path / "battery.json"
    config_path.write_text(
        json.dumps(
            {
                "battery_config": {
                    "series_cells": 8,
                    "max_per_cell_v": 4.15,
                }
            }
        ),
        encoding="utf-8",
    )

    runtime_config, _, _ = battery_module.load_runtime_config(["--config", str(config_path)])
    charge_stage = next(stage for stage in runtime_config["stages"] if stage["name"] == "recharge_cc_cv")

    assert runtime_config["protection_config"]["source_hard_limits"]["voltage_protection_v"] == pytest.approx(33.35)
    assert runtime_config["protection_config"]["source_supervision"]["OVD"]["threshold"] == pytest.approx(33.30)
    assert charge_stage["voltage_v"] == pytest.approx(33.20)


def test_load_runtime_config_applies_stage_overrides_by_name(tmp_path):
    battery_module = load_battery_module()
    config_path = tmp_path / "battery.json"
    config_path.write_text(
        json.dumps(
            {
                "stage_overrides": {
                    "sanity_1a": {"current_a": 0.25, "duration_s": 2.0},
                    "burst_50a": {"enabled": False},
                }
            }
        ),
        encoding="utf-8",
    )

    runtime_config, _, _ = battery_module.load_runtime_config(["--config", str(config_path)])

    assert runtime_config["stages"][0]["name"] == "sanity_1a"
    assert runtime_config["stages"][0]["current_a"] == pytest.approx(0.25)
    assert runtime_config["stages"][0]["duration_s"] == pytest.approx(2.0)
    assert all(stage["name"] != "burst_50a" for stage in runtime_config["stages"])


def test_load_runtime_config_rejects_unknown_stage_override(tmp_path):
    battery_module = load_battery_module()
    config_path = tmp_path / "battery.json"
    config_path.write_text(
        json.dumps(
            {
                "stage_overrides": {
                    "does_not_exist": {"enabled": False},
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit, match="Unknown stage overrides"):
        battery_module.load_runtime_config(["--config", str(config_path)])


def test_load_runtime_config_supports_yaml(tmp_path):
    battery_module = load_battery_module()
    config_path = tmp_path / "battery.yaml"
    config_path.write_text(
        "\n".join(
            [
                "battery_config:",
                "  series_cells: 8",
                "stage_overrides:",
                "  sanity_1a:",
                "    current_a: 0.4",
            ]
        ),
        encoding="utf-8",
    )

    runtime_config, _, _ = battery_module.load_runtime_config(["--config", str(config_path)])

    assert runtime_config["battery_config"]["series_cells"] == 8
    assert runtime_config["stages"][0]["current_a"] == pytest.approx(0.4)


def test_stage_accounting_uses_stage_kind_not_measurement_sign():
    battery_module = load_battery_module()
    runtime_config = battery_module.default_runtime_config(
        {
            "stage_overrides": {
                "burst_50a": {"enabled": False},
                "sustained_30a": {"enabled": False},
                "cruise_15a_to_cutoff": {"enabled": False},
                "recharge_cc_cv": {"enabled": False},
                "post_charge_recovery": {"enabled": False},
            }
        }
    )
    tester = battery_module.BatteryPerformanceTest(
        connection_config=runtime_config["connection_config"],
        battery_config=runtime_config["battery_config"],
        test_config=runtime_config["test_config"],
        stages=runtime_config["stages"],
        protection_config=runtime_config["protection_config"],
        output_config=runtime_config["output_config"],
    )
    measurement = Measurement(voltage_v=31.7, current_a=0.06, power_w=2.0)

    assert tester.discharge_current_from_measurement(measurement, "discharge") == pytest.approx(0.06)
    assert tester.discharge_power_from_measurement(measurement, "discharge") == pytest.approx(2.0)
    assert tester.charge_current_from_measurement(measurement, "charge") == pytest.approx(0.06)
    assert tester.charge_power_from_measurement(measurement, "charge") == pytest.approx(2.0)
    assert tester.discharge_current_from_measurement(measurement, "rest") == 0.0
    assert tester.charge_power_from_measurement(measurement, "idle") == 0.0
