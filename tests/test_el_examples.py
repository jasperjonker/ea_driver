import sys
from datetime import datetime, timezone
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest


def load_example_module(filename: str, module_name: str):
    script_path = Path(__file__).resolve().parent.parent / "examples" / filename
    spec = spec_from_file_location(module_name, script_path)
    module = module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_el_complete_build_log_path_uses_timestamp_and_serial():
    example_module = load_example_module("el_complete.py", "el_complete_example")

    log_path = example_module.build_log_path(
        example_module.DEFAULT_CONFIG,
        "Pack 01/02",
        now=datetime(2026, 4, 21, 13, 14, 15),
    )

    assert log_path == Path("logging/20260421_131415_Pack-01-02_el_cc_discharge.csv")


def test_el_profile_build_text_log_path_uses_same_stem():
    example_module = load_example_module("el_profile.py", "el_profile_text_log")

    assert example_module.build_text_log_path(Path("logging/run.csv")) == Path("logging/run.log")


def test_el_profile_loads_yaml_and_applies_stage_cutoff_only_when_explicit(tmp_path):
    example_module = load_example_module("el_profile.py", "el_profile_example")
    profile_path = tmp_path / "el_profile.yaml"
    profile_path.write_text(
        "\n".join(
            [
                "connection:",
                "  transport: usb-scpi",
                "  serial_port: /dev/ttyUSB0",
                "protections:",
                "  ovp_v: 35.0",
                "  ocp_a: 20.0",
                "limits:",
                "  voltage_max_v: 35.0",
                "  current_max_a: 25.0",
                "  power_max_w: 800.0",
                "stages:",
                "  - name: warmup",
                "    mode: cc",
                "    setpoint: 5.0",
                "    duration_s: 10.0",
                "  - name: hold_voltage",
                "    mode: cv",
                "    setpoint: 28.0",
                "    duration_s: 5.0",
                "    cutoff_voltage_v: 26.4",
                "    cutoff_confirm_samples: 4",
                "  - name: recovery",
                "    mode: off",
                "    duration_s: 2.0",
            ]
        ),
        encoding="utf-8",
    )

    profile = example_module.load_profile(profile_path)

    assert profile.connection.transport == "usb-scpi"
    assert profile.connection.serial_port == "/dev/ttyUSB0"
    assert profile.run.log_directory == Path("logging")
    assert profile.protections.ovp_v == pytest.approx(35.0)
    assert profile.protections.ocp_a == pytest.approx(20.0)
    assert profile.limits.current_max_a == pytest.approx(25.0)
    assert [stage.mode for stage in profile.stages] == ["cc", "cv", "off"]
    assert profile.stages[0].cutoff_voltage_v is None
    assert profile.stages[1].cutoff_voltage_v == pytest.approx(26.4)
    assert profile.stages[1].cutoff_confirm_samples == 4


def test_el_profile_builds_csv_metadata_lines():
    example_module = load_example_module("el_profile.py", "el_profile_metadata")

    config = example_module.ProfileConfig(
        connection=example_module.ConnectionSettings(
            transport="usb-modbus",
            serial_port="/dev/ttyACM0",
            serial_glob=None,
            host=None,
            lan_scpi_port=5025,
            baudrate=115200,
            unit_id=0,
            timeout_s=1.0,
        ),
        run=example_module.RunSettings(log_directory=Path("logging")),
        protections=example_module.ProtectionSettings(ovp_v=25.2, ocp_a=65.0, opp_w=1300.0),
        limits=example_module.AdjustmentLimits(voltage_min_v=15.0, voltage_max_v=25.2, power_max_w=1200.0),
        stages=[
            example_module.ProfileStage(
                name="takeoff",
                mode="cc",
                setpoint=60.0,
                duration_s=30.0,
                cutoff_voltage_v=None,
                cutoff_confirm_samples=3,
            )
        ],
    )

    lines = example_module.build_csv_metadata_lines(
        config=config,
        battery_serial="Pack 01",
        profile_path=Path("/tmp/el_profile.yaml"),
        started_at=datetime(2026, 4, 22, 10, 30, 0, tzinfo=timezone.utc),
        hostname="test-host",
    )

    assert lines[:5] == [
        "# host: test-host",
        "# started_at: 2026-04-22T10:30:00+00:00",
        "# profile_path: /tmp/el_profile.yaml",
        "# battery_serial: Pack 01",
        "# timestamp_column: seconds since run start (time.monotonic)",
    ]
    assert "# profile:" in lines
    assert "#   connection:" in lines
    assert "#     serial_port: /dev/ttyACM0" in lines
    assert "#   stages:" in lines
    assert "#   - name: takeoff" in lines


def test_el_profile_rejects_unknown_stage_mode(tmp_path):
    example_module = load_example_module("el_profile.py", "el_profile_invalid_stage")
    profile_path = tmp_path / "el_profile.yaml"
    profile_path.write_text(
        "\n".join(
            [
                "stages:",
                "  - name: invalid",
                "    mode: xyz",
                "    setpoint: 5.0",
                "    duration_s: 5.0",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit, match="unsupported mode"):
        example_module.load_profile(profile_path)


def test_el_profile_rejects_stage_outside_configured_limits(tmp_path):
    example_module = load_example_module("el_profile.py", "el_profile_limit_violation")
    profile_path = tmp_path / "el_profile.yaml"
    profile_path.write_text(
        "\n".join(
            [
                "limits:",
                "  current_max_a: 10.0",
                "stages:",
                "  - name: too_high",
                "    mode: cc",
                "    setpoint: 12.0",
                "    duration_s: 5.0",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit, match="allowed maximum"):
        example_module.load_profile(profile_path)


def test_el_profile_rejects_cutoff_above_voltage_limit_or_ovp(tmp_path):
    example_module = load_example_module("el_profile.py", "el_profile_voltage_conflict")
    profile_path = tmp_path / "el_profile.yaml"
    profile_path.write_text(
        "\n".join(
            [
                "protections:",
                "  ovp_v: 25.5",
                "limits:",
                "  voltage_max_v: 25.2",
                "stages:",
                "  - name: takeoff_peak",
                "    mode: cc",
                "    setpoint: 50.0",
                "    duration_s: 60.0",
                "    cutoff_voltage_v: 26.4",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit, match="Stage 'takeoff_peak' cutoff_voltage_v=26.4 exceeds the allowed maximum of 25.2"):
        example_module.load_profile(profile_path)


def test_el_profile_resolves_effective_limits_from_device_values():
    example_module = load_example_module("el_profile.py", "el_profile_effective_limits")

    configured = example_module.AdjustmentLimits(
        voltage_max_v=25.2,
        power_max_w=1200.0,
    )
    observed = {
        "voltage_min_v": 2.000441311217907,
        "voltage_max_v": 25.200067318660363,
        "current_max_a": 59.99950632982404,
        "power_max_w": 1199.9901265964806,
    }

    effective = example_module.resolve_effective_limits(configured, observed)

    assert effective.voltage_min_v == pytest.approx(2.000441311217907)
    assert effective.voltage_max_v == pytest.approx(25.2)
    assert effective.current_max_a == pytest.approx(59.99950632982404)
    assert effective.power_max_w == pytest.approx(1200.0)


def test_el_profile_allows_stage_at_quantized_active_current_limit():
    example_module = load_example_module("el_profile.py", "el_profile_quantized_limit")

    stage = example_module.ProfileStage(
        name="takeoff_peak",
        mode="cc",
        setpoint=60.0,
        duration_s=30.0,
        cutoff_voltage_v=None,
        cutoff_confirm_samples=3,
    )
    protections = example_module.ProtectionSettings(
        ovp_v=25.2,
        ocp_a=65.0,
        opp_w=1300.0,
    )
    limits = example_module.AdjustmentLimits(
        voltage_min_v=15.0,
        voltage_max_v=25.2,
        current_min_a=0.0,
        current_max_a=59.99950632982404,
        power_max_w=1200.0,
    )
    measurement = type("Measurement", (), {"voltage_v": 24.0})()

    example_module.validate_profile_against_active_device(
        measurement=measurement,
        protections=protections,
        limits=limits,
        stages=[stage],
    )


def test_el_profile_rejects_stage_when_active_ocp_matches_programmed_current():
    example_module = load_example_module("el_profile.py", "el_profile_active_ocp_conflict")

    stage = example_module.ProfileStage(
        name="takeoff_peak",
        mode="cc",
        setpoint=60.0,
        duration_s=30.0,
        cutoff_voltage_v=None,
        cutoff_confirm_samples=3,
    )
    protections = example_module.ProtectionSettings(
        ovp_v=30.0,
        ocp_a=60.0,
        opp_w=1300.0,
    )
    limits = example_module.AdjustmentLimits(
        voltage_min_v=2.0,
        voltage_max_v=25.2,
        current_max_a=60.0,
        power_max_w=1200.0,
    )
    measurement = type("Measurement", (), {"voltage_v": 24.0})()

    with pytest.raises(RuntimeError, match="active OCP threshold"):
        example_module.validate_profile_against_active_device(
            measurement=measurement,
            protections=protections,
            limits=limits,
            stages=[stage],
        )


def test_el_profile_cp_stage_uses_cutoff_voltage_and_relaxes_other_inputs():
    example_module = load_example_module("el_profile.py", "el_profile_apply_stage")

    class FakeDevice:
        def __init__(self):
            self.calls = []

        def set_input_enabled(self, enabled):
            self.calls.append(("set_input_enabled", enabled))

        def set_voltage(self, value):
            self.calls.append(("set_voltage", value))

        def set_current(self, value):
            self.calls.append(("set_current", value))

        def set_power(self, value):
            self.calls.append(("set_power", value))

        def set_resistance(self, value):
            self.calls.append(("set_resistance", value))

    device = FakeDevice()
    stage = example_module.ProfileStage(
        name="hold_power",
        mode="cp",
        setpoint=300.0,
        duration_s=5.0,
        cutoff_voltage_v=26.4,
        cutoff_confirm_samples=3,
    )
    run_settings = example_module.RunSettings(enable_settle_s=0.0, stage_settle_s=0.0)
    limits = example_module.AdjustmentLimits(voltage_max_v=35.2, current_max_a=30.0, power_max_w=1000.0)

    example_module.apply_stage(device, stage, run_settings, limits)

    assert device.calls == [
        ("set_input_enabled", False),
        ("set_voltage", 26.4),
        ("set_current", 30.0),
        ("set_power", 300.0),
        ("set_input_enabled", True),
    ]
