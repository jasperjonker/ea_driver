import pytest

from ea_driver.ea import (
    EA_MEASURE_FULL_SCALE,
    EA_PROTECTION_FULL_SCALE,
    EA_SET_VALUE_FULL_SCALE,
    EABatteryTestResult,
    EAEL9080_60DT,
    EAELSCPIBase,
    EAModbusBase,
    EAPSB10060_60,
    EAPSBModbusBase,
    EAPSBSCPIBase,
    EASCPIBase,
    _raw_to_value,
    _value_to_raw,
    decode_ea_device_state,
)
from ea_driver.modbus import pack_float_be


def test_set_value_scaling_round_trip():
    raw = _value_to_raw(51.0, nominal=60.0, percent_limit=1.02, full_scale=EA_SET_VALUE_FULL_SCALE)
    value = _raw_to_value(raw, nominal=60.0, percent_limit=1.02, full_scale=EA_SET_VALUE_FULL_SCALE)
    assert abs(value - 51.0) < 0.02


def test_measurement_scaling_round_trip():
    raw = _value_to_raw(750.0, nominal=1000.0, percent_limit=1.25, full_scale=EA_MEASURE_FULL_SCALE)
    value = _raw_to_value(raw, nominal=1000.0, percent_limit=1.25, full_scale=EA_MEASURE_FULL_SCALE)
    assert abs(value - 750.0) < 0.05


def test_protection_scaling_round_trip():
    raw = _value_to_raw(63.0, nominal=60.0, percent_limit=1.10, full_scale=EA_PROTECTION_FULL_SCALE)
    value = _raw_to_value(raw, nominal=60.0, percent_limit=1.10, full_scale=EA_PROTECTION_FULL_SCALE)
    assert abs(value - 63.0) < 0.02


def test_device_state_decode():
    value = 0
    value |= 0x06
    value |= 1 << 7
    value |= 0b10 << 9
    value |= 1 << 11
    value |= 1 << 12
    value |= 1 << 15
    value |= 1 << 16
    value |= 1 << 30

    status = decode_ea_device_state(value)
    assert status.control_location == 0x06
    assert status.dc_on is True
    assert status.regulation_mode == "CC"
    assert status.remote is True
    assert status.sink_mode is True
    assert status.operation_mode == "SINK"
    assert status.alarms_active is True
    assert status.ovp_active is True
    assert status.rem_sb_inhibiting is True


class FakeSCPITransport:
    def __init__(self, responses: dict[str, str] | None = None) -> None:
        self.responses = responses or {}
        self.commands: list[tuple[str, str]] = []
        self.open_calls = 0
        self.close_calls = 0

    def open(self) -> None:
        self.open_calls += 1

    def close(self) -> None:
        self.close_calls += 1

    def write(self, command: str) -> None:
        self.commands.append(("write", command))

    def query(self, command: str) -> str:
        self.commands.append(("query", command))
        return self.responses[command]


def test_psb_scpi_uses_outp_commands():
    transport = FakeSCPITransport({"OUTP?": "ON"})
    device = EASCPIBase(transport, EAPSB10060_60.RATINGS)

    device.set_output_enabled(True)
    assert device.is_output_enabled() is True
    assert transport.commands == [("write", "OUTP ON"), ("query", "OUTP?")]


def test_scpi_device_supports_context_manager():
    transport = FakeSCPITransport()
    device = EASCPIBase(transport, EAPSB10060_60.RATINGS)

    with device as opened:
        assert opened is device

    assert transport.open_calls == 1
    assert transport.close_calls == 1


def test_el_scpi_uses_inp_commands():
    transport = FakeSCPITransport({"INP?": "ON"})
    device = EAELSCPIBase(transport, EAEL9080_60DT.RATINGS)

    device.set_output_enabled(True)
    assert device.is_output_enabled() is True
    assert transport.commands == [("write", "INP ON"), ("query", "INP?")]


class FakeModbusClient:
    def __init__(self, responses: dict[tuple[int, int], list[int]]) -> None:
        self.responses = responses
        self.read_calls: list[tuple[int, int]] = []
        self.coil_writes: list[tuple[int, bool]] = []
        self.register_writes: list[tuple[int, int]] = []
        self.open_calls = 0
        self.close_calls = 0

    def open(self) -> None:
        self.open_calls += 1

    def close(self) -> None:
        self.close_calls += 1

    def read_holding_registers(self, address: int, count: int) -> list[int]:
        self.read_calls.append((address, count))
        return self.responses[(address, count)]

    def write_single_register(self, address: int, value: int) -> None:
        self.register_writes.append((address, value))

    def write_single_coil(self, address: int, enabled: bool) -> None:
        self.coil_writes.append((address, enabled))


def test_read_protection_thresholds_uses_sparse_registers():
    client = FakeModbusClient(
        {
            (550, 1): [1000],
            (553, 1): [2000],
            (556, 1): [3000],
        }
    )
    device = EAModbusBase(client, EAEL9080_60DT.RATINGS)

    thresholds = device.read_protection_thresholds()

    assert thresholds.voltage_v == _raw_to_value(1000, 80.0, 1.10, EA_PROTECTION_FULL_SCALE)
    assert thresholds.current_a == _raw_to_value(2000, 60.0, 1.10, EA_PROTECTION_FULL_SCALE)
    assert thresholds.power_w == _raw_to_value(3000, 1200.0, 1.10, EA_PROTECTION_FULL_SCALE)
    assert client.read_calls == [(550, 1), (553, 1), (556, 1)]


def test_modbus_boolean_controls_use_coil_writes():
    client = FakeModbusClient({})
    device = EAModbusBase(client, EAEL9080_60DT.RATINGS)

    device.set_remote(True)
    device.set_input_enabled(False)

    assert client.coil_writes == [(402, True), (405, False)]


def test_modbus_device_supports_context_manager():
    client = FakeModbusClient({})
    device = EAModbusBase(client, EAEL9080_60DT.RATINGS)

    with device as opened:
        assert opened is device

    assert client.open_calls == 1
    assert client.close_calls == 1


def test_el_modbus_rtu_defaults_to_unit_id_zero():
    device = EAEL9080_60DT.modbus_rtu("/dev/ttyACM0")
    assert device.client.unit_id == 0


def test_psb_modbus_tcp_defaults_to_unit_id_zero():
    device = EAPSB10060_60.modbus_tcp("192.168.0.42")
    assert isinstance(device, EAPSBModbusBase)
    assert device.client.unit_id == 0


def test_psb_ratings_match_psb_10060_60():
    assert EAPSB10060_60.RATINGS.voltage_v == 60.0
    assert EAPSB10060_60.RATINGS.current_a == 60.0
    assert EAPSB10060_60.RATINGS.power_w == 1500.0
    assert EAPSB10060_60.RATINGS.resistance_ohm_min == 0.04
    assert EAPSB10060_60.RATINGS.resistance_ohm_max == 48.0


def test_psb_scpi_factory_returns_psb_specific_class():
    device = EAPSB10060_60.scpi_tcp("192.168.0.42")
    assert isinstance(device, EAPSBSCPIBase)


def test_psb_scpi_supports_sink_and_mode_helpers():
    transport = FakeSCPITransport(
        {
            "SYST:CONF:MODE?": "UIR",
            "BATT:TEST?": "1.25Ah, 33.5Wh, 00:01:02",
            "SINK:CURR?": "5.00 A",
        }
    )
    device = EAPSBSCPIBase(transport, EAPSB10060_60.RATINGS)

    device.set_sink_current(5.0)
    device.set_sink_power(120.0)
    device.set_sink_resistance(8.0)
    device.set_source_only_mode()
    device.set_sink_only_mode()
    device.set_resistance_mode_enabled(True)
    sink_current = device.get_sink_current_setpoint()
    result = device.read_battery_test()

    assert transport.commands == [
        ("write", "SINK:CURR 5.0"),
        ("write", "SINK:POW 120.0"),
        ("write", "SINK:RES 8.0"),
        ("write", "SINK:CURR 0.0"),
        ("write", "VOLT 0.0"),
        ("write", "SYST:CONF:MODE UIR"),
        ("query", "SINK:CURR?"),
        ("query", "BATT:TEST?"),
    ]
    assert sink_current == 5.0
    assert result == EABatteryTestResult(capacity_ah=1.25, energy_wh=33.5, elapsed="00:01:02")
    assert device.power_stage_mode() == "UIR"


def test_psb_scpi_clamps_sink_resistance_to_valid_range():
    transport = FakeSCPITransport({})
    device = EAPSBSCPIBase(transport, EAPSB10060_60.RATINGS)

    device.set_sink_resistance(0.01)

    assert transport.commands == [("write", "SINK:RES 0.04")]


def test_psb_scpi_function_generator_helpers_emit_documented_commands():
    transport = FakeSCPITransport({})
    device = EAPSBSCPIBase(transport, EAPSB10060_60.RATINGS)

    device.select_function_generator_mode("CURRENT")
    device.configure_arbitrary_sequence_point(
        12,
        start_amplitude=30.0,
        end_amplitude=30.0,
        start_frequency_hz=10.0,
        end_frequency_hz=10.0,
        start_offset=50.0,
        end_offset=50.0,
        duration_s=60.0,
    )
    device.set_arbitrary_start(12)
    device.set_arbitrary_end(12)
    device.set_arbitrary_cycles(1)
    device.submit_arbitrary()
    device.set_arbitrary_state(True)
    device.load_xy_table([0.0, 5.0], table=2, start_index=7)

    assert transport.commands == [
        ("write", "FUNC:GEN:SEL CURRENT"),
        ("write", "FUNC:GEN:WAVE:LEV 12"),
        ("write", "FUNC:GEN:WAVE:IND 0"),
        ("write", "FUNC:GEN:WAVE:DAT 30.0"),
        ("write", "FUNC:GEN:WAVE:IND 1"),
        ("write", "FUNC:GEN:WAVE:DAT 30.0"),
        ("write", "FUNC:GEN:WAVE:IND 2"),
        ("write", "FUNC:GEN:WAVE:DAT 10.0"),
        ("write", "FUNC:GEN:WAVE:IND 3"),
        ("write", "FUNC:GEN:WAVE:DAT 10.0"),
        ("write", "FUNC:GEN:WAVE:IND 5"),
        ("write", "FUNC:GEN:WAVE:DAT 50.0"),
        ("write", "FUNC:GEN:WAVE:IND 6"),
        ("write", "FUNC:GEN:WAVE:DAT 50.0"),
        ("write", "FUNC:GEN:WAVE:IND 7"),
        ("write", "FUNC:GEN:WAVE:DAT 60.0"),
        ("write", "FUNC:GEN:WAVE:STAR 12"),
        ("write", "FUNC:GEN:WAVE:END 12"),
        ("write", "FUNC:GEN:WAVE:NUM 1"),
        ("write", "FUNC:GEN:WAVE:SUBM"),
        ("write", "FUNC:GEN:WAVE:STAT RUN"),
        ("write", "FUNC:GEN:XY:SEC:LEV 7"),
        ("write", "FUNC:GEN:XY:SEC:DAT 0.0"),
        ("write", "FUNC:GEN:XY:SEC:LEV 8"),
        ("write", "FUNC:GEN:XY:SEC:DAT 5.0"),
        ("write", "FUNC:GEN:XY:SUBM SECOND"),
    ]


def test_psb_modbus_supports_sink_registers_signed_measurements_and_r_mode():
    status_value = 0
    status_value |= 1 << 7
    status_value |= 0b10 << 9
    status_value |= 1 << 11
    status_value |= 1 << 12
    client = FakeModbusClient(
        {
            (121, 2): list(pack_float_be(60.0)),
            (123, 2): list(pack_float_be(60.0)),
            (125, 2): list(pack_float_be(1500.0)),
            (127, 2): list(pack_float_be(48.0)),
            (129, 2): list(pack_float_be(0.04)),
            (507, 3): [
                _value_to_raw(24.0, 60.0, 1.25, EA_MEASURE_FULL_SCALE),
                _value_to_raw(5.0, 60.0, 1.25, EA_MEASURE_FULL_SCALE),
                _value_to_raw(120.0, 1500.0, 1.25, EA_MEASURE_FULL_SCALE),
            ],
            (505, 2): [(status_value >> 16) & 0xFFFF, status_value & 0xFFFF],
        }
    )
    device = EAPSBModbusBase(client, EAPSB10060_60.RATINGS)

    device.set_sink_current(4.0)
    device.set_sink_power(100.0)
    device.set_sink_resistance(10.0)
    device.set_resistance_mode_enabled(True)
    measurement = device.read_measurements()
    nominals = device.read_nominals()

    assert client.register_writes == [
        (499, _value_to_raw(4.0, 60.0, 1.02, EA_SET_VALUE_FULL_SCALE)),
        (498, _value_to_raw(100.0, 1500.0, 1.02, EA_SET_VALUE_FULL_SCALE)),
        (504, _value_to_raw(10.0, 48.0, 1.02, EA_SET_VALUE_FULL_SCALE)),
    ]
    assert client.coil_writes == [(409, True)]
    assert measurement.voltage_v == pytest.approx(24.0, abs=0.05)
    assert measurement.current_a == pytest.approx(-5.0, abs=0.05)
    assert measurement.power_w == pytest.approx(-120.0, abs=0.1)
    assert nominals.resistance_ohm_min == pytest.approx(0.04)
    assert nominals.resistance_ohm_max == pytest.approx(48.0)


def test_psb_modbus_clamps_sink_resistance_to_valid_range():
    client = FakeModbusClient({})
    device = EAPSBModbusBase(client, EAPSB10060_60.RATINGS)

    device.set_sink_resistance(0.01)

    assert client.register_writes == [
        (504, _value_to_raw(0.04, 48.0, 1.02, EA_SET_VALUE_FULL_SCALE)),
    ]


def test_el_modbus_tcp_is_not_supported():
    with pytest.raises(NotImplementedError):
        EAEL9080_60DT.modbus_tcp("192.168.0.42")
