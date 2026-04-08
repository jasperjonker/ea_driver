import pytest

from ea_driver.ea import (
    EA_MEASURE_FULL_SCALE,
    EA_PROTECTION_FULL_SCALE,
    EA_SET_VALUE_FULL_SCALE,
    EAEL9080_60DT,
    EAELSCPIBase,
    EAModbusBase,
    EAPSB10060_60,
    EASCPIBase,
    _raw_to_value,
    _value_to_raw,
    decode_ea_device_state,
)


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
    value |= 1 << 15
    value |= 1 << 16
    value |= 1 << 30

    status = decode_ea_device_state(value)
    assert status.control_location == 0x06
    assert status.dc_on is True
    assert status.regulation_mode == "CC"
    assert status.remote is True
    assert status.alarms_active is True
    assert status.ovp_active is True
    assert status.rem_sb_inhibiting is True


class FakeSCPITransport:
    def __init__(self, responses: dict[str, str] | None = None) -> None:
        self.responses = responses or {}
        self.commands: list[tuple[str, str]] = []

    def open(self) -> None:
        pass

    def close(self) -> None:
        pass

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

    def open(self) -> None:
        pass

    def close(self) -> None:
        pass

    def read_holding_registers(self, address: int, count: int) -> list[int]:
        self.read_calls.append((address, count))
        return self.responses[(address, count)]

    def write_single_register(self, address: int, value: int) -> None:
        raise NotImplementedError

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


def test_el_modbus_rtu_defaults_to_unit_id_zero():
    device = EAEL9080_60DT.modbus_rtu("/dev/ttyACM0")
    assert device.client.unit_id == 0


def test_psb_modbus_tcp_defaults_to_unit_id_zero():
    device = EAPSB10060_60.modbus_tcp("192.168.0.42")
    assert device.client.unit_id == 0


def test_el_modbus_tcp_is_not_supported():
    with pytest.raises(NotImplementedError):
        EAEL9080_60DT.modbus_tcp("192.168.0.42")
