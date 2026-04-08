from ea_driver.ea import (
    EA_MEASURE_FULL_SCALE,
    EA_PROTECTION_FULL_SCALE,
    EA_SET_VALUE_FULL_SCALE,
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
