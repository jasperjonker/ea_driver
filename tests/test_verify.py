import pytest

from ea_driver.verify import (
    DEVICE_BY_KEY,
    annotate_modbus_remote_error,
    ensure_output_enabled,
    parse_unit_id_selection,
    resolve_device_spec,
)


def test_parse_unit_id_selection_auto():
    assert parse_unit_id_selection("auto") == [0, 1]


def test_parse_unit_id_selection_explicit():
    assert parse_unit_id_selection("0") == [0]
    assert parse_unit_id_selection("1") == [1]


def test_annotate_modbus_remote_error_for_exception_7():
    error = annotate_modbus_remote_error(RuntimeError("Modbus exception 7 for function 0x5"))
    assert "rejected by the device" in str(error)
    assert "Local mode" in str(error)


def test_ensure_output_enabled_raises_with_scpi_errors():
    class FakeDevice:
        def is_output_enabled(self) -> bool:
            return False

        def read_errors(self, *, max_errors: int = 5) -> list[str]:
            return ['-221,"Settings conflict"', '0,"No error"']

    with pytest.raises(RuntimeError, match="Settings conflict"):
        ensure_output_enabled(FakeDevice())


def test_resolve_device_spec_auto_detects_psb(monkeypatch):
    monkeypatch.setattr(
        "ea_driver.verify.identify_device",
        lambda port, baudrate, timeout: "EA Elektro-Automatik GmbH & Co. KG, PSB 10060-60, 2538170001, FW",
    )
    spec, idn = resolve_device_spec("/dev/ttyACM0", 115200, 1.0, "auto")
    assert spec is DEVICE_BY_KEY["psb10060-60"]
    assert "PSB 10060-60" in idn


def test_resolve_device_spec_auto_detects_el(monkeypatch):
    monkeypatch.setattr(
        "ea_driver.verify.identify_device",
        lambda port, baudrate, timeout: "EA Elektro-Automatik GmbH & Co. KG, EL 9080-60 DT, 2228100002, FW",
    )
    spec, idn = resolve_device_spec("/dev/ttyACM0", 115200, 1.0, "auto")
    assert spec is DEVICE_BY_KEY["el9080-60-dt"]
    assert "EL 9080-60 DT" in idn
