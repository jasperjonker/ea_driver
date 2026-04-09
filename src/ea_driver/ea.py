from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Sequence

from .core import DeviceRatings, Measurement
from .modbus import ModbusRTUClient, ModbusTCPClient, unpack_float_be
from .scpi import SCPIDevice, SCPITransport, SerialSCPITransport, SocketSCPITransport

EA_SET_VALUE_FULL_SCALE = 0xD0E5
EA_MEASURE_FULL_SCALE = 0xFFFF
EA_PROTECTION_FULL_SCALE = 0xE147

_SCPI_NUMERIC_RE = re.compile(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?")


def _percent_to_raw(percent: float, full_scale: int) -> int:
    return max(0, min(full_scale, round(percent * full_scale)))


def _raw_to_percent(raw: int, full_scale: int) -> float:
    return raw / full_scale


def _value_to_raw(value: float, nominal: float, percent_limit: float, full_scale: int) -> int:
    percent = 0.0 if nominal == 0 else min(max(value / nominal, 0.0), percent_limit)
    return _percent_to_raw(percent / percent_limit, full_scale)


def _raw_to_value(raw: int, nominal: float, percent_limit: float, full_scale: int) -> float:
    return nominal * (_raw_to_percent(raw, full_scale) * percent_limit)


def _normalize_resistance_value(value: float, ratings: DeviceRatings) -> float:
    if ratings.resistance_ohm_min is not None:
        value = max(value, ratings.resistance_ohm_min)
    if ratings.resistance_ohm_max is not None:
        value = min(value, ratings.resistance_ohm_max)
    return value


def _parse_scpi_numeric(response: str) -> float:
    match = _SCPI_NUMERIC_RE.search(response.strip())
    if match is None:
        raise ValueError(f"Could not parse numeric value from SCPI response: {response!r}")
    return float(match.group(0))


def _parse_scpi_csv(response: str) -> list[str]:
    return [item.strip() for item in response.split(",")]


@dataclass(frozen=True, slots=True)
class EAStatus:
    control_location: int
    dc_on: bool
    regulation_mode: str
    remote: bool
    sink_mode: bool
    function_generator_running: bool
    remote_sensing: bool
    alarms_active: bool
    ovp_active: bool
    ocp_active: bool
    opp_active: bool
    over_temperature: bool
    power_fail: bool
    rem_sb_inhibiting: bool

    @property
    def operation_mode(self) -> str:
        return "SINK" if self.sink_mode else "SOURCE"


@dataclass(frozen=True, slots=True)
class EABatteryTestResult:
    capacity_ah: float
    energy_wh: float
    elapsed: str


def decode_ea_device_state(value: int) -> EAStatus:
    control_location = value & 0x1F
    mode_bits = (value >> 9) & 0x03
    regulation_mode = {0b00: "CV", 0b01: "CR", 0b10: "CC", 0b11: "CP"}[mode_bits]
    return EAStatus(
        control_location=control_location,
        dc_on=bool((value >> 7) & 0x1),
        regulation_mode=regulation_mode,
        remote=bool((value >> 11) & 0x1),
        sink_mode=bool((value >> 12) & 0x1),
        function_generator_running=bool((value >> 13) & 0x1),
        remote_sensing=bool((value >> 14) & 0x1),
        alarms_active=bool((value >> 15) & 0x1),
        ovp_active=bool((value >> 16) & 0x1),
        ocp_active=bool((value >> 17) & 0x1),
        opp_active=bool((value >> 18) & 0x1),
        over_temperature=bool((value >> 19) & 0x1),
        power_fail=bool((value >> 21) & 0x1 or (value >> 22) & 0x1 or (value >> 23) & 0x1),
        rem_sb_inhibiting=bool((value >> 30) & 0x1),
    )


class EASCPIBase(SCPIDevice):
    def __init__(self, transport: SCPITransport, ratings: DeviceRatings) -> None:
        super().__init__(transport)
        self.ratings = ratings

    def read_nominals(self) -> DeviceRatings:
        resistance_min = self.ratings.resistance_ohm_min
        resistance_max = self.ratings.resistance_ohm_max
        try:
            resistance_min = _parse_scpi_numeric(self.query("SYST:NOM:RES:MIN?"))
            resistance_max = _parse_scpi_numeric(self.query("SYST:NOM:RES:MAX?"))
        except Exception:
            pass
        return DeviceRatings(
            voltage_v=_parse_scpi_numeric(self.query("SYST:NOM:VOLT?")),
            current_a=_parse_scpi_numeric(self.query("SYST:NOM:CURR?")),
            power_w=_parse_scpi_numeric(self.query("SYST:NOM:POW?")),
            resistance_ohm_min=resistance_min,
            resistance_ohm_max=resistance_max,
        )

    def set_remote(self, enabled: bool) -> None:
        self.write(f"SYST:LOCK {'ON' if enabled else 'OFF'}")

    def remote_owner(self) -> str:
        return self.query("SYST:LOCK:OWNER?")

    def set_output_enabled(self, enabled: bool) -> None:
        self.write(f"OUTP {'ON' if enabled else 'OFF'}")

    def is_output_enabled(self) -> bool:
        return self.query("OUTP?").strip().upper() == "ON"

    def set_voltage(self, volts: float) -> None:
        self.write(f"VOLT {volts}")

    def get_voltage_setpoint(self) -> float:
        return _parse_scpi_numeric(self.query("VOLT?"))

    def set_source_current(self, amps: float) -> None:
        self.write(f"CURR {amps}")

    def get_source_current_setpoint(self) -> float:
        return _parse_scpi_numeric(self.query("CURR?"))

    def set_current(self, amps: float) -> None:
        self.set_source_current(amps)

    def set_source_power(self, watts: float) -> None:
        self.write(f"POW {watts}")

    def get_source_power_setpoint(self) -> float:
        return _parse_scpi_numeric(self.query("POW?"))

    def set_power(self, watts: float) -> None:
        self.set_source_power(watts)

    def set_source_resistance(self, ohms: float) -> None:
        self.write(f"RES {_normalize_resistance_value(ohms, self.ratings)}")

    def get_source_resistance_setpoint(self) -> float:
        return _parse_scpi_numeric(self.query("RES?"))

    def set_resistance(self, ohms: float) -> None:
        self.set_source_resistance(ohms)

    def set_sink_current(self, amps: float) -> None:
        self.write(f"SINK:CURR {amps}")

    def measure_voltage(self) -> float:
        return _parse_scpi_numeric(self.query("MEAS:VOLT?"))

    def measure_current(self) -> float:
        return _parse_scpi_numeric(self.query("MEAS:CURR?"))

    def measure_power(self) -> float:
        return _parse_scpi_numeric(self.query("MEAS:POW?"))

    def measure_all(self) -> Measurement:
        parts = _parse_scpi_csv(self.query("MEAS:ARR?"))
        return Measurement(
            voltage_v=_parse_scpi_numeric(parts[0]),
            current_a=_parse_scpi_numeric(parts[1]),
            power_w=_parse_scpi_numeric(parts[2]),
        )


class EAELSCPIBase(EASCPIBase):
    def set_input_enabled(self, enabled: bool) -> None:
        self.write(f"INP {'ON' if enabled else 'OFF'}")

    def set_output_enabled(self, enabled: bool) -> None:
        self.set_input_enabled(enabled)

    def is_input_enabled(self) -> bool:
        return self.query("INP?").strip().upper() == "ON"

    def is_output_enabled(self) -> bool:
        return self.is_input_enabled()


class EAPSBSCPIBase(EASCPIBase):
    def get_sink_current_setpoint(self) -> float:
        return _parse_scpi_numeric(self.query("SINK:CURR?"))

    _FG_MODES = {"FC", "IUPS", "IUEL", "UI", "PV", "PVA", "PVB", "VOLTAGE", "CURRENT", "NONE"}
    _POWER_STAGE_MODES = {"UIP", "UIR"}

    def set_sink_power(self, watts: float) -> None:
        self.write(f"SINK:POW {watts}")

    def get_sink_power_setpoint(self) -> float:
        return _parse_scpi_numeric(self.query("SINK:POW?"))

    def set_sink_resistance(self, ohms: float) -> None:
        self.write(f"SINK:RES {_normalize_resistance_value(ohms, self.ratings)}")

    def get_sink_resistance_setpoint(self) -> float:
        return _parse_scpi_numeric(self.query("SINK:RES?"))

    def set_source_only_mode(self) -> None:
        self.set_sink_current(0.0)

    def set_sink_only_mode(self) -> None:
        self.set_voltage(0.0)

    def set_power_stage_mode(self, mode: str) -> None:
        normalized = mode.strip().upper()
        if normalized not in self._POWER_STAGE_MODES:
            raise ValueError(f"Unsupported power stage mode: {mode}")
        self.write(f"SYST:CONF:MODE {normalized}")

    def power_stage_mode(self) -> str:
        return self.query("SYST:CONF:MODE?").strip().upper()

    def set_resistance_mode_enabled(self, enabled: bool) -> None:
        self.set_power_stage_mode("UIR" if enabled else "UIP")

    def questionable_status(self) -> int:
        return int(self.query("STAT:QUES:COND?").strip())

    def read_alarm_counts(self) -> dict[str, int]:
        return {
            "ovp": int(self.query("SYST:ALARM:COUNT:OVOLTAGE?").strip()),
            "ocp": int(self.query("SYST:ALARM:COUNT:OCURRENT?").strip()),
            "opp": int(self.query("SYST:ALARM:COUNT:OPOWER?").strip()),
            "over_temperature": int(self.query("SYST:ALARM:COUNT:OTEMPERATURE?").strip()),
            "power_fail": int(self.query("SYST:ALARM:COUNT:PFAIL?").strip()),
            "share_bus_fail": int(self.query("SYST:ALARM:COUNT:SHAREBUSFAIL?").strip()),
        }

    def read_sink_alarm_counts(self) -> dict[str, int]:
        return {
            "ocp": int(self.query("SYST:SINK:ALARM:COUNT:OCURRENT?").strip()),
            "opp": int(self.query("SYST:SINK:ALARM:COUNT:OPOWER?").strip()),
        }

    def select_function_generator_mode(self, mode: str) -> None:
        normalized = mode.strip().upper()
        if normalized not in self._FG_MODES:
            raise ValueError(f"Unsupported function generator mode: {mode}")
        self.write(f"FUNC:GEN:SEL {normalized}")

    def function_generator_mode(self) -> str:
        return self.query("FUNC:GEN:SEL?").strip().upper()

    def select_arbitrary_sequence(self, sequence: int) -> None:
        self.write(f"FUNC:GEN:WAVE:LEV {sequence}")

    def selected_arbitrary_sequence(self) -> int:
        return int(self.query("FUNC:GEN:WAVE:LEV?").strip())

    def select_arbitrary_index(self, index: int) -> None:
        self.write(f"FUNC:GEN:WAVE:IND {index}")

    def selected_arbitrary_index(self) -> int:
        return int(self.query("FUNC:GEN:WAVE:IND?").strip())

    def set_arbitrary_data(self, value: float) -> None:
        self.write(f"FUNC:GEN:WAVE:DAT {value}")

    def get_arbitrary_data(self) -> float:
        return _parse_scpi_numeric(self.query("FUNC:GEN:WAVE:DAT?"))

    def configure_arbitrary_sequence_point(
        self,
        sequence: int,
        *,
        start_amplitude: float | None = None,
        end_amplitude: float | None = None,
        start_frequency_hz: float | None = None,
        end_frequency_hz: float | None = None,
        start_angle_deg: float | None = None,
        start_offset: float | None = None,
        end_offset: float | None = None,
        duration_s: float | None = None,
    ) -> None:
        parameter_values = (
            (0, start_amplitude),
            (1, end_amplitude),
            (2, start_frequency_hz),
            (3, end_frequency_hz),
            (4, start_angle_deg),
            (5, start_offset),
            (6, end_offset),
            (7, duration_s),
        )
        self.select_arbitrary_sequence(sequence)
        for index, value in parameter_values:
            if value is None:
                continue
            self.select_arbitrary_index(index)
            self.set_arbitrary_data(value)

    def set_arbitrary_start(self, sequence: int) -> None:
        self.write(f"FUNC:GEN:WAVE:STAR {sequence}")

    def arbitrary_start(self) -> int:
        return int(self.query("FUNC:GEN:WAVE:STAR?").strip())

    def set_arbitrary_end(self, sequence: int) -> None:
        self.write(f"FUNC:GEN:WAVE:END {sequence}")

    def arbitrary_end(self) -> int:
        return int(self.query("FUNC:GEN:WAVE:END?").strip())

    def set_arbitrary_cycles(self, cycles: int) -> None:
        self.write(f"FUNC:GEN:WAVE:NUM {cycles}")

    def arbitrary_cycles(self) -> int:
        return int(self.query("FUNC:GEN:WAVE:NUM?").strip())

    def submit_arbitrary(self) -> None:
        self.write("FUNC:GEN:WAVE:SUBM")

    def set_arbitrary_state(self, running: bool) -> None:
        self.write(f"FUNC:GEN:WAVE:STAT {'RUN' if running else 'STOP'}")

    def arbitrary_state(self) -> str:
        return self.query("FUNC:GEN:WAVE:STAT?").strip().upper()

    def set_xy_table_entry(self, index: int, value: float, *, table: int = 1) -> None:
        if table == 1:
            self.write(f"FUNC:GEN:XY:LEV {index}")
            self.write(f"FUNC:GEN:XY:DAT {value}")
            return
        if table == 2:
            self.write(f"FUNC:GEN:XY:SEC:LEV {index}")
            self.write(f"FUNC:GEN:XY:SEC:DAT {value}")
            return
        raise ValueError(f"Unsupported XY table: {table}")

    def load_xy_table(self, values: Sequence[float], *, table: int = 1, start_index: int = 0) -> None:
        for offset, value in enumerate(values):
            self.set_xy_table_entry(start_index + offset, value, table=table)
        self.submit_xy_table(table=table)

    def submit_xy_table(self, *, table: int = 1) -> None:
        if table == 1:
            self.write("FUNC:GEN:XY:SUBM FIRST")
            return
        if table == 2:
            self.write("FUNC:GEN:XY:SUBM SECOND")
            return
        raise ValueError(f"Unsupported XY table: {table}")

    def set_irradiation(self, percent: float) -> None:
        self.write(f"IRR {percent}")

    def irradiation(self) -> float:
        return _parse_scpi_numeric(self.query("IRR?"))

    def set_photovoltaic_mode(self, mode: str) -> None:
        self.write(f"FUNC:PHOT:MODE {mode}")

    def photovoltaic_mode(self) -> str:
        return self.query("FUNC:PHOT:MODE?").strip().upper()

    def set_photovoltaic_input_mode(self, mode: str) -> None:
        self.write(f"FUNC:PHOT:IMOD {mode}")

    def photovoltaic_input_mode(self) -> str:
        return self.query("FUNC:PHOT:IMOD?").strip().upper()

    def set_photovoltaic_technology(self, technology: str) -> None:
        self.write(f"FUNC:PHOT:TECH {technology}")

    def photovoltaic_technology(self) -> str:
        return self.query("FUNC:PHOT:TECH?").strip().upper()

    def set_photovoltaic_state(self, running: bool) -> None:
        self.write(f"FUNC:PHOT:STAT {'RUN' if running else 'STOP'}")

    def photovoltaic_state(self) -> str:
        return self.query("FUNC:PHOT:STAT?").strip().upper()

    def set_mpp_index(self, index: int) -> None:
        self.write(f"FUNC:GEN:MPP:IND {index}")

    def set_mpp_level(self, level: int) -> None:
        self.write(f"FUNC:GEN:MPP:LEV {level}")

    def set_mpp_data(self, value: str | int | float) -> None:
        self.write(f"FUNC:GEN:MPP:DAT {value}")

    def get_mpp_data(self) -> str:
        return self.query("FUNC:GEN:MPP:DAT?")

    def set_mpp_state(self, running: bool) -> None:
        self.write(f"FUNC:GEN:MPP:STAT {'RUN' if running else 'STOP'}")

    def mpp_state(self) -> str:
        return self.query("FUNC:GEN:MPP:STAT?").strip().upper()

    def set_battery_mode(self, mode: str) -> None:
        self.write(f"BATT:MODE {mode}")

    def battery_mode(self) -> str:
        return self.query("BATT:MODE?").strip().upper()

    def set_battery_state(self, running: bool) -> None:
        self.write(f"BATT:STAT {'RUN' if running else 'STOP'}")

    def battery_state(self) -> str:
        return self.query("BATT:STAT?").strip().upper()

    def battery_condition(self) -> str:
        return self.query("BATT:COND?").strip().upper()

    def read_battery_test(self) -> EABatteryTestResult:
        parts = _parse_scpi_csv(self.query("BATT:TEST?"))
        if len(parts) != 3:
            raise ValueError(f"Unexpected battery test response: {parts!r}")
        return EABatteryTestResult(
            capacity_ah=_parse_scpi_numeric(parts[0]),
            energy_wh=_parse_scpi_numeric(parts[1]),
            elapsed=parts[2],
        )

    def reset_battery_test(self, mode: str = "RESET") -> None:
        self.write(f"BATT:TEST {mode}")


class EAModbusBase:
    REG_NOMINAL_VOLTAGE = 121
    REG_NOMINAL_CURRENT = 123
    REG_NOMINAL_POWER = 125
    REG_REMOTE = 402
    REG_DC_STATE = 405
    REG_SET_VOLTAGE = 500
    REG_SET_CURRENT = 501
    REG_SET_POWER = 502
    REG_SET_RESISTANCE = 503
    REG_DEVICE_STATE = 505
    REG_ACTUAL_VOLTAGE = 507
    REG_ACTUAL_CURRENT = 508
    REG_ACTUAL_POWER = 509
    REG_OVP = 550
    REG_OCP = 553
    REG_OPP = 556

    def __init__(self, client: ModbusTCPClient | ModbusRTUClient, ratings: DeviceRatings) -> None:
        self.client = client
        self.ratings = ratings

    def __enter__(self) -> "EAModbusBase":
        self.open()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def open(self) -> None:
        self.client.open()

    def close(self) -> None:
        self.client.close()

    def read_nominals(self) -> DeviceRatings:
        voltage = unpack_float_be(self.client.read_holding_registers(self.REG_NOMINAL_VOLTAGE, 2))
        current = unpack_float_be(self.client.read_holding_registers(self.REG_NOMINAL_CURRENT, 2))
        power = unpack_float_be(self.client.read_holding_registers(self.REG_NOMINAL_POWER, 2))
        return DeviceRatings(voltage_v=voltage, current_a=current, power_w=power)

    def set_remote(self, enabled: bool) -> None:
        self.client.write_single_coil(self.REG_REMOTE, enabled)

    def set_input_enabled(self, enabled: bool) -> None:
        self.client.write_single_coil(self.REG_DC_STATE, enabled)

    def set_output_enabled(self, enabled: bool) -> None:
        self.set_input_enabled(enabled)

    def set_source_current(self, amps: float) -> None:
        self.set_current(amps)

    def set_source_power(self, watts: float) -> None:
        self.set_power(watts)

    def set_source_resistance(self, ohms: float) -> None:
        self.set_resistance(ohms)

    def set_voltage(self, volts: float) -> None:
        self.client.write_single_register(
            self.REG_SET_VOLTAGE,
            _value_to_raw(volts, self.ratings.voltage_v, 1.02, EA_SET_VALUE_FULL_SCALE),
        )

    def set_current(self, amps: float) -> None:
        self.client.write_single_register(
            self.REG_SET_CURRENT,
            _value_to_raw(amps, self.ratings.current_a, 1.02, EA_SET_VALUE_FULL_SCALE),
        )

    def set_power(self, watts: float) -> None:
        self.client.write_single_register(
            self.REG_SET_POWER,
            _value_to_raw(watts, self.ratings.power_w, 1.02, EA_SET_VALUE_FULL_SCALE),
        )

    def set_resistance(self, ohms: float) -> None:
        if self.ratings.resistance_ohm_max is None:
            raise ValueError("Resistance rating unknown for this model")
        normalized = _normalize_resistance_value(ohms, self.ratings)
        self.client.write_single_register(
            self.REG_SET_RESISTANCE,
            _value_to_raw(normalized, self.ratings.resistance_ohm_max, 1.02, EA_SET_VALUE_FULL_SCALE),
        )

    def read_measurements(self) -> Measurement:
        regs = self.client.read_holding_registers(self.REG_ACTUAL_VOLTAGE, 3)
        return Measurement(
            voltage_v=_raw_to_value(regs[0], self.ratings.voltage_v, 1.25, EA_MEASURE_FULL_SCALE),
            current_a=_raw_to_value(regs[1], self.ratings.current_a, 1.25, EA_MEASURE_FULL_SCALE),
            power_w=_raw_to_value(regs[2], self.ratings.power_w, 1.25, EA_MEASURE_FULL_SCALE),
        )

    def read_status(self) -> EAStatus:
        regs = self.client.read_holding_registers(self.REG_DEVICE_STATE, 2)
        value = (regs[0] << 16) | regs[1]
        return decode_ea_device_state(value)

    def read_protection_thresholds(self) -> Measurement:
        return Measurement(
            voltage_v=_raw_to_value(
                self.client.read_holding_registers(self.REG_OVP, 1)[0],
                self.ratings.voltage_v,
                1.10,
                EA_PROTECTION_FULL_SCALE,
            ),
            current_a=_raw_to_value(
                self.client.read_holding_registers(self.REG_OCP, 1)[0],
                self.ratings.current_a,
                1.10,
                EA_PROTECTION_FULL_SCALE,
            ),
            power_w=_raw_to_value(
                self.client.read_holding_registers(self.REG_OPP, 1)[0],
                self.ratings.power_w,
                1.10,
                EA_PROTECTION_FULL_SCALE,
            ),
        )


class EAPSBModbusBase(EAModbusBase):
    REG_NOMINAL_RESISTANCE_MAX = 127
    REG_NOMINAL_RESISTANCE_MIN = 129
    REG_SINK_POWER = 498
    REG_SINK_CURRENT = 499
    REG_MODE = 409
    REG_SINK_RESISTANCE = 504

    def read_nominals(self) -> DeviceRatings:
        ratings = super().read_nominals()
        return DeviceRatings(
            voltage_v=ratings.voltage_v,
            current_a=ratings.current_a,
            power_w=ratings.power_w,
            resistance_ohm_min=unpack_float_be(self.client.read_holding_registers(self.REG_NOMINAL_RESISTANCE_MIN, 2)),
            resistance_ohm_max=unpack_float_be(self.client.read_holding_registers(self.REG_NOMINAL_RESISTANCE_MAX, 2)),
        )

    def set_sink_current(self, amps: float) -> None:
        self.client.write_single_register(
            self.REG_SINK_CURRENT,
            _value_to_raw(amps, self.ratings.current_a, 1.02, EA_SET_VALUE_FULL_SCALE),
        )

    def set_sink_power(self, watts: float) -> None:
        self.client.write_single_register(
            self.REG_SINK_POWER,
            _value_to_raw(watts, self.ratings.power_w, 1.02, EA_SET_VALUE_FULL_SCALE),
        )

    def set_sink_resistance(self, ohms: float) -> None:
        if self.ratings.resistance_ohm_max is None:
            raise ValueError("Resistance rating unknown for this model")
        normalized = _normalize_resistance_value(ohms, self.ratings)
        self.client.write_single_register(
            self.REG_SINK_RESISTANCE,
            _value_to_raw(normalized, self.ratings.resistance_ohm_max, 1.02, EA_SET_VALUE_FULL_SCALE),
        )

    def set_source_only_mode(self) -> None:
        self.set_sink_current(0.0)

    def set_sink_only_mode(self) -> None:
        self.set_voltage(0.0)

    def set_power_stage_mode(self, mode: str) -> None:
        normalized = mode.strip().upper()
        if normalized not in {"UIP", "UIR"}:
            raise ValueError(f"Unsupported power stage mode: {mode}")
        self.client.write_single_coil(self.REG_MODE, normalized == "UIR")

    def set_resistance_mode_enabled(self, enabled: bool) -> None:
        self.set_power_stage_mode("UIR" if enabled else "UIP")

    def read_measurements(self) -> Measurement:
        measurement = super().read_measurements()
        status = self.read_status()
        if not status.sink_mode:
            return measurement
        return Measurement(
            voltage_v=measurement.voltage_v,
            current_a=-measurement.current_a,
            power_w=-measurement.power_w,
        )


class EAPSB10060_60:
    RATINGS = DeviceRatings(
        voltage_v=60.0,
        current_a=60.0,
        power_w=1500.0,
        resistance_ohm_min=0.04,
        resistance_ohm_max=48.0,
    )

    @classmethod
    def scpi_tcp(cls, host: str, *, port: int = 5025, timeout: float = 2.0) -> EAPSBSCPIBase:
        return EAPSBSCPIBase(SocketSCPITransport(host=host, port=port, timeout=timeout), cls.RATINGS)

    @classmethod
    def scpi_serial(cls, port: str, *, baudrate: int = 115200, timeout: float = 1.0) -> EAPSBSCPIBase:
        return EAPSBSCPIBase(SerialSCPITransport(port=port, baudrate=baudrate, timeout=timeout), cls.RATINGS)

    @classmethod
    def modbus_tcp(cls, host: str, *, port: int = 502, unit_id: int = 0, timeout: float = 2.0) -> EAPSBModbusBase:
        return EAPSBModbusBase(ModbusTCPClient(host=host, port=port, unit_id=unit_id, timeout=timeout), cls.RATINGS)

    @classmethod
    def modbus_rtu(
        cls,
        port: str,
        *,
        baudrate: int = 115200,
        unit_id: int = 0,
        timeout: float = 0.5,
    ) -> EAPSBModbusBase:
        return EAPSBModbusBase(ModbusRTUClient(port=port, baudrate=baudrate, unit_id=unit_id, timeout=timeout), cls.RATINGS)


class EAEL9080_60DT:
    RATINGS = DeviceRatings(
        voltage_v=80.0,
        current_a=60.0,
        power_w=1200.0,
        resistance_ohm_min=0.09,
        resistance_ohm_max=30.0,
    )

    @classmethod
    def scpi_tcp(cls, host: str, *, port: int = 5025, timeout: float = 2.0) -> EAELSCPIBase:
        return EAELSCPIBase(SocketSCPITransport(host=host, port=port, timeout=timeout), cls.RATINGS)

    @classmethod
    def scpi_serial(cls, port: str, *, baudrate: int = 115200, timeout: float = 1.0) -> EAELSCPIBase:
        return EAELSCPIBase(SerialSCPITransport(port=port, baudrate=baudrate, timeout=timeout), cls.RATINGS)

    @classmethod
    def modbus_tcp(cls, host: str, *, port: int = 502, unit_id: int = 0, timeout: float = 2.0) -> EAModbusBase:
        raise NotImplementedError("EA-EL 9000 DT/T does not support Modbus TCP; use SCPI over TCP or Modbus RTU")

    @classmethod
    def modbus_rtu(
        cls,
        port: str,
        *,
        baudrate: int = 115200,
        unit_id: int = 0,
        timeout: float = 0.5,
    ) -> EAModbusBase:
        return EAModbusBase(ModbusRTUClient(port=port, baudrate=baudrate, unit_id=unit_id, timeout=timeout), cls.RATINGS)
