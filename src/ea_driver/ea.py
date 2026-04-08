from __future__ import annotations

from dataclasses import dataclass

from .core import DeviceRatings, Measurement
from .modbus import ModbusRTUClient, ModbusTCPClient, unpack_float_be
from .scpi import SCPIDevice, SCPITransport, SerialSCPITransport, SocketSCPITransport

EA_SET_VALUE_FULL_SCALE = 0xD0E5
EA_MEASURE_FULL_SCALE = 0xFFFF
EA_PROTECTION_FULL_SCALE = 0xE147


def _percent_to_raw(percent: float, full_scale: int) -> int:
    return max(0, min(full_scale, round(percent * full_scale)))


def _raw_to_percent(raw: int, full_scale: int) -> float:
    return raw / full_scale


def _value_to_raw(value: float, nominal: float, percent_limit: float, full_scale: int) -> int:
    percent = 0.0 if nominal == 0 else min(max(value / nominal, 0.0), percent_limit)
    return _percent_to_raw(percent / percent_limit, full_scale)


def _raw_to_value(raw: int, nominal: float, percent_limit: float, full_scale: int) -> float:
    return nominal * (_raw_to_percent(raw, full_scale) * percent_limit)


@dataclass(frozen=True, slots=True)
class EAStatus:
    control_location: int
    dc_on: bool
    regulation_mode: str
    remote: bool
    function_generator_running: bool
    remote_sensing: bool
    alarms_active: bool
    ovp_active: bool
    ocp_active: bool
    opp_active: bool
    over_temperature: bool
    power_fail: bool
    rem_sb_inhibiting: bool


def decode_ea_device_state(value: int) -> EAStatus:
    control_location = value & 0x1F
    mode_bits = (value >> 9) & 0x03
    regulation_mode = {0b00: "CV", 0b01: "CR", 0b10: "CC", 0b11: "CP"}[mode_bits]
    return EAStatus(
        control_location=control_location,
        dc_on=bool((value >> 7) & 0x1),
        regulation_mode=regulation_mode,
        remote=bool((value >> 11) & 0x1),
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

    def set_source_current(self, amps: float) -> None:
        self.write(f"CURR {amps}")

    def set_current(self, amps: float) -> None:
        self.set_source_current(amps)

    def set_source_power(self, watts: float) -> None:
        self.write(f"POW {watts}")

    def set_power(self, watts: float) -> None:
        self.set_source_power(watts)

    def set_source_resistance(self, ohms: float) -> None:
        self.write(f"RES {ohms}")

    def set_resistance(self, ohms: float) -> None:
        self.set_source_resistance(ohms)

    def set_sink_current(self, amps: float) -> None:
        self.write(f"SINK:CURR {amps}")

    def measure_voltage(self) -> float:
        return float(self.query("MEAS:VOLT?").rstrip("V"))

    def measure_current(self) -> float:
        return float(self.query("MEAS:CURR?").rstrip("A"))

    def measure_power(self) -> float:
        return float(self.query("MEAS:POW?").rstrip("W"))

    def measure_all(self) -> Measurement:
        parts = [item.strip() for item in self.query("MEAS:ARR?").split(",")]
        return Measurement(
            voltage_v=float(parts[0].rstrip("V")),
            current_a=float(parts[1].rstrip("A")),
            power_w=float(parts[2].rstrip("W")),
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
        self.client.write_single_register(
            self.REG_SET_RESISTANCE,
            _value_to_raw(ohms, self.ratings.resistance_ohm_max, 1.02, EA_SET_VALUE_FULL_SCALE),
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


class EAPSB10060_60:
    RATINGS = DeviceRatings(voltage_v=60.0, current_a=1000.0, power_w=30000.0)

    @classmethod
    def scpi_tcp(cls, host: str, *, port: int = 5025, timeout: float = 2.0) -> EASCPIBase:
        return EASCPIBase(SocketSCPITransport(host=host, port=port, timeout=timeout), cls.RATINGS)

    @classmethod
    def scpi_serial(cls, port: str, *, baudrate: int = 115200, timeout: float = 1.0) -> EASCPIBase:
        return EASCPIBase(SerialSCPITransport(port=port, baudrate=baudrate, timeout=timeout), cls.RATINGS)

    @classmethod
    def modbus_tcp(cls, host: str, *, port: int = 502, unit_id: int = 0, timeout: float = 2.0) -> EAModbusBase:
        return EAModbusBase(ModbusTCPClient(host=host, port=port, unit_id=unit_id, timeout=timeout), cls.RATINGS)

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
