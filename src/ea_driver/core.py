from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Measurement:
    voltage_v: float
    current_a: float
    power_w: float


@dataclass(frozen=True, slots=True)
class DeviceRatings:
    voltage_v: float
    current_a: float
    power_w: float
    resistance_ohm_min: float | None = None
    resistance_ohm_max: float | None = None


class InstrumentError(RuntimeError):
    pass


class TransportClosedError(InstrumentError):
    pass
