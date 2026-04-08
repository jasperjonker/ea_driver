from __future__ import annotations

import socket
import struct
from dataclasses import dataclass, field

from .core import InstrumentError, TransportClosedError

try:
    import serial  # type: ignore
except ImportError:  # pragma: no cover
    serial = None


def crc16_modbus(data: bytes) -> int:
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            lsb = crc & 0x0001
            crc >>= 1
            if lsb:
                crc ^= 0xA001
    return crc


def pack_float_be(value: float) -> tuple[int, int]:
    raw = struct.pack(">f", value)
    return struct.unpack(">HH", raw)


def unpack_float_be(registers: list[int]) -> float:
    if len(registers) != 2:
        raise ValueError("Need exactly 2 registers for a float")
    raw = struct.pack(">HH", registers[0], registers[1])
    return struct.unpack(">f", raw)[0]


@dataclass(slots=True)
class ModbusTCPClient:
    host: str
    port: int = 502
    unit_id: int = 1
    timeout: float = 2.0
    _socket: socket.socket | None = None
    _transaction_id: int = field(default=0, init=False)

    def open(self) -> None:
        if self._socket is not None:
            return
        self._socket = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self._socket.settimeout(self.timeout)

    def close(self) -> None:
        if self._socket is None:
            return
        self._socket.close()
        self._socket = None

    def read_holding_registers(self, address: int, count: int) -> list[int]:
        response = self._request(0x03, struct.pack(">HH", address, count))
        byte_count = response[1]
        payload = response[2 : 2 + byte_count]
        return list(struct.unpack(f">{count}H", payload))

    def write_single_register(self, address: int, value: int) -> None:
        self._request(0x06, struct.pack(">HH", address, value))

    def write_single_coil(self, address: int, enabled: bool) -> None:
        self._request(0x05, struct.pack(">HH", address, 0xFF00 if enabled else 0x0000))

    def _request(self, function_code: int, payload: bytes) -> bytes:
        self._ensure_open()
        self._transaction_id = (self._transaction_id + 1) & 0xFFFF
        pdu = bytes([function_code]) + payload
        header = struct.pack(">HHHB", self._transaction_id, 0, len(pdu) + 1, self.unit_id)
        self._socket.sendall(header + pdu)
        mbap = self._recv_exact(7)
        _, _, length, unit_id = struct.unpack(">HHHB", mbap)
        if unit_id != self.unit_id:
            raise InstrumentError(f"Unexpected unit id {unit_id}")
        body = self._recv_exact(length - 1)
        if body[0] & 0x80:
            raise InstrumentError(f"Modbus exception {body[1]} for function {function_code:#x}")
        return body

    def _recv_exact(self, count: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < count:
            chunk = self._socket.recv(count - len(chunks))
            if not chunk:
                raise TransportClosedError("Modbus TCP socket closed by peer")
            chunks.extend(chunk)
        return bytes(chunks)

    def _ensure_open(self) -> None:
        if self._socket is None:
            raise TransportClosedError("Modbus TCP client is not open")


@dataclass(slots=True)
class ModbusRTUClient:
    port: str
    baudrate: int = 115200
    unit_id: int = 1
    timeout: float = 0.5
    _serial: object | None = None

    def open(self) -> None:
        if self._serial is not None:
            return
        if serial is None:  # pragma: no cover
            raise InstrumentError("pyserial is required for Modbus RTU support")
        self._serial = serial.Serial(port=self.port, baudrate=self.baudrate, timeout=self.timeout)

    def close(self) -> None:
        if self._serial is None:
            return
        self._serial.close()
        self._serial = None

    def read_holding_registers(self, address: int, count: int) -> list[int]:
        response = self._request(0x03, struct.pack(">HH", address, count), expected_min=5 + count * 2)
        byte_count = response[2]
        payload = response[3 : 3 + byte_count]
        return list(struct.unpack(f">{count}H", payload))

    def write_single_register(self, address: int, value: int) -> None:
        self._request(0x06, struct.pack(">HH", address, value), expected_min=8)

    def write_single_coil(self, address: int, enabled: bool) -> None:
        self._request(0x05, struct.pack(">HH", address, 0xFF00 if enabled else 0x0000), expected_min=8)

    def _request(self, function_code: int, payload: bytes, expected_min: int) -> bytes:
        self._ensure_open()
        frame = bytes([self.unit_id, function_code]) + payload
        crc = crc16_modbus(frame)
        frame += struct.pack("<H", crc)
        self._serial.reset_input_buffer()
        self._serial.write(frame)
        response = self._serial.read(expected_min)
        if len(response) < 5:
            raise TransportClosedError("Timed out waiting for Modbus RTU response")
        body, checksum = response[:-2], response[-2:]
        if struct.unpack("<H", checksum)[0] != crc16_modbus(body):
            raise InstrumentError("Invalid Modbus RTU CRC")
        if body[1] & 0x80:
            raise InstrumentError(f"Modbus exception {body[2]} for function {function_code:#x}")
        return body

    def _ensure_open(self) -> None:
        if self._serial is None:
            raise TransportClosedError("Modbus RTU client is not open")
