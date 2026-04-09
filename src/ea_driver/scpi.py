from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import Protocol

from .core import InstrumentError, TransportClosedError

try:
    import serial  # type: ignore
except ImportError:  # pragma: no cover
    serial = None


class SCPITransport(Protocol):
    def open(self) -> None: ...
    def close(self) -> None: ...
    def write(self, command: str) -> None: ...
    def query(self, command: str) -> str: ...


@dataclass(slots=True)
class SocketSCPITransport:
    host: str
    port: int = 5025
    timeout: float = 2.0
    terminator: bytes = b"\n"

    _socket: socket.socket | None = None
    _buffer: bytes = b""

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
        self._buffer = b""

    def write(self, command: str) -> None:
        self._ensure_open()
        self._socket.sendall(command.encode("ascii") + self.terminator)

    def query(self, command: str) -> str:
        self.write(command)
        return self._readline()

    def _readline(self) -> str:
        self._ensure_open()
        while self.terminator not in self._buffer:
            chunk = self._socket.recv(4096)
            if not chunk:
                raise TransportClosedError("SCPI socket closed by peer")
            self._buffer += chunk
        line, self._buffer = self._buffer.split(self.terminator, 1)
        return line.decode("ascii", errors="replace").strip()

    def _ensure_open(self) -> None:
        if self._socket is None:
            raise TransportClosedError("SCPI socket transport is not open")


@dataclass(slots=True)
class SerialSCPITransport:
    port: str
    baudrate: int = 115200
    timeout: float = 1.0
    terminator: bytes = b"\n"

    _serial: object | None = None

    def open(self) -> None:
        if self._serial is not None:
            return
        if serial is None:  # pragma: no cover
            raise InstrumentError("pyserial is required for serial SCPI support")
        self._serial = serial.Serial(port=self.port, baudrate=self.baudrate, timeout=self.timeout)

    def close(self) -> None:
        if self._serial is None:
            return
        self._serial.close()
        self._serial = None

    def write(self, command: str) -> None:
        self._ensure_open()
        self._serial.write(command.encode("ascii") + self.terminator)

    def query(self, command: str) -> str:
        self.write(command)
        line = self._serial.readline()
        if not line:
            raise TransportClosedError("Timed out waiting for SCPI serial response")
        return line.decode("ascii", errors="replace").strip()

    def _ensure_open(self) -> None:
        if self._serial is None:
            raise TransportClosedError("SCPI serial transport is not open")


class SCPIDevice:
    def __init__(self, transport: SCPITransport) -> None:
        self.transport = transport

    def __enter__(self) -> "SCPIDevice":
        self.open()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def open(self) -> None:
        self.transport.open()

    def close(self) -> None:
        self.transport.close()

    def write(self, command: str) -> None:
        self.transport.write(command)

    def query(self, command: str) -> str:
        return self.transport.query(command)

    def identify(self) -> str:
        return self.query("*IDN?")

    def clear_status(self) -> None:
        self.write("*CLS")

    def next_error(self) -> str:
        return self.query("SYST:ERR?")

    def read_errors(self, *, max_errors: int = 5) -> list[str]:
        errors: list[str] = []
        for _ in range(max_errors):
            error = self.next_error()
            errors.append(error)
            if error.startswith("0,"):
                break
        return errors
