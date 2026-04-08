# Elektro-Automatik Python Driver

`ea_driver` is a standalone Python package for EA Elektro-Automatik instruments.

Current models:

- EA PSB 10060-60
- EA-EL 9080-60 DT

The package bundles the two EA models because they share the same transport model and much of the same SCPI and Modbus behavior.

## Features

- SCPI over LAN TCP sockets
- SCPI over USB serial / virtual COM
- Modbus TCP over LAN on supported models
- Modbus RTU over USB serial / virtual COM
- Remote lock handling
- DC input/output enable control
- Voltage, current, power, and resistance setpoints
- Voltage, current, and power measurements
- EA device-state decoding
- Protection threshold readback

## Install

With `uv`:

```bash
uv sync
```

With USB serial support:

```bash
uv sync --extra serial
```

Or with `pip`:

```bash
pip install -e .
```

With USB serial support:

```bash
pip install -e .[serial]
```

## Usage

### PSB over LAN SCPI

```python
from ea_driver import EAPSB10060_60

psu = EAPSB10060_60.scpi_tcp("192.168.0.42")
psu.open()
psu.set_remote(True)
psu.set_voltage(54.0)
psu.set_source_current(10.0)
psu.set_output_enabled(True)
print(psu.measure_all())
psu.close()
```

### EL over USB Modbus RTU

```python
from ea_driver import EAEL9080_60DT

load = EAEL9080_60DT.modbus_rtu("/dev/ttyUSB0", baudrate=115200, unit_id=0)
load.open()
load.set_remote(True)
load.set_current(20.0)
load.set_input_enabled(True)
print(load.read_measurements())
print(load.read_status())
load.close()
```

For devices left in the default "Limited" Modbus compliance mode, use RTU slave address `0`.
If you switch the device to "Full" Modbus compliance, slave address `1` is also supported.
For Modbus TCP, use unit identifier `0`. The library now defaults to `0` for supported Modbus TCP models.
For `EA-EL 9000 DT / T`, Modbus TCP is not supported by the device series; use SCPI over LAN or Modbus RTU instead.

### LAN Notes

SCPI over LAN uses a normal TCP socket on port `5025`.
Modbus TCP uses port `502`.

Be aware that EA devices can close idle TCP socket connections after a timeout. If your application keeps a
connection open for long gaps between commands, either reopen the connection before the next operation or set
the device's TCP timeout / keep-alive settings appropriately on the instrument.

### EL over USB Modbus RTU with Terminal Logging, Kelvin Sense, 1 A CC

Enable remote sensing / Kelvin mode on the instrument first. The example below verifies that the
device reports `remote_sensing=True` before it enables the load.

```python
import logging
import time

from ea_driver import EAEL9080_60DT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("ea_driver.example")

port = "/dev/serial/by-id/usb-EA_Elektro-Automatik_GmbH___Co._KG_EL_9080-60_DT_2228100002-if00"
load = EAEL9080_60DT.modbus_rtu(port, baudrate=115200, unit_id=0)

load.open()
try:
    status = load.read_status()
    log.info("Initial status: %s", status)
    if not status.remote_sensing:
        raise RuntimeError("Kelvin / remote sensing is not active on the load")

    load.set_remote(True)
    time.sleep(0.3)

    load.set_current(1.0)
    time.sleep(0.1)
    load.set_input_enabled(True)
    time.sleep(0.5)

    status = load.read_status()
    measurement = load.read_measurements()
    log.info("Under load status: %s", status)
    log.info(
        "Under load measurement: %.3f V, %.3f A, %.3f W",
        measurement.voltage_v,
        measurement.current_a,
        measurement.power_w,
    )

    if status.regulation_mode != "CC":
        raise RuntimeError(f"Expected CC mode, got {status.regulation_mode}")
finally:
    try:
        load.set_input_enabled(False)
    finally:
        try:
            load.set_remote(False)
        finally:
            load.close()
```

### Verification CLI

The packaged verifier uses Python `logging` and can log directly to the terminal:

```bash
uv sync --extra serial
uv run ea-driver-verify \
  --port /dev/serial/by-id/usb-EA_Elektro-Automatik_GmbH___Co._KG_EL_9080-60_DT_2228100002-if00 \
  --exercise-modbus \
  --exercise-current-a 1.0 \
  --exercise-duration-s 2 \
  --require-remote-sensing \
  --log-level INFO
```

You can also run it as a module:

```bash
uv sync --extra serial
uv run python -m ea_driver.verify --help
```

## Build

```bash
hatch build
```

or:

```bash
uv run --with hatch hatch build
```

## Versioning

Versions come from git tags via `hatch-vcs`.

Suggested tag format:

```bash
git tag v0.1.0
```

If no git metadata is available, the build falls back to `0.1.0.dev0`.

## Release Readiness

Before publishing to PyPI:

- add a `LICENSE`
- add real project URLs in `pyproject.toml`
- validate the drivers on real hardware
- build from a tagged git checkout
