# Elektro-Automatik Python Driver

`ea_driver` is a standalone Python package for EA Elektro-Automatik instruments.

Current models:

- EA PSB 10060-60
- EA-EL 9080-60 DT

The package bundles the two EA models because they share the same transport model and much of the same SCPI and Modbus behavior.

## Features

- SCPI over LAN TCP sockets
- SCPI over USB serial / virtual COM
- Modbus TCP over LAN
- Modbus RTU over USB serial / virtual COM
- Remote lock handling
- DC input/output enable control
- Voltage, current, power, and resistance setpoints
- Voltage, current, and power measurements
- EA device-state decoding
- Protection threshold readback

## Install

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

load = EAEL9080_60DT.modbus_rtu("/dev/ttyUSB0", baudrate=115200)
load.open()
load.set_remote(True)
load.set_current(20.0)
load.set_input_enabled(True)
print(load.read_measurements())
print(load.read_status())
load.close()
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
