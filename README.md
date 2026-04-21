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

Or with `pip`:

```bash
pip install -e .
```

## Usage

### PSB over LAN SCPI

```python
from ea_driver import EAPSB10060_60

with EAPSB10060_60.scpi_tcp("192.168.0.42") as psu:
    psu.set_remote(True)
    psu.set_voltage(24.0)
    psu.set_source_current(10.0)
    psu.set_output_enabled(True)
    print(psu.measure_all())
```

### EL over USB Modbus RTU

```python
from ea_driver import EAEL9080_60DT

with EAEL9080_60DT.modbus_rtu("/dev/ttyUSB0", baudrate=115200, unit_id=0) as load:
    load.set_remote(True)
    load.set_current(20.0)
    load.set_input_enabled(True)
    print(load.read_measurements())
    print(load.read_status())
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

### Example Scripts

The repository includes three plain Python example files:

- `examples/el_complete.py`
- `examples/el_profile.py`
- `examples/psb_complete.py`

These are intended to be run after `uv sync` or `pip install -e .`, so they import the installed
package directly with `from ea_driver import ...`.

The two EL examples are fixed-configuration scripts on purpose: you edit either the constants
inside the Python file or the sibling YAML profile instead of using `argparse`. Both EL scripts
prompt for the battery serial number before they touch the device and write timestamped CSV logs
to `logging/`.

### EL Examples

`examples/el_complete.py` is the smallest EL example. It performs a constant-current discharge
at a fixed current until the measured battery voltage falls below the configured cutoff voltage
for a few consecutive samples.

```bash
uv sync
uv run python examples/el_complete.py
```

Edit the constants at the top of the file if you want a different current, cutoff voltage,
sample interval, or transport (`usb-modbus`, `usb-scpi`, or `lan-scpi`).

`examples/el_profile.py` is the more advanced EL example. It auto-loads the editable sibling
file `examples/el_profile.yaml` and runs the listed stages in sequence while logging every sample.
The stage sequence is defined only in the YAML file, not duplicated in Python.
Supported stage modes are `off`, `cv`, `cc`, `cp`, and `cr`, and each stage can stop on
`duration_s` and/or `cutoff_voltage_v`.

```bash
uv run python examples/el_profile.py
```

This is intended for battery discharge profiles where you want to keep the connection settings,
device protections, device adjustment limits, and stage sequence in YAML instead of editing
Python code for every run.

The YAML file now has distinct sections for:

- `protections`: EL `OVP`, `OCP`, and `OPP`
- `limits`: device adjustment limits such as `U-Min`, `U-Max`, `I-Min`, `I-Max`, `P-Max`, and `R-Max`
- `stages`: the actual test sequence

### PSB Example

The PSB example script in `examples/psb_complete.py` supports:

- USB Modbus RTU for Kelvin-aware runs with `remote_sensing` status checks
- USB SCPI
- Ethernet SCPI
- Ethernet Modbus TCP
- source and sink current workflows
- optional source power and resistance setpoints
- explicit source-only and sink-only helpers according to the PSB manual

By default it is configured for USB Modbus RTU source mode with a 4-wire / Kelvin check. Run it with:

```bash
uv sync
uv run python examples/psb_complete.py
```

To switch it to Ethernet sink mode over SCPI:

```bash
uv run python examples/psb_complete.py --transport lan-scpi --host 192.168.0.50 --mode sink
```

To inspect the effective configuration without opening the device:

```bash
uv run python examples/psb_complete.py --print-config
```

On PSB 10000 devices, remote sensing is enabled by wiring the rear `Sense` connector and the instrument
detects it automatically. The library does not expose a command to turn Kelvin sensing on or off because EA's
programming interface exposes it as a status bit, not a software toggle. If you want the example to verify that
4-wire sensing is active, keep it on `usb-modbus` or switch it to `lan-modbus`. EA's operating manual also
notes that remote sensing is only effective during constant-voltage operation.

The PSB API also now exposes PSB-specific helpers beyond the generic PSU methods, for example:

- `set_source_only_mode()` to force source-only behavior by setting the sink current to `0`
- `set_sink_only_mode()` to force sink-only behavior by setting the voltage to `0`
- `set_power_stage_mode("UIP" | "UIR")` and `set_resistance_mode_enabled(...)`
- `set_sink_current(...)`, `set_sink_power(...)`, `set_sink_resistance(...)`
- function-generator entry points such as `select_function_generator_mode(...)`, `configure_arbitrary_sequence_point(...)`, and `load_xy_table(...)`

Without a connected DUT, battery, or external load, the PSB example will still exercise control,
logging, CSV capture, and measurement readback, but measured power transfer will stay near zero.

### Battery Test Example

The repository also includes `examples/battery_performance_test.py` plus the
editable sibling config file `examples/battery_performance_test.yaml` for staged
discharge / rest / recharge workflows on the PSB.

The example auto-loads that YAML file, so the normal workflow is simply:

```bash
uv run python examples/battery_performance_test.py
```

Battery-dependent defaults are derived from `battery_config`, so changing
`series_cells` or `max_per_cell_v` also updates the default charge voltage and
source-side voltage protections. To trim or tweak the default sequence without
copying the whole `stages` list, use `stage_overrides` keyed by stage name.

You can still point at another config file or print the fully expanded runtime
config:

```bash
uv run python examples/battery_performance_test.py --config /tmp/my_profile.yaml
uv run python examples/battery_performance_test.py --print-config > /tmp/expanded_profile.yaml
```

For example, this is enough to switch to an `8S` pack and keep only a tiny
sanity discharge plus a short rest:

```yaml
battery_config:
  series_cells: 8

stage_overrides:
  sanity_1a:
    current_a: 0.2
    duration_s: 2.0
  burst_50a:
    enabled: false
  sustained_30a:
    enabled: false
  cruise_15a_to_cutoff:
    enabled: false
  post_discharge_recovery:
    duration_s: 2.0
  recharge_cc_cv:
    enabled: false
  post_charge_recovery:
    enabled: false
```

### Verification CLI

The packaged verifier uses Python `logging` and can log directly to the terminal:

```bash
uv sync
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
uv sync
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
