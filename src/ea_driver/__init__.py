from .ea import EAEL9080_60DT, EAPSB10060_60

try:
    from ._version import version as __version__
except ImportError:  # pragma: no cover
    from importlib.metadata import version as _pkg_version

    try:
        __version__ = _pkg_version("ea_driver")
    except Exception:  # pragma: no cover
        __version__ = "0.0.0+unknown"

__all__ = [
    "EAEL9080_60DT",
    "EAPSB10060_60",
    "__version__",
]
