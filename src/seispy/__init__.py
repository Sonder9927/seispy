"""SeisPy public API with lazy imports for optional and heavy modules."""

from importlib import import_module
from typing import TYPE_CHECKING

_MODULES = {
    name: f"seispy.{name}"
    for name in ("collate", "correct", "download", "event", "mcmc", "response")
}
_ATTRS = {
    "decimate_files": ("seispy.decimate", "decimate_files"),
    "DecimationIssue": ("seispy.decimate", "DecimationIssue"),
    "DecimationSummary": ("seispy.decimate", "DecimationSummary"),
}

if TYPE_CHECKING:
    from seispy import collate, correct, download, event, mcmc, response
    from seispy.decimate import (
        DecimationIssue,
        DecimationSummary,
        decimate_files,
    )


def __getattr__(name: str):
    if name in _MODULES:
        value = import_module(_MODULES[name])
    elif name in _ATTRS:
        module_name, attribute = _ATTRS[name]
        value = getattr(import_module(module_name), attribute)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))


__all__ = [
    "collate",
    "correct",
    "download",
    "event",
    "mcmc",
    "response",
    "decimate_files",
    "DecimationIssue",
    "DecimationSummary",
]
