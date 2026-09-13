"""Task-oriented interfaces for reproducible seismic-data workflows."""

from importlib import import_module
from typing import TYPE_CHECKING

_MODULES = {
    name: f"seispy.{name}"
    for name in (
        "archive",
        "correct",
        "download",
        "event",
        "inventory",
        "mcmc",
        "response",
        "waveform",
        "workflow",
    )
}

if TYPE_CHECKING:
    from seispy import (
        archive,
        correct,
        download,
        event,
        inventory,
        mcmc,
        response,
        waveform,
        workflow,
    )


def __getattr__(name: str):
    try:
        module_name = _MODULES[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    value = import_module(module_name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))


__all__ = list(_MODULES)
