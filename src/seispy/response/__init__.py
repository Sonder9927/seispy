"""Instrument metadata and response-removal public API."""

from importlib import import_module

_EXPORTS = {
    "combine_inventories": ("seispy.response.inventory", "combine_inventories"),
    "select_inventory": ("seispy.response.inventory", "select_inventory"),
    "shift_channel_starttime": (
        "seispy.response.inventory",
        "shift_channel_starttime",
    ),
    "write_inventory": ("seispy.response.inventory", "write_inventory"),
    "deconvolution_by_station": (
        "seispy.response.remove_response",
        "deconvolution_by_station",
    ),
    "stream_removed_response": (
        "seispy.response.remove_response",
        "stream_removed_response",
    ),
    "DeconvolutionResult": (
        "seispy.response.remove_response",
        "DeconvolutionResult",
    ),
    "DeconvolutionSummary": (
        "seispy.response.remove_response",
        "DeconvolutionSummary",
    ),
}


def __getattr__(name: str):
    try:
        module_name, attribute = _EXPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))


__all__ = list(_EXPORTS)
