"""Instrument-response removal workflows."""

from importlib import import_module

_EXPORTS = {
    "remove_instrument_response": (
        "seispy.response.removal",
        "remove_instrument_response",
    ),
    "remove_response_from_file": (
        "seispy.response.removal",
        "remove_response_from_file",
    ),
    "ResponseRemovalIssue": (
        "seispy.response.removal",
        "ResponseRemovalIssue",
    ),
    "ResponseRemovalSummary": (
        "seispy.response.removal",
        "ResponseRemovalSummary",
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
