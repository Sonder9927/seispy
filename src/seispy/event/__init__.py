"""Event catalog and waveform-cutting public API."""

from importlib import import_module

_EXPORTS = {
    "cut_events": ("seispy.event.cut", "cut_events"),
    "CutEventIssue": ("seispy.event.cut", "CutEventIssue"),
    "CutEventSummary": ("seispy.event.cut", "CutEventSummary"),
    "cut_events_binary": ("seispy.event.cut_binary", "cut_events_binary"),
    "filter_events": ("seispy.event.catalog", "filter_events"),
    "write_event_catalog": ("seispy.event.catalog", "write_event_catalog"),
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
