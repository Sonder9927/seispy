"""Lazy station-correction workflow interfaces."""

from importlib import import_module

_EXPORTS = {
    "correct_clock_drift": ("seispy.correct.clock_drift", "correct_clock_drift"),
    "correct_orientation": ("seispy.correct.orientation", "correct_orientation"),
    "CorrectionIssue": ("seispy.correct.summary", "CorrectionIssue"),
    "CorrectionSummary": ("seispy.correct.summary", "CorrectionSummary"),
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
