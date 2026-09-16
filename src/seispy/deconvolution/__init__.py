"""Instrument-response deconvolution workflows."""

from importlib import import_module

_EXPORTS = {
    "deconvolve_waveforms": (
        "seispy.deconvolution.removal",
        "deconvolve_waveforms",
    ),
    "remove_response_from_file": (
        "seispy.deconvolution.removal",
        "remove_response_from_file",
    ),
    "DeconvolutionIssue": (
        "seispy.deconvolution.removal",
        "DeconvolutionIssue",
    ),
    "DeconvolutionSummary": (
        "seispy.deconvolution.removal",
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
