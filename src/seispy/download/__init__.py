"""Lazy FDSN download interfaces."""

from importlib import import_module

_EXPORTS = {
    "download_earthquake_events": (
        "seispy.download.catalog",
        "download_earthquake_events",
    ),
    "download_inventory": ("seispy.download.stations", "download_inventory"),
    "ResponseConflictError": ("seispy.download.stations", "ResponseConflictError"),
    "ResponseConflictWarning": ("seispy.download.stations", "ResponseConflictWarning"),
    "mass_download_waveforms": ("seispy.download.bulk", "mass_download_waveforms"),
    "BulkDownloadSummary": ("seispy.download.bulk", "BulkDownloadSummary"),
    "download_waveforms": ("seispy.download.waveforms", "download_waveforms"),
    "WaveformDownloadSummary": ("seispy.download.waveforms", "WaveformDownloadSummary"),
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
