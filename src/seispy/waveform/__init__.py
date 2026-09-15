"""Lazy waveform preparation and processing interfaces."""

from importlib import import_module

_EXPORTS = {
    "archive_waveforms": ("seispy.waveform.archiving", "archive_waveforms"),
    "WaveformArchiveIssue": (
        "seispy.waveform.archiving",
        "WaveformArchiveIssue",
    ),
    "WaveformArchiveSummary": (
        "seispy.waveform.archiving",
        "WaveformArchiveSummary",
    ),
    "convert_mseed_to_sac": ("seispy.waveform.conversion", "convert_mseed_to_sac"),
    "waveform_coverage": ("seispy.waveform.coverage", "waveform_coverage"),
    "scan_waveform_coverage": (
        "seispy.waveform.coverage",
        "scan_waveform_coverage",
    ),
    "summarize_waveform_coverage": (
        "seispy.waveform.coverage",
        "summarize_waveform_coverage",
    ),
    "plot_waveform_coverage": (
        "seispy.waveform.coverage",
        "plot_waveform_coverage",
    ),
    "WaveformCoverageReport": (
        "seispy.waveform.coverage",
        "WaveformCoverageReport",
    ),
    "WaveformConversionIssue": (
        "seispy.waveform.conversion",
        "WaveformConversionIssue",
    ),
    "WaveformConversionSummary": (
        "seispy.waveform.conversion",
        "WaveformConversionSummary",
    ),
    "decimate_waveforms": ("seispy.waveform.decimation", "decimate_waveforms"),
    "DecimationIssue": ("seispy.waveform.decimation", "DecimationIssue"),
    "DecimationSummary": ("seispy.waveform.decimation", "DecimationSummary"),
    "format_sac_headers": ("seispy.waveform.headers", "format_sac_headers"),
    "SacHeaderIssue": ("seispy.waveform.headers", "SacHeaderIssue"),
    "SacHeaderSummary": ("seispy.waveform.headers", "SacHeaderSummary"),
    "merge_waveforms_by_day": ("seispy.waveform.merge", "merge_waveforms_by_day"),
    "sort_waveforms": ("seispy.waveform.organization", "sort_waveforms"),
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
