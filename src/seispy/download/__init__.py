"""FDSN downloads for metadata, earthquake catalogs and waveforms."""

from seispy.download.events import download_earthquake_events
from seispy.download.inventory import (
    ResponseConflictError,
    ResponseConflictWarning,
    download_inventory,
)
from seispy.download.mass import MassDownloadResult, download_waveforms_mass
from seispy.download.stats import (
    DownloadAnalysis,
    download_status,
    plot_download_availability,
    scan_download_availability,
    summarize_download_availability,
)
from seispy.download.waveform import WaveformDownloadSummary, download_waveforms

__all__ = [
    "download_inventory",
    "ResponseConflictError",
    "ResponseConflictWarning",
    "download_earthquake_events",
    "download_waveforms",
    "download_waveforms_mass",
    "download_status",
    "scan_download_availability",
    "summarize_download_availability",
    "plot_download_availability",
    "DownloadAnalysis",
    "WaveformDownloadSummary",
    "MassDownloadResult",
]
