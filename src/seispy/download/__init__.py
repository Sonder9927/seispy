"""FDSN downloads for metadata, earthquake catalogs and waveforms."""

from seispy.download.events import download_earthquake_events
from seispy.download.inventory import download_inventory
from seispy.download.waveform import WaveformDownloadSummary, download_waveforms

__all__ = [
    "download_inventory",
    "download_earthquake_events",
    "download_waveforms",
    "WaveformDownloadSummary",
]
