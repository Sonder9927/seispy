import inspect

from seispy import correct, deconvolution, download, event, waveform
from seispy.workflow import BatchSummary


def test_public_batch_functions_persist_reports_and_logs_by_default():
    functions = (
        download.download_waveforms,
        download.mass_download_waveforms,
        waveform.convert_mseed_to_sac,
        waveform.format_sac_headers,
        waveform.decimate_waveforms,
        deconvolution.remove_instrument_response,
        event.cut_event_waveforms,
        correct.correct_clock_drift,
        correct.correct_orientation,
    )

    for function in functions:
        parameters = inspect.signature(function).parameters
        assert parameters["save_report"].default is True
        assert parameters["save_log"].default is True


def test_batch_summaries_share_one_counter_and_issue_contract():
    summary_types = (
        download.BulkDownloadSummary,
        download.WaveformDownloadSummary,
        waveform.WaveformConversionSummary,
        waveform.SacHeaderSummary,
        waveform.DecimationSummary,
        deconvolution.DeconvolutionSummary,
        event.CutEventSummary,
        correct.CorrectionSummary,
    )

    for summary_type in summary_types:
        assert issubclass(summary_type, BatchSummary)
        assert {"total", "succeeded", "failed", "issue_samples"}.issubset(
            summary_type.__dataclass_fields__
        )
