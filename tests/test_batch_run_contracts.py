import inspect

from seispy import collate, decimate_files, download, event, response


def test_public_batch_functions_persist_reports_and_logs_by_default():
    functions = (
        download.download_waveforms,
        download.download_waveforms_mass,
        collate.mseed2sac,
        collate.format_head,
        decimate_files,
        response.deconvolution_by_station,
        event.cut_events,
    )

    for function in functions:
        parameters = inspect.signature(function).parameters
        assert parameters["save_report"].default is True
        assert parameters["save_log"].default is True
