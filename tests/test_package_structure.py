"""Contracts for the supported 0.1 package structure."""

import seispy


def test_top_level_exposes_only_supported_domains():
    assert set(seispy.__all__) == {
        "archive",
        "correct",
        "download",
        "event",
        "inventory",
        "mcmc",
        "response",
        "waveform",
        "workflow",
    }


def test_waveform_interface_uses_task_oriented_names():
    from seispy import waveform

    expected = {
        "convert_mseed_to_sac",
        "decimate_waveforms",
        "format_sac_headers",
        "merge_waveforms_by_day",
        "sort_waveforms",
        "DecimationIssue",
        "DecimationSummary",
        "SacHeaderIssue",
        "SacHeaderSummary",
        "WaveformConversionIssue",
        "WaveformConversionSummary",
    }
    assert set(waveform.__all__) == expected


def test_each_operation_has_one_public_owner():
    from seispy import inventory, response

    assert callable(inventory.combine_inventories)
    assert callable(response.remove_instrument_response)
    assert not hasattr(response, "combine_inventories")
    assert not hasattr(response, "analyze_inventory")
