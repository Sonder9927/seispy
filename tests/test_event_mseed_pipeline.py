"""Public archive-to-cut contract and multi-trace MiniSEED regression tests."""

from unittest.mock import patch

import numpy as np
import pytest
from obspy import Stream, Trace, UTCDateTime, read
from obspy.io.sac.util import get_sac_reftime

from seispy.event import cut_event_waveforms
from seispy.event.archive_index import WaveformArchiveIndex, WaveformReader
from seispy.event.cutting import cut_event_station
from seispy.waveform import archive_waveforms

BASE = UTCDateTime("2025-01-02")


def trace(offset=0, npts=11, *, channel="BHZ", station="WEL", bias=0):
    return Trace(
        data=np.arange(npts, dtype=np.int32) + bias,
        header=dict(
            network="NZ",
            station=station,
            location="",
            channel=channel,
            starttime=BASE + offset,
            sampling_rate=1,
        ),
    )


def event():
    return dict(
        start=BASE + 0.123456,
        end=BASE + 5.123456,
        latitude=1,
        longitude=2,
        depth=3,
        mag=4,
    )


@pytest.mark.parametrize("with_station_metadata", [False, True])
@pytest.mark.parametrize("segments", ["continuous", "cross_day", "gap", "overlap"])
def test_archive_to_cut_matches_sac_path(tmp_path, segments, with_station_metadata):
    raw = tmp_path / "raw"
    raw.mkdir()
    traces = {
        "continuous": [trace()],
        "cross_day": [trace(-5, 16)],
        "gap": [trace(0, 2), trace(4, 2)],
        "overlap": [trace(0, 5), trace(2, 5, bias=100)],
    }[segments]
    # Also exercise separate locations and channels through both public functions.
    traces += [trace(channel="BHN")]
    other_location = trace()
    other_location.stats.location = "10"
    traces.append(other_location)
    Stream(traces).write(raw / "response.mseed.raw", format="MSEED")
    catalog = tmp_path / "events.csv"
    catalog.write_text(
        "time,latitude,longitude,depth,mag\n2025-01-02T00:00:00.123456Z,1,2,3,4\n"
    )
    stations = None
    if with_station_metadata:
        stations = tmp_path / "stations.csv"
        stations.write_text(
            "network,station,latitude,longitude,elevation,depth\nNZ,WEL,10,20,30,40\n"
        )
    summaries = []
    outputs = []
    for fmt in ("mseed", "sac"):
        archive = tmp_path / fmt
        archived = archive_waveforms(
            raw,
            archive,
            output_format=fmt,
            max_workers=1,
            save_log=False,
            save_report=False,
        )
        assert archived.failed == 0
        assert archived.files_written > 0
        destination = tmp_path / f"events-{fmt}"
        summary = cut_event_waveforms(
            archive,
            catalog,
            output_dir=destination,
            pattern=f"*.{fmt}",
            station_csv=stations,
            time_window=5,
            max_workers=1,
            save_log=False,
            save_report=False,
        )
        assert summary.failed == summary.input_read_failed == 0
        summaries.append(summary)
        outputs.append(
            {p.relative_to(destination): read(p)[0] for p in destination.rglob("*.sac")}
        )
    assert outputs[0].keys() == outputs[1].keys()
    assert len(outputs[0]) == (4 if segments in {"gap", "overlap"} else 3)
    for name, actual in outputs[0].items():
        expected = outputs[1][name]
        np.testing.assert_array_equal(actual.data, expected.data)
        assert actual.id == expected.id
        assert actual.stats.starttime == expected.stats.starttime
        assert actual.stats.endtime == expected.stats.endtime
        for header in ("b", "e", "o", "evla", "evlo", "evdp", "mag", "iztype"):
            assert actual.stats.sac[header] == pytest.approx(expected.stats.sac[header])
        assert get_sac_reftime(actual.stats.sac) == BASE
        assert actual.stats.sac.o == pytest.approx(0.123456)
        if with_station_metadata:
            for header in (
                "stla",
                "stlo",
                "stel",
                "stdp",
                "dist",
                "az",
                "baz",
                "gcarc",
            ):
                assert actual.stats.sac[header] == pytest.approx(
                    expected.stats.sac[header]
                )
        else:
            assert all(
                h not in actual.stats.sac
                for h in ("stla", "stlo", "dist", "az", "baz", "gcarc")
            )
    for field in (
        "channels",
        "partial_channels",
        "gapped_channels",
        "overlapping_channels",
    ):
        assert getattr(summaries[0], field) == getattr(summaries[1], field)
    assert summaries[0].gapped_channels == int(segments == "gap")
    assert summaries[0].overlapping_channels == int(segments == "overlap")


def test_multitrace_file_indexes_every_segment_and_reuses_file_cache(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    Stream(
        [trace(-2, 4), trace(4, 2), trace(channel="BHN"), trace(station="OTHER")]
    ).write(source / "multi.mseed", format="MSEED")
    index = WaveformArchiveIndex.build(source, pattern="*.mseed")
    assert not index.issues
    assert index.station_keys == (("NZ", "OTHER"), ("NZ", "WEL"))
    assert len(index.records_for_station("NZ", "WEL")) == 3
    records = index.overlapping("NZ", "WEL", BASE - 1, BASE + 5)
    # The first segment spans midnight and must not appear twice.
    assert len(records) == 3
    reader = WaveformReader()
    with patch("seispy.event.archive_index.obspy.read", wraps=read) as load:
        result = cut_event_station(
            event(),
            {"network": "NZ", "station": "WEL"},
            tmp_path / "out",
            archive_index=index,
            waveform_reader=reader,
        )
        record = records[0]
        first = reader.read(record)
        first[0].data[:] = -999
        assert np.all(reader.read(record)[0].data != -999)
    assert load.call_count == 1
    assert result.read_failed == 0
    assert result.outputs == 3
    assert result.channels == 2
    assert result.gapped_channels == 1
    actual = sorted(
        (read(p)[0] for p in (tmp_path / "out").rglob("*.sac")),
        key=lambda tr: (tr.id, tr.stats.starttime),
    )
    assert all(tr.stats.station == "WEL" for tr in actual)
    z = [tr for tr in actual if tr.stats.channel == "BHZ"]
    np.testing.assert_array_equal(z[0].data, [2, 3])
    np.testing.assert_array_equal(z[1].data, [0, 1])
    assert result.coverage_samples[0].coverage_fraction == pytest.approx(4 / 6)


def test_multitrace_conflicting_overlap_preserves_both_sources(tmp_path):
    Stream([trace(0, 5), trace(2, 5, bias=100)]).write(
        tmp_path / "overlap.mseed", format="MSEED"
    )
    index = WaveformArchiveIndex.build(tmp_path, pattern="*.mseed")
    result = cut_event_station(
        event(),
        {"network": "NZ", "station": "WEL"},
        tmp_path / "out",
        archive_index=index,
    )
    assert result.outputs == 2
    assert result.overlapping_channels == 1
    arrays = [
        read(p)[0].data.tolist() for p in sorted((tmp_path / "out").rglob("*.sac"))
    ]
    assert arrays == [[0, 1, 2, 3, 4], [100, 101, 102, 103]]


def test_public_multitrace_cut_with_multiple_station_workers(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    Stream([trace(0, 2), trace(4, 2), trace(station="OTHER")]).write(
        source / "multi.mseed", format="MSEED"
    )
    catalog = tmp_path / "events.csv"
    catalog.write_text(
        "time,latitude,longitude,depth,mag\n"
        "2025-01-02T00:00:00.000000Z,1,2,3,4\n"
        "2025-01-02T00:00:01.250000Z,1,2,3,4\n"
    )
    summary = cut_event_waveforms(
        source,
        catalog,
        pattern="*.mseed",
        output_dir=tmp_path / "out",
        time_window=5,
        max_workers=2,
        save_log=False,
        save_report=False,
    )
    assert summary.total == summary.succeeded == 4
    assert summary.failed == summary.input_read_failed == 0
    assert summary.outputs_written == 6
    assert summary.gapped_channels == 2


def test_multitrace_index_filters_stations_and_reader_detects_changed_headers(tmp_path):
    path = tmp_path / "multi.mseed"
    original = Stream([trace(), trace(station="OTHER")])
    original.write(path, format="MSEED")
    index = WaveformArchiveIndex.build(
        tmp_path, pattern="*.mseed", stations={("NZ", "WEL")}
    )
    assert index.station_keys == (("NZ", "WEL"),)
    record = index.records_for_station("NZ", "WEL")[0]
    original[0].stats.sampling_rate = 2
    original.write(path, format="MSEED")
    with pytest.raises(ValueError, match="headers changed"):
        WaveformReader().read(record)
