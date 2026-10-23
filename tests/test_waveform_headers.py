"""Public SAC metadata contracts using disposable binary waveforms."""

from importlib import import_module
import struct
from unittest.mock import patch

import numpy as np
from obspy.io.sac import SACTrace
from obspy.io.sac.header import FLOATHDRS, INTHDRS
import pytest

module = import_module("seispy.waveform.headers")
FOOTER = (
    "delta",
    "b",
    "e",
    "o",
    "a",
    *[f"t{i}" for i in range(10)],
    "f",
    "evlo",
    "evla",
    "stlo",
    "stla",
    "internal1",
    "internal2",
)


def make_sac(
    path, *, channel="BHZ", network="XX", station="GL04", byteorder="little", version=6
):
    path.parent.mkdir(parents=True, exist_ok=True)
    sac = SACTrace(
        data=np.arange(103, dtype=np.float32),
        delta=0.2,
        knetwk=network,
        kstnm=station,
        khole="",
        kcmpnm=channel,
        nzyear=2024,
        nzjday=352,
        nzhour=22,
        nzmin=10,
        nzsec=40,
        b=0.005,
        o=0.83,
        a=2.125,
        t0=4.5,
        lcalda=False,
        stla=1.0,
        stlo=2.0,
        evla=3.0,
        evlo=4.0,
        dist=1.0,
    )
    sac.write(str(path), byteorder=byteorder)
    if version == 7:
        endian = "<" if byteorder == "little" else ">"
        raw = bytearray(path.read_bytes())
        struct.pack_into(endian + "i", raw, 280 + 4 * INTHDRS.index("nvhdr"), 7)
        values = [
            struct.unpack_from(endian + "f", raw, 4 * FLOATHDRS.index(k))[0]
            for k in FOOTER
        ]
        # A footer contains more precision than the float header: preserve it.
        values[1] = 0.005000000123
        raw.extend(struct.pack(endian + "22d", *values))
        path.write_bytes(raw)
    return path


@pytest.fixture
def tables(tmp_path):
    stations = tmp_path / "stations.csv"
    stations.write_text(
        "network,station,latitude,longitude,elevation\nXX,GL04,35,115,2872\n"
    )
    events = tmp_path / "events.csv"
    events.write_text(
        "time,latitude,longitude,depth,mag\n2024-12-17T22:10:40.830Z,30,110,10,5.6\n"
    )
    return stations, events


def run(src, dest=None, tables=None, *, event=False, **kwargs):
    stations, events = tables
    return module.format_sac_headers(
        src,
        dest,
        events if event else None,
        stations,
        save_report=False,
        save_log=False,
        **kwargs,
    )


@pytest.mark.parametrize("workers", [1, 4])
def test_real_event_layout_preserves_components_segments_and_empty_location(
    tmp_path, tables, workers
):
    src = tmp_path / "src"
    event = src / "20241217221040"
    inputs = [
        make_sac(event / f"XX.GL04.--.{ch}.20241217221040{suffix}.sac", channel=ch)
        for ch, suffix in [("BHE", ""), ("BHN", ".s01"), ("BHN", ".s02"), ("BHZ", "")]
    ]
    before = {p: p.read_bytes() for p in inputs}
    result = run(src, tmp_path / "out", tables, event=True, max_workers=workers)
    assert result.total == result.succeeded == 4
    assert result.failed == result.skipped == 0
    assert result.events_total == result.events_processed == 1
    for p in inputs:
        out = tmp_path / "out" / p.relative_to(src)
        raw = out.read_bytes()
        assert raw[632:] == before[p][632:]
        assert raw[280:432] == before[p][280:432]
        assert raw[436:632] == before[p][436:632]  # only lcalda may change
        for key in ("delta", "b", "e", "o", "a", "f", *[f"t{i}" for i in range(10)]):
            i = 4 * FLOATHDRS.index(key)
            assert raw[i : i + 4] == before[p][i : i + 4]
        h = SACTrace.read(str(out), headonly=True)
        assert h.kcmpnm in ("BHE", "BHN", "BHZ") and h.khole in (None, "")
        assert h.stla == 35 and h.stlo == 115 and h.stel == 2872
        assert h.evla == 30 and h.evlo == 110 and h.evdp == 10
        assert h.dist == pytest.approx(726.5581993, abs=0.0001)
        assert h.gcarc == pytest.approx(6.5330634, abs=0.00003)
        assert p.read_bytes() == before[p]


@pytest.mark.parametrize("version", [6, 7])
@pytest.mark.parametrize("byteorder", ["little", "big"])
@pytest.mark.parametrize("inplace", [False, True])
def test_header_only_updates_preserve_data_and_precision(
    tmp_path, tables, version, byteorder, inplace
):
    src = tmp_path / "src"
    p = make_sac(
        src / "XX" / "GL04" / "2024" / "arbitrary.sac",
        byteorder=byteorder,
        version=version,
    )
    before = p.read_bytes()
    result = run(src, None if inplace else tmp_path / "out", tables)
    assert result.succeeded == 1 and result.failed == 0
    out = p if inplace else tmp_path / "out" / p.relative_to(src)
    after = out.read_bytes()
    endian = "<" if byteorder == "little" else ">"
    assert len(after) == len(before)
    assert after[632 : 632 + 103 * 4] == before[632 : 632 + 103 * 4]
    assert after[280:432] == before[280:432]
    assert after[436:632] == before[436:632]
    assert struct.unpack_from(endian + "f", after, 4 * FLOATHDRS.index("stla"))[0] == 35
    assert (
        after[4 * FLOATHDRS.index("evla") : 4 * FLOATHDRS.index("evla") + 4]
        == before[4 * FLOATHDRS.index("evla") : 4 * FLOATHDRS.index("evla") + 4]
    )
    if version == 7:
        old = struct.unpack(endian + "22d", before[-176:])
        new = struct.unpack(endian + "22d", after[-176:])
        assert new[:18] == old[:18] and new[20:] == old[20:]
        assert new[18:20] == (115.0, 35.0)
    if not inplace:
        assert p.read_bytes() == before
    else:
        second = run(src, None, tables)
        assert second.succeeded == second.unchanged == 1
        assert p.read_bytes() == after


def test_daily_optional_values_and_time_fields_remain_untouched(tmp_path, tables):
    src = tmp_path / "src"
    p = make_sac(src / "day.sac")
    before = p.read_bytes()
    summary = run(src, tables=tables)
    assert summary.succeeded == 1 and summary.events_total == 0
    raw = p.read_bytes()
    for key in ("stdp", "o", "evla", "evlo", "mag", "evdp"):
        i = 4 * FLOATHDRS.index(key)
        assert raw[i : i + 4] == before[i : i + 4]


def test_existing_destination_is_skipped_or_atomically_replaced(tmp_path, tables):
    src = tmp_path / "src"
    p = make_sac(src / "day.sac")
    dest = tmp_path / "out"
    dest.mkdir()
    target = dest / p.name
    target.write_bytes(b"keep")
    result = run(src, dest, tables)
    assert result.skipped == result.output_conflicts == 1 and result.failed == 0
    assert target.read_bytes() == b"keep"
    assert run(src, dest, tables, overwrite=True).succeeded == 1
    assert target.read_bytes()[632:] == p.read_bytes()[632:]


def test_missing_event_is_a_visible_failure(tmp_path, tables):
    src = tmp_path / "src"
    p = make_sac(src / "20240101000000" / "x.sac")
    before = p.read_bytes()
    result = run(src, tables=tables, event=True)
    assert result.total == result.failed == result.events_skipped == 1
    assert result.has_issues and p.read_bytes() == before


def test_network_aware_matching_and_no_filename_guessing(tmp_path, tables):
    tables[0].write_text(
        "network,station,latitude,longitude\nYY,GL04,20,30\nXX,GL04,35,115\n"
    )
    src = tmp_path / "src"
    p = make_sac(src / "wrong.filename.sac")
    assert run(src, tables=tables).succeeded == 1
    assert SACTrace.read(str(p), headonly=True).stla == 35


@pytest.mark.parametrize(
    "damage", ["truncated", "extra", "bad_version", "missing_station"]
)
def test_bad_file_is_not_modified(tmp_path, tables, damage):
    src = tmp_path / "src"
    p = make_sac(src / "bad.sac")
    raw = bytearray(p.read_bytes())
    if damage == "truncated":
        raw = raw[:-4]
    elif damage == "extra":
        raw.extend(b"extra")
    elif damage == "bad_version":
        struct.pack_into("<i", raw, 280 + 4 * INTHDRS.index("nvhdr"), 42)
    else:
        raw[440:448] = b"-12345  "
    p.write_bytes(raw)
    before = p.read_bytes()
    result = run(src, tables=tables)
    assert result.failed == 1 and result.succeeded == 0
    assert p.read_bytes() == before


def test_recover_original_header_on_partial_write(tmp_path, tables):
    from seispy.waveform import _sac_header_io as io

    src = tmp_path / "src"
    p = make_sac(src / "day.sac", version=7)
    before = p.read_bytes()
    real = io._write_regions

    def fail(handle, regions):
        handle.seek(0)
        handle.write(regions[0][1][:200])
        handle.flush()
        raise OSError("simulated write failure")

    with patch.object(io, "_write_regions", side_effect=fail):
        result = run(src, tables=tables)
    assert result.failed == 1 and p.read_bytes() == before
    assert real is not None


def test_no_matching_files_does_not_report_empty_success(tmp_path, tables):
    src = tmp_path / "empty"
    src.mkdir()
    with pytest.raises(ValueError, match="No SAC files"):
        run(src, tables=tables)


@pytest.mark.parametrize("placement", ["same", "child", "parent"])
def test_output_tree_cannot_overlap_source(tmp_path, tables, placement):
    src = tmp_path / "src"
    make_sac(src / "day.sac")
    dest = {"same": src, "child": src / "out", "parent": tmp_path}[placement]
    with pytest.raises(ValueError, match="separate|outside|overlap"):
        run(src, dest, tables)


def test_invalid_metadata_is_rejected_before_any_update(tmp_path, tables):
    src = tmp_path / "src"
    p = make_sac(src / "day.sac")
    before = p.read_bytes()
    tables[0].write_text("station,latitude,longitude\nGL04,123,115\n")
    with pytest.raises(ValueError, match="latitude"):
        run(src, tables=tables)
    assert p.read_bytes() == before


@pytest.mark.parametrize("byteorder", ["little", "big"])
def test_event_v7_updates_both_coordinate_copies_only(tmp_path, tables, byteorder):
    src = tmp_path / "src"
    p = make_sac(src / "20241217221040" / "event.sac", version=7, byteorder=byteorder)
    before = p.read_bytes()
    assert run(src, tables=tables, event=True).succeeded == 1
    after = p.read_bytes()
    order = "<" if byteorder == "little" else ">"
    old = struct.unpack(order + "22d", before[-176:])
    new = struct.unpack(order + "22d", after[-176:])
    assert new[:16] == old[:16] and new[20:] == old[20:]
    assert new[16:20] == (110.0, 30.0, 115.0, 35.0)
    assert after[632:-176] == before[632:-176]


def test_copy_failure_preserves_existing_target_and_cleans_temporary(tmp_path, tables):
    from seispy.waveform import _sac_header_io as io

    src = tmp_path / "src"
    p = make_sac(src / "day.sac")
    before = p.read_bytes()
    dest = tmp_path / "out"
    dest.mkdir()
    target = dest / p.name
    target.write_bytes(b"existing")
    with patch.object(io, "_write_regions", side_effect=OSError("write failed")):
        result = run(src, dest, tables, overwrite=True)
    assert result.failed == 1 and target.read_bytes() == b"existing"
    assert p.read_bytes() == before and sorted(x.name for x in dest.iterdir()) == [
        target.name
    ]


def test_many_files_use_bounded_threads_and_reports(tmp_path, tables):
    src = tmp_path / "src"
    for i in range(130):
        make_sac(src / f"{i:03d}.sac")
    summary = module.format_sac_headers(src, stations_csv=tables[0], max_workers=4)
    assert summary.total == summary.succeeded == 130
    assert summary.report_path.is_file() and summary.log_path.is_file()
    again = run(src, tables=tables, max_workers=4)
    assert again.total == again.unchanged == 130  # logs never become inputs


def test_network_mismatch_and_ambiguous_table_do_not_guess(tmp_path, tables):
    src = tmp_path / "src"
    p = make_sac(src / "x.sac", network="YY")
    before = p.read_bytes()
    assert run(src, tables=tables).failed == 1
    assert p.read_bytes() == before
    tables[0].write_text(
        "network,station,latitude,longitude\n,GL04,20,30\nXX,GL04,35,115\n"
    )
    with pytest.raises(ValueError, match="ambiguous"):
        run(src, tables=tables)


def test_inplace_rejects_file_aliases_but_copy_accepts_symlink(tmp_path, tables):
    src = tmp_path / "src"
    src.mkdir()
    original = make_sac(tmp_path / "original.sac")
    (src / "link.sac").symlink_to(original)
    before = original.read_bytes()
    assert run(src, tables=tables).failed == 1
    assert original.read_bytes() == before
    assert run(src, tmp_path / "out", tables).succeeded == 1
    assert original.read_bytes() == before


def test_missing_optional_values_preserve_existing_metadata(tmp_path, tables):
    src = tmp_path / "src"
    p = make_sac(src / "day.sac")
    sac = SACTrace.read(str(p))
    sac.stel = 1234.0
    sac.stdp = 5.0
    sac.write(str(p))
    tables[0].write_text("station,latitude,longitude,elevation,depth\nGL04,35,115,,\n")
    assert run(src, tables=tables).succeeded == 1
    result = SACTrace.read(str(p), headonly=True)
    assert result.stel == 1234 and result.stdp == 5


def test_event_table_duplicate_second_is_rejected(tmp_path, tables):
    src = tmp_path / "src"
    p = make_sac(src / "20241217221040" / "event.sac")
    before = p.read_bytes()
    text = tables[1].read_text()
    tables[1].write_text(text + text.splitlines()[1] + "\n")
    with pytest.raises(ValueError, match="duplicate event"):
        run(src, tables=tables, event=True)
    assert p.read_bytes() == before


def test_direct_event_directory_and_nested_glob(tmp_path, tables):
    src = tmp_path / "20241217221040"
    make_sac(src / "nested" / "name.sac")
    assert run(src, tables=tables, event=True, pattern="**/*.sac").succeeded == 1


def test_recursive_glob_also_matches_source_root(tmp_path, tables):
    src = tmp_path / "src"
    make_sac(src / "x.sac")
    assert run(src, tables=tables, pattern="**/*.sac").succeeded == 1


def test_optional_nan_and_sac_undefined_preserve_values(tmp_path, tables):
    src = tmp_path / "src"
    p = make_sac(src / "day.sac")
    sac = SACTrace.read(str(p))
    sac.stel = 1234.0
    sac.stdp = 5.0
    sac.write(str(p))
    tables[0].write_text(
        "station,latitude,longitude,elevation,depth\nGL04,35,115,NaN,-12345\n"
    )
    assert run(src, tables=tables).succeeded == 1
    result = SACTrace.read(str(p), headonly=True)
    assert result.stel == 1234 and result.stdp == 5
