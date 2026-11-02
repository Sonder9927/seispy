"""MiniSEED-to-SAC conversion contracts."""

import pytest

from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

module = import_module("seispy.waveform.conversion")


class _Time:
    year = 2026
    julday = 8

    def strftime(self, value):
        return "010203"


class _Trace:
    stats = SimpleNamespace(
        network="NZ",
        station="AAA",
        location="",
        channel="BHZ",
        starttime=_Time(),
    )

    def write(self, filename, format):
        assert format == "SAC"
        Path(filename).write_bytes(b"sac")


class _Stream(list):
    def get_gaps(self):
        return []

    def merge(self, **kwargs):
        return self


@pytest.mark.parametrize(
    "scenario",
    [
        "conversion_writes_sac_and_preserves_source",
        "conflict_does_not_overwrite_output",
        "partial_conversion_is_rolled_back",
    ],
)
def test_conversion_and_source_protection(tmp_path, scenario):
    source = tmp_path / "input.miniseed"
    source.write_bytes(b"mseed")
    if scenario == "conversion_writes_sac_and_preserves_source":
        with patch.object(module.obspy, "read", return_value=_Stream([_Trace()])):
            result = module._convert_file(source, tmp_path / "output", 1)
        assert result.succeeded == 1
        assert result.traces_written == 1
        assert source.read_bytes() == b"mseed"
        destination = (
            tmp_path
            / "output"
            / "NZ"
            / "AAA"
            / "2026"
            / "NZ.AAA.--.BHZ.2026.008.010203.sac"
        )
        assert destination.read_bytes() == b"sac"
    elif scenario == "conflict_does_not_overwrite_output":
        destination = module._trace_destination(_Trace(), tmp_path / "output")
        destination.parent.mkdir(parents=True)
        destination.write_bytes(b"existing")
        with patch.object(module.obspy, "read", return_value=_Stream([_Trace()])):
            result = module._convert_file(source, tmp_path / "output", 1)
        assert result.failed == 1
        assert result.conflicts == 1
        assert destination.read_bytes() == b"existing"
    elif scenario == "partial_conversion_is_rolled_back":
        second = _Trace()
        second.stats = SimpleNamespace(**vars(_Trace.stats))
        second.stats.channel = "BHN"
        second.write = lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("bad")
        )
        with patch.object(
            module.obspy, "read", return_value=_Stream([_Trace(), second])
        ):
            result = module._convert_file(source, tmp_path / "output", 1)
        assert result.failed == 1
        assert not list((tmp_path / "output").rglob("*.sac"))
