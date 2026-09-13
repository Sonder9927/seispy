from unittest.mock import patch

import numpy as np
import pytest
from obspy import Stream, Trace, UTCDateTime, read
from obspy.io.mseed.util import get_record_information

from seispy.waveform import mseed_recovery


def _multi_record_mseed(path):
    trace = Trace(np.arange(20_000, dtype=np.int32))
    trace.stats.network = "NZ"
    trace.stats.station = "ABAZ"
    trace.stats.location = "11"
    trace.stats.channel = "HHE"
    trace.stats.starttime = UTCDateTime("2023-08-01")
    trace.stats.sampling_rate = 100.0
    Stream([trace]).write(path, format="MSEED", reclen=4096, encoding="STEIM1")


def test_filter_valid_records_drops_only_the_corrupt_record(tmp_path):
    source = tmp_path / "raw.mseed"
    destination = tmp_path / "recovered.mseed"
    _multi_record_mseed(source)
    info = get_record_information(source)
    record_length = info["record_length"]
    record_count = info["number_of_records"]
    verdicts = [True] * record_count
    verdicts[1] = False

    with patch.object(mseed_recovery, "_record_is_valid", side_effect=verdicts):
        result = mseed_recovery.filter_valid_mseed_records(
            source,
            destination,
            network="NZ",
            station="ABAZ",
            location="11",
            channel="HHE",
            sample_rate=100.0,
        )

    assert result.total_records == record_count
    assert result.valid_records == record_count - 1
    assert result.discarded_records == 1
    assert destination.stat().st_size == source.stat().st_size - record_length
    recovered = read(destination)
    assert len(recovered) == 2
    assert recovered.get_gaps()


def test_filter_valid_records_rejects_wrong_identity(tmp_path):
    source = tmp_path / "raw.mseed"
    destination = tmp_path / "recovered.mseed"
    _multi_record_mseed(source)

    with pytest.raises(ValueError, match="all miniSEED records"):
        mseed_recovery.filter_valid_mseed_records(
            source,
            destination,
            network="NZ",
            station="WRONG",
            location="11",
            channel="HHE",
            sample_rate=100.0,
        )

    assert not destination.exists()
