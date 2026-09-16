import pandas as pd
import pytest

from seispy.event.catalog import load_events, load_stations


def test_load_events_preserves_subsecond_origin_time(tmp_path):
    catalog = tmp_path / "events.csv"
    pd.DataFrame(
        [
            {
                "time": "2025-01-02T03:04:05.678Z",
                "latitude": 1,
                "longitude": 2,
                "depth": 3,
                "mag": 4,
            }
        ]
    ).to_csv(catalog, index=False)

    event = load_events(catalog, 10)[0]

    assert event["start"].microsecond == 678_000


def test_station_csv_requires_network_for_multiple_networks(tmp_path):
    stations = tmp_path / "stations.csv"
    pd.DataFrame([{"station": "WEL"}]).to_csv(stations, index=False)

    with pytest.raises(ValueError, match="multiple networks"):
        load_stations({("NZ", "WEL"), ("IU", "WEL")}, stations)


def test_station_csv_may_omit_network_for_one_network(tmp_path):
    stations = tmp_path / "stations.csv"
    pd.DataFrame([{"station": "WEL", "latitude": -41.3}]).to_csv(stations, index=False)

    loaded = load_stations({("NZ", "WEL")}, stations)

    assert loaded[0]["network"] == "NZ"
    assert loaded[0]["latitude"] == -41.3
