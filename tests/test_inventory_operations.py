import pytest

from obspy import UTCDateTime
from obspy.core.inventory import Channel, Inventory, Network, Site, Station

from seispy.inventory import (
    combine_inventories,
    select_inventory,
    shift_channel_starttime,
    write_inventory,
)


def _inventory(network_code="AA", station_code="STA", channel_code="BHZ"):
    channel = Channel(
        channel_code,
        "10",
        latitude=1,
        longitude=2,
        elevation=3,
        depth=0,
        azimuth=0,
        dip=-90,
        sample_rate=1,
        start_date=UTCDateTime("2020-01-01"),
    )
    station = Station(
        station_code,
        latitude=1,
        longitude=2,
        elevation=3,
        site=Site("test"),
        channels=[channel],
    )
    return Inventory([Network(network_code, stations=[station])], source="test")


@pytest.mark.parametrize(
    "scenario",
    [
        "select_inventory_supports_multiple_networks_without_mutation",
        "combine_and_shift_do_not_mutate_inputs",
    ],
)
def test_in_memory_operations(scenario):
    if scenario == "select_inventory_supports_multiple_networks_without_mutation":
        inventory = _inventory("AA", "ONE", "BHZ") + _inventory("BB", "TWO", "BHN")
        selected = select_inventory(inventory, networks=["BB"], channels=["BHN"])
        assert selected.get_contents()["networks"] == ["BB"]
        assert selected.get_contents()["channels"] == ["BB.TWO.10.BHN"]
        assert len(inventory.networks) == 2
    elif scenario == "combine_and_shift_do_not_mutate_inputs":
        first = _inventory("AA")
        second = _inventory("BB")
        combined = combine_inventories([first, second], channel_starttime="2025-01-01")
        assert len(combined.networks) == 2
        assert combined[0][0][0].start_date == UTCDateTime("2025-01-01")
        assert first[0][0][0].start_date == UTCDateTime("2020-01-01")
        shifted = shift_channel_starttime(first, (2024, 1, 1))
        assert shifted[0][0][0].start_date == UTCDateTime("2024-01-01")


@pytest.mark.parametrize(
    "scenario",
    ["write_inventory_is_safe_by_default", "select_inventory_can_write_output"],
)
def test_inventory_exports(tmp_path, scenario):
    if scenario == "write_inventory_is_safe_by_default":
        destination = tmp_path / "inventory.xml"
        write_inventory(_inventory(), destination)
        original = destination.read_bytes()
        try:
            write_inventory(_inventory("BB"), destination)
        except FileExistsError:
            pass
        else:
            raise AssertionError("FileExistsError was not raised")
        assert destination.read_bytes() == original
    elif scenario == "select_inventory_can_write_output":
        destination = tmp_path / "selected.xml"
        result = select_inventory(
            _inventory(), stations=["STA"], output_file=destination
        )
        assert len(result.networks) == 1
        assert destination.exists()
