import pytest

from seispy.event.external import cut_events_binary


def test_binary_cutter_rejects_output_inside_network_directory(tmp_path):
    network_dir = tmp_path / "NZ"
    network_dir.mkdir()

    with pytest.raises(ValueError, match="separate directory trees"):
        cut_events_binary(
            network_dir,
            network_dir / "events",
            tmp_path / "events.cat",
        )
