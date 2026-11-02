import pytest

from seispy.event.external import _integer_second_catalog, cut_events_binary


def test_binary_catalog_floors_window_and_preserves_original_catalog(tmp_path):
    source = tmp_path / "events.cat"
    text = "2024/01/01,23:59:59.999999 105 35 6 12\n"
    source.write_text(text)
    output = tmp_path / "temporary.cat"
    origins = _integer_second_catalog(source, output)
    assert output.read_text() == "2024/01/01,23:59:59.000000 105 35 6 12\n"
    assert next(iter(origins.values())).microsecond == 999999
    assert source.read_text() == text


@pytest.mark.parametrize(
    "scenario",
    [
        "binary_catalog_rejects_integer_second_directory_collision",
        "binary_cutter_rejects_output_inside_network_directory",
    ],
)
def test_external_output_safety(tmp_path, scenario):
    if scenario == "binary_catalog_rejects_integer_second_directory_collision":
        source = tmp_path / "events.cat"
        source.write_text("2024/01/01,00:00:00.1\n2024/01/01,00:00:00.2\n")
        with pytest.raises(ValueError, match="multiple events"):
            _integer_second_catalog(source, tmp_path / "temporary.cat")
    elif scenario == "binary_cutter_rejects_output_inside_network_directory":
        network_dir = tmp_path / "NZ"
        network_dir.mkdir()

        with pytest.raises(ValueError, match="separate directory trees"):
            cut_events_binary(
                network_dir,
                network_dir / "events",
                tmp_path / "events.cat",
            )
