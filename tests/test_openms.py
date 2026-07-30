import pytest

from sdrf_pipelines.converters.openms import CHANNEL_MAP
from sdrf_pipelines.converters.openms.constants import (
    ITRAQ_4PLEX,
    ITRAQ_8PLEX,
    SILAC_2PLEX,
    SILAC_3PLEX,
    TMT_PLEXES,
)
from sdrf_pipelines.converters.openms.utils import (
    get_openms_file_name,
    infer_itraqplex,
    infer_tmtplex,
    parse_tolerance,
)

test_functions = [
    ("file.raw", "file.mzML", "raw:mzML"),
    ("file.mzML", "file.mzML", "mzML:mzML"),
    ("file.mzML", "file.mzml", "mzML:mzml"),
    ("file.mzml", "file.mzML", "mzml:mzML"),
    ("file.d", "file.mzML", "d:mzML"),
    ("file.d", "file.d", "d:d"),
    ("file.gz", "file.d", "gz:d"),
    ("file.d.tar", "file.d", "d.tar:d"),
    ("file.d.zip", "file.d", ".zip:"),
]


@pytest.mark.parametrize("input_file,expected_file,extension", test_functions)
def test_get_openms_file_name(input_file, expected_file, extension):
    assert get_openms_file_name(input_file, extension) == expected_file


test_tol_string = [
    ("10ppm", "10", "ppm"),
    ("10 ppm", "10", "ppm"),
    ("10 ppm ", "10", "ppm"),
    ("10ppm", "10", "ppm"),
    ("10ppmmm", "10", "ppm"),  # good?
    ("30 Da", "30", "Da"),
    ("40 Da", "40", "Da"),
    ("40Da", "40", "Da"),
    ("50da", "50", "Da"),
    ("1daaaaaa", "1", "Da"),  # good?
    ("2mmu", "0.002", "Da"),
]


@pytest.mark.parametrize("input_str,expected_tol,expected_unit", test_tol_string)
def test_parse_tolerence(input_str, expected_tol, expected_unit):
    assert parse_tolerance(input_str) == (expected_tol, expected_unit)


@pytest.mark.parametrize("plex_name", TMT_PLEXES)
def test_tmt_label_inference_full_plexes(plex_name):
    assert plex_name == infer_tmtplex(TMT_PLEXES[plex_name])


@pytest.mark.parametrize("plex_name", TMT_PLEXES)
def test_tmt_label_inference_from_incomplete_plexes(plex_name):
    assert plex_name == infer_tmtplex(TMT_PLEXES[plex_name])


@pytest.mark.parametrize(
    ("plex_name", "channel_count"),
    [
        ("tmt6plex", 6),
        ("tmt10plex", 10),
        ("tmt11plex", 11),
        ("tmt16plex", 16),
        ("tmt18plex", 18),
        ("tmt32plex", 32),
        ("tmt35plex", 35),
        ("itraq4plex", 4),
        ("itraq8plex", 8),
        ("silac2plex", 2),
        ("silac3plex", 3),
        ("lfq", 1),
    ],
)
def test_channel_map_uses_contiguous_one_based_ordinals(plex_name, channel_count):
    assert sorted(CHANNEL_MAP[plex_name].values()) == list(range(1, channel_count + 1))


def test_channel_map_uses_canonical_labels():
    assert CHANNEL_MAP["tmt10plex"]["TMT131"] == 10
    assert CHANNEL_MAP["itraq4plex"] == {
        "ITRAQ114": 1,
        "ITRAQ115": 2,
        "ITRAQ116": 3,
        "ITRAQ117": 4,
    }
    assert CHANNEL_MAP["itraq8plex"]["ITRAQ121"] == 8
    assert CHANNEL_MAP["silac3plex"] == {
        "SILAC light": 1,
        "SILAC medium": 2,
        "SILAC heavy": 3,
    }
    assert CHANNEL_MAP["lfq"] == {"LFQ": 1}


def test_tmt32_and_tmt35_follow_openms_channel_order():
    assert CHANNEL_MAP["tmt32plex"]["TMT127D"] == 4
    assert CHANNEL_MAP["tmt32plex"]["TMT135ND"] == 32
    assert CHANNEL_MAP["tmt35plex"]["TMT134C"] == 30
    assert CHANNEL_MAP["tmt35plex"]["TMT135CD"] == 35

    tmt35_only = {"TMT134C", "TMT135N", "TMT135CD"}
    tmt32_labels = [label for label, _ in sorted(CHANNEL_MAP["tmt32plex"].items(), key=lambda item: item[1])]
    tmt35_labels = [label for label, _ in sorted(CHANNEL_MAP["tmt35plex"].items(), key=lambda item: item[1])]
    assert tmt32_labels == [label for label in tmt35_labels if label not in tmt35_only]


def test_compatibility_maps_are_derived_from_canonical_channel_map():
    for plex_name, label_to_channel_id in TMT_PLEXES.items():
        assert label_to_channel_id == CHANNEL_MAP[plex_name]

    assert ITRAQ_4PLEX == {label.lower(): channel_id for label, channel_id in CHANNEL_MAP["itraq4plex"].items()}
    assert ITRAQ_8PLEX == {label.lower(): channel_id for label, channel_id in CHANNEL_MAP["itraq8plex"].items()}
    assert SILAC_2PLEX == {label.lower(): channel_id for label, channel_id in CHANNEL_MAP["silac2plex"].items()}
    assert SILAC_3PLEX == {label.lower(): channel_id for label, channel_id in CHANNEL_MAP["silac3plex"].items()}


@pytest.mark.parametrize("plex_name", ["itraq4plex", "itraq8plex"])
def test_itraq_label_inference(plex_name):
    assert infer_itraqplex(CHANNEL_MAP[plex_name]) == plex_name


def test_extended_tmt_label_inference():
    assert infer_tmtplex({"TMT127D"}) == "tmt32plex"
    assert infer_tmtplex({"TMT135CD"}) == "tmt35plex"
