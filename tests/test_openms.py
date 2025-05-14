import pytest

from sdrf_pipelines.openms.openms import (
    TMT_PLEXES,
    OpenMS,
    get_openms_file_name,
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
]


@pytest.mark.parametrize("input_str,expected_tol,expected_unit", test_tol_string)
def test_parse_tolerence(input_str, expected_tol, expected_unit):
    assert parse_tolerance(input_str) == (expected_tol, expected_unit)


@pytest.mark.parametrize("plex_name", TMT_PLEXES)
def test_tmt_label_inference_full_plexes(plex_name):
    assert plex_name == infer_tmtplex(TMT_PLEXES[plex_name])


@pytest.mark.parametrize("plex_name", TMT_PLEXES)
def test_tmt_label_inference_from_incomplete_plexes(plex_name):
    labels = {**TMT_PLEXES[plex_name]}
    assert plex_name == infer_tmtplex(TMT_PLEXES[plex_name])
