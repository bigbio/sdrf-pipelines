import pytest

from sdrf_pipelines.openms.openms import get_openms_file_name

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
