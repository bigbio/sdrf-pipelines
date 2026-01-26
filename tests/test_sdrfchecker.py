import re

import pytest
from click.testing import CliRunner
from packaging.version import InvalidVersion, Version

from sdrf_pipelines.parse_sdrf import cli

from .helpers import run_and_check_status_code


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])

    assert result.exit_code == 0
    # Extract version string from output (format: "sdrf_pipelines X.Y.Z+local123\n")
    output_parts = result.output.strip().split()
    assert len(output_parts) == 2, f"Unexpected output format: {repr(result.output)}"
    assert output_parts[0] == "sdrf_pipelines"
    
    version_str = output_parts[1]
    # Validate using packaging.version which supports PEP 440 including local version identifiers
    try:
        Version(version_str)
    except InvalidVersion:
        pytest.fail(f"Invalid version string: {repr(version_str)}")


def test_help():
    result = run_and_check_status_code(cli, ["--help"])
    match = re.search(r"validate-sdrf\s+Command to validate the sdrf file", result.output)
    assert match


def test_validate_srdf_errors_on_bad_file(shared_datadir, on_tmpdir):
    """
    :return:
    """
    test_sdrf = shared_datadir / "erroneous/PXD000288/PXD000288.sdrf.tsv"
    run_and_check_status_code(cli, ["validate-sdrf", "--sdrf_file", str(test_sdrf)], 1)


def test_validate_srdf_fails_on_bad_file2(shared_datadir, on_tmpdir):
    """
    :return:
    """
    test_sdrf = shared_datadir / "PXD001819/PXD001819.sdrf.tsv"
    result = run_and_check_status_code(cli, ["validate-sdrf", "--sdrf_file", str(test_sdrf)], 1)

    expected_error = "Required column 'characteristics[biological replicate]'"
    assert expected_error in result.output, result.output


def test_validate_srdf_fails_on_bad_file3(shared_datadir, on_tmpdir):
    """
    :return:
    """
    test_sdrf = shared_datadir / "erroneous/example.sdrf.tsv"
    run_and_check_status_code(cli, ["validate-sdrf", "--sdrf_file", str(test_sdrf)], 1)


reference_samples = [
    "reference/PXD002137/PXD002137.sdrf.tsv",
    "reference/PDC000126/PDC000126.sdrf.tsv",
    "reference/PXD008934/PXD008934.sdrf.tsv",
    "reference/PXD006482/PXD006482.sdrf.tsv",
    "reference/PXD004684/PXD004684.sdrf.tsv",
    "reference/PXD001474/PXD001474.sdrf.tsv",
]


@pytest.mark.parametrize("file_subpath", reference_samples)
def test_on_reference_sdrf(file_subpath, shared_datadir, on_tmpdir):
    """
    :return:
    """
    test_sdrf = shared_datadir / file_subpath
    file_path = str(test_sdrf)
    result = run_and_check_status_code(cli, ["validate-sdrf", "--sdrf_file", file_path])
    assert (
        "There were validation errors." in result.output
        or "Everything seems to be fine. Well done." in result.output
        or "Most seems to be fine. There were only warnings." in result.output
    )
