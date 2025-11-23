import re
from textwrap import dedent

import pytest
from click.testing import CliRunner

from sdrf_pipelines.parse_sdrf import cli

from .helpers import run_and_check_status_code

# Regex matchin Semantic Versioning 2.0 version numbers. Adapted from https://semver.org/
SEMVER_REGEX = dedent(
    r"""
    (?P<major>0|[1-9]\d*)\.
    (?P<minor>0|[1-9]\d*)\.
    (?P<patch>0|[1-9]\d*)
    (?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)
    (?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?
    (?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$
    """
).replace("\n", "")


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])

    assert result.exit_code == 0
    regex = f"sdrf_pipelines {SEMVER_REGEX}\n"
    match = re.fullmatch(regex, result.output)
    assert match, f"{repr(result.output)} does not match {repr(regex)}"


def test_help():
    result = run_and_check_status_code(cli, ["--help"])
    match = re.search(r"validate-sdrf\s+Command to validate the sdrf file", result.output)
    assert match


def test_validate_srdf_errors_on_bad_file(shared_datadir, on_tmpdir):
    """
    :return:
    """
    test_sdrf = shared_datadir / "erroneous/PXD000288/PXD000288.sdrf.tsv"
    result = run_and_check_status_code(cli, ["validate-sdrf", "--sdrf_file", str(test_sdrf)], 1)

    expected_error = (
        "The following columns are mandatory and not present in the SDRF: comment[technical replicate] -- ERROR"
    )
    assert "ERROR" in result.output.upper(), result.output


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
    result = run_and_check_status_code(cli, ["validate-sdrf", "--sdrf_file", str(test_sdrf)], 1)

    expected_errors = [
        (
            "Make sure your SDRF have a sample characteristics or data comment 'concentration of' for "
            "your factor value column 'factor value[concentration of]' -- ERROR"
        ),
        (
            "Factor 'factor value[compound]' and column 'characteristics[compound]' do not have the same "
            "values for the following rows: [11, 20] -- ERROR"
        ),
    ]
    # for expected_error in expected_errors:
    #     assert expected_error in result.output, result.output


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
