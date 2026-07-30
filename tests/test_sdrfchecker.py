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


# ``comment[cleavage agent details]`` is required by ``ms-proteomics`` but not by
# ``cell-lines`` (the two templates are siblings that both extend ``sample-metadata``).
_SDRF_MISSING_MS_PROTEOMICS_COLUMN = (
    "\t".join(
        [
            "source name",
            "characteristics[organism]",
            "characteristics[cell line]",
            "characteristics[disease]",
            "characteristics[cellosaurus accession]",
            "assay name",
            "comment[instrument]",
            "comment[label]",
            "comment[fraction identifier]",
            "comment[proteomics data acquisition method]",
        ]
    )
    + "\n"
    + "\t".join(
        [
            "sample 1",
            "Homo sapiens",
            "HeLa",
            "cervical cancer",
            "CVCL_0030",
            "run 1",
            "NT=Orbitrap Fusion;AC=MS:1002416",
            "label free sample",
            "1",
            "NT=Data-Dependent Acquisition;AC=NCIT:C161785",
        ]
    )
    + "\n"
)

_MS_PROTEOMICS_ONLY_COLUMN = "Required column 'comment[cleavage agent details]'"


def test_validate_sdrf_honors_multiple_templates(tmp_path):
    """Regression test for issue #312.

    Passing several ``--template`` flags used to silently drop all but the last value,
    so a rule that only belongs to an earlier template was never enforced. With the
    fix, the SDRF is validated against the union of every ``--template`` given, so the
    ms-proteomics-only required column is reported even though it is not the last
    template on the command line.
    """
    sdrf_file = tmp_path / "cell_lines_missing_ms_proteomics_column.sdrf.tsv"
    sdrf_file.write_text(_SDRF_MISSING_MS_PROTEOMICS_COLUMN)

    runner = CliRunner()

    # Only the "last" template used to win: cell-lines does not require the column,
    # so on its own it must not report it.
    single = runner.invoke(
        cli,
        ["validate-sdrf", "--sdrf_file", str(sdrf_file), "-t", "cell-lines", "--skip-ontology"],
        catch_exceptions=False,
        standalone_mode=False,
    )
    assert _MS_PROTEOMICS_ONLY_COLUMN not in single.output, single.output

    # With both templates, the ms-proteomics rule must be enforced even though
    # cell-lines is given last.
    multi = runner.invoke(
        cli,
        [
            "validate-sdrf",
            "--sdrf_file",
            str(sdrf_file),
            "-t",
            "ms-proteomics",
            "-t",
            "cell-lines",
            "--skip-ontology",
        ],
        catch_exceptions=False,
        standalone_mode=False,
    )
    assert _MS_PROTEOMICS_ONLY_COLUMN in multi.output, multi.output
