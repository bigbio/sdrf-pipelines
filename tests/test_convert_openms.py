from pathlib import Path

import pytest
from click.testing import CliRunner

from sdrf_pipelines.parse_sdrf import cli

from .helpers import compare_files
from .helpers import run_and_check_status_code


def _check_output_existance(out_dir: Path, two_files=True):
    files_in_dir = list(out_dir.iterdir())
    if two_files:
        assert (out_dir / "openms.tsv").exists(), files_in_dir
        assert (out_dir / "experimental_design.tsv").exists(), files_in_dir
        # Check that the files have at least 3 lines
        with open(out_dir / "openms.tsv", "r") as f:
            assert len(f.readlines()) > 2, "openms.tsv is empty"
        with open(out_dir / "experimental_design.tsv", "r") as f:
            assert len(f.readlines()) > 2, "experimental_design.tsv is empty"

    else:
        assert (out_dir / "openms.tsv").exists(), files_in_dir
        # Check that the files have at least 3 lines
        with open(out_dir / "openms.tsv", "r") as f:
            assert len(f.readlines()) > 2, "openms.tsv is empty"


def test_convert_openms(shared_datadir, on_tmpdir):
    """
    :return:
    """

    # why does this work? This file does not pass the validation ...
    test_sdrf = shared_datadir / "PXD001819/PXD001819.sdrf.tsv"
    result = run_and_check_status_code(cli, ["convert-openms", "-t2", "-s", test_sdrf])
    assert "ERROR" not in result.output.upper(), result.output
    _check_output_existance(on_tmpdir)


def test_convert_openms_file_extensions(shared_datadir, on_tmpdir):
    """
    :return:
    """
    test_sdrf = shared_datadir / "PXD001819/PXD001819.sdrf.tsv"
    result = run_and_check_status_code(
        cli, ["convert-openms", "-t2", "-s", test_sdrf, "--extension_convert", "raw:mzML"]
    )
    assert "ERROR" not in result.output.upper(), result.output
    _check_output_existance(on_tmpdir)
    diff1 = compare_files(
        shared_datadir / "PXD001819/expected_experimental_design.tsv",
        on_tmpdir / "experimental_design.tsv",
    )
    diff2 = compare_files(
        shared_datadir / "PXD001819/expected_openms.tsv",
        on_tmpdir / "openms.tsv",
    )
    assert len(diff1) == 0, diff1
    assert len(diff2) == 0, diff2


reference_samples = [
    "reference/PXD002137/PXD002137.sdrf.tsv",
    "reference/PDC000126/PDC000126.sdrf.tsv",
    "reference/PXD008934/PXD008934.sdrf.tsv",
    "reference/PXD006482/PXD006482.sdrf.tsv",
    "reference/PXD004684/PXD004684.sdrf.tsv",
]


@pytest.mark.parametrize("file_subpath", reference_samples)
@pytest.mark.parametrize("two_files", [True, False])
def test_on_reference_sdrf(file_subpath, two_files, shared_datadir, on_tmpdir):
    """
    :return:
    """
    test_sdrf = shared_datadir / file_subpath
    cmd = ["convert-openms", "-t2"]
    if two_files:
        cmd.append("-t2")

    # This fails, why is the --sdrf_file option not consistent within the
    # sub-commands ?. Since it works on `validate-sdrf`, it should be the same
    # for `convert-openms`.
    # result = run_and_check_status_code(cli, cmd + ["--sdrf_file", str(test_sdrf)])
    result = run_and_check_status_code(cli, cmd + ["-s", str(test_sdrf)])
    assert "ERROR" not in result.output.upper(), result.output
    _check_output_existance(on_tmpdir, two_files=two_files)
