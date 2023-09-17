from pathlib import Path

import pytest

from sdrf_pipelines.parse_sdrf import cli

from .helpers import compare_files
from .helpers import run_and_check_status_code


def _check_output_existance(out_dir: Path, two_files=True, min_num_samples=6):
    min_lines = min_num_samples + 1
    files_in_dir = list(out_dir.iterdir())
    if two_files:
        assert (out_dir / "openms.tsv").exists(), files_in_dir
        assert (out_dir / "experimental_design.tsv").exists(), files_in_dir
        # Check that the files have at least 3 lines
        with open(out_dir / "openms.tsv", "r") as f:
            assert len(f.readlines()) >= min_lines, "openms.tsv is empty"
        with open(out_dir / "experimental_design.tsv", "r") as f:
            assert len(f.readlines()) > min_lines, "experimental_design.tsv is empty"

    else:
        assert (out_dir / "openms.tsv").exists(), files_in_dir
        # Check that the files have at least 3 lines
        with open(out_dir / "openms.tsv", "r") as f:
            assert len(f.readlines()) > min_lines, "openms.tsv is empty"


def _check_output_file_extensions(out_dir: Path, expected_extension):
    with open(out_dir / "openms.tsv", "r") as f:
        content = f.readlines()

    content = content[1:]
    files = [line.split("\t")[1] for line in content]
    # FOR SOME REASON, this does not matter ...
    # assert all([file.endswith(expected_extension) for file in files]), str(files) + "\n" + str(content)

    with open(out_dir / "experimental_design.tsv", "r") as f:
        content = f.readlines()

    content = content[1 : content.index("\n")]
    files = [line.split("\t")[2] for line in content]
    assert all([file.endswith(expected_extension) for file in files]), str(files) + "\n" + str(content)


def test_convert_openms(shared_datadir, on_tmpdir):
    """
    :return:
    """

    # why does this work? This file does not pass the validation ...
    test_sdrf = shared_datadir / "PXD001819/PXD001819.sdrf.tsv"
    result = run_and_check_status_code(cli, ["convert-openms", "-t2", "-s", test_sdrf])
    assert "ERROR" not in result.output.upper(), result.output
    _check_output_existance(on_tmpdir)


@pytest.mark.parametrize("change_extension", [True, False])
def test_convert_openms_file_extensions(change_extension, shared_datadir, on_tmpdir):
    """
    :return:
    """
    test_sdrf = shared_datadir / "PXD001819/PXD001819.sdrf.tsv"
    cmd = [
        "convert-openms",
        "-t2",
        "-s",
        test_sdrf,
    ]
    if change_extension:
        cmd.extend(["--extension_convert", "raw:mzML"])
    result = run_and_check_status_code(cli, cmd)
    assert "ERROR" not in result.output.upper(), result.output
    _check_output_existance(on_tmpdir)
    if change_extension:
        _check_output_file_extensions(on_tmpdir, ".mzML")
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
    else:
        _check_output_file_extensions(on_tmpdir, ".raw")


def test_convert_openms_file_extensions_dotd(shared_datadir, on_tmpdir):
    test_sdrf = shared_datadir / "generic/quantms_dia_dotd_sample.sdrf"
    cmd = ["convert-openms", "-t2", "-s", test_sdrf, "--extension_convert", ".d.zip:.d"]
    result = run_and_check_status_code(cli, cmd)
    _check_output_existance(on_tmpdir, min_num_samples=1)
    _check_output_file_extensions(on_tmpdir, ".d")


@pytest.mark.parametrize("convertsion_flag", [True, False])
def test_nocovnersion_openms_file_extensions_dotd(shared_datadir, on_tmpdir, convertsion_flag):
    test_sdrf = shared_datadir / "generic/quantms_dia_dotd_sample_converted.sdrf"
    cmd = ["convert-openms", "-t2", "-s", test_sdrf]
    if convertsion_flag:
        cmd.extend(["--extension_convert", "raw:mzML"])
    result = run_and_check_status_code(cli, cmd)
    _check_output_existance(on_tmpdir, min_num_samples=1)
    _check_output_file_extensions(on_tmpdir, ".d")


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
