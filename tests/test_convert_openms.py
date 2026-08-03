from pathlib import Path

import pandas as pd
import pytest

from sdrf_pipelines.parse_sdrf import cli

from .helpers import compare_files, run_and_check_status_code


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


def test_convert_openms_without_file_uri(shared_datadir, on_tmpdir):
    """Regression test for issue #313.

    ``comment[file uri]`` is optional in SDRF-Proteomics. ``convert-openms`` used to
    read it unconditionally and crash with ``KeyError: 'comment[file uri]'`` on SDRFs
    that do not have that column. Ensure conversion succeeds and the URI falls back to
    the (required) data file name.
    """
    src_sdrf = shared_datadir / "PXD001819/PXD001819.sdrf.tsv"
    df = pd.read_csv(src_sdrf, sep="\t", dtype=str)
    assert "comment[file uri]" in df.columns, "fixture expected to have comment[file uri]"
    df = df.drop(columns=["comment[file uri]"])

    # Write the modified SDRF to the isolated tmp dir, not shared_datadir (which
    # pytest-datadir shares across the session).
    test_sdrf = on_tmpdir / "PXD001819.no_file_uri.sdrf.tsv"
    df.to_csv(test_sdrf, sep="\t", index=False)

    result = run_and_check_status_code(cli, ["convert-openms", "-t2", "-s", test_sdrf])
    assert "ERROR" not in result.output.upper(), result.output
    _check_output_existance(on_tmpdir)

    # URI (column 0) must fall back to the data file name (column 1).
    with open(on_tmpdir / "openms.tsv", "r") as f:
        rows = [line.rstrip("\n").split("\t") for line in f.readlines()[1:]]
    assert rows, "openms.tsv has no data rows"
    for row in rows:
        assert row[0] == row[1], f"URI did not fall back to data file name: {row[0]!r} != {row[1]!r}"


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
    _ = run_and_check_status_code(cli, cmd)
    _check_output_existance(on_tmpdir, min_num_samples=1)
    _check_output_file_extensions(on_tmpdir, ".d")


@pytest.mark.parametrize("convertsion_flag", [True, False])
def test_nocovnersion_openms_file_extensions_dotd(shared_datadir, on_tmpdir, convertsion_flag):
    test_sdrf = shared_datadir / "generic/quantms_dia_dotd_sample_converted.sdrf"
    cmd = ["convert-openms", "-t2", "-s", test_sdrf]
    if convertsion_flag:
        cmd.extend(["--extension_convert", "raw:mzML"])
    _ = run_and_check_status_code(cli, cmd)
    _check_output_existance(on_tmpdir, min_num_samples=1)
    _check_output_file_extensions(on_tmpdir, ".d")


reference_samples = [
    "reference/PXD002137/PXD002137.sdrf.tsv",
    "reference/PDC000126/PDC000126.sdrf.tsv",
    "reference/PXD008934/PXD008934.sdrf.tsv",
    "reference/PXD006482/PXD006482.sdrf.tsv",
    "reference/PXD004684/PXD004684.sdrf.tsv",
    "reference/MSV000079033/MSV000079033-Blood-Plasma-iTRAQ.sdrf.tsv",
    "reference/MSV000079033/MSV000079033-Blood-Plasma-TMT10.sdrf.tsv",
    "reference/PDC000113/PDC000113.sdrf.tsv",
    "reference/PDC000180/PDC000180.sdrf.tsv",
    "reference/PXD000612/PXD000612.sdrf.tsv",
    "reference/PXD022661/PXD022661.sdrf.tsv",
    "reference/PXD027125/PXD027125.sdrf.tsv",
    "reference/PXD030304/PXD030304.sdrf.tsv",
    "reference/PXD030598/PXD030598.sdrf.tsv",
    "reference/PXD038526/PXD038526.sdrf.tsv",
    "reference/PXD034244/PXD034244.sdrf.tsv",
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


# Regression tests for issue #320: source names that merely *end* in the same 'sample N' must not
# collapse into one OpenMS Sample / MSstats BioReplicate. Only a name that is exactly 'sample N'
# takes the numeric shortcut; every other name gets a distinct per-name id.
class TestSampleIdTrackerSuffixCollapse:
    """Regression tests for the issue #320 suffix-collapse bug."""

    def test_distinct_names_sharing_a_suffix_do_not_merge(self):
        from sdrf_pipelines.converters.openms.experimental_design import SampleIdTracker

        tracker = SampleIdTracker()
        results = {
            name: tracker.get_sample_info(name)
            for name in ["Tumor sample 1", "Normal sample 1", "Tumor sample 2", "Normal sample 2"]
        }
        samples = [sample for sample, _bio in results.values()]
        assert len(set(samples)) == 4, f"names collapsed: {results}"

    def test_canonical_sample_n_still_uses_numeric_shortcut(self):
        from sdrf_pipelines.converters.openms.experimental_design import SampleIdTracker

        tracker = SampleIdTracker()
        assert tracker.get_sample_info("Sample 1") == ("1", "1")
        assert tracker.get_sample_info("Sample 2") == ("2", "2")
        # the same source name maps back to the same sample
        assert tracker.get_sample_info("Sample 1") == ("1", "1")

    def test_repeated_distinct_name_is_stable(self):
        from sdrf_pipelines.converters.openms.experimental_design import SampleIdTracker

        tracker = SampleIdTracker()
        first = tracker.get_sample_info("Tumor sample 1")
        # an unrelated name in between must not disturb the mapping
        tracker.get_sample_info("Normal sample 1")
        assert tracker.get_sample_info("Tumor sample 1") == first
