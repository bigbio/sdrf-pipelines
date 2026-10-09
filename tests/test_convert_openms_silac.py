"""SILAC regression tests exercise the complete SDRF-to-design conversion."""

from io import StringIO
from pathlib import Path

import pandas as pd
import pytest

from sdrf_pipelines.converters.openms import OpenMS

FORMATS = [(False, False), (True, False), (True, True)]
FORMAT_IDS = ["two-table", "one-table", "legacy-one-table"]


def make_silac_sdrf(plex: int, layout: str = "pairs") -> pd.DataFrame:
    """Two mixtures, with known sample identities and conditions per channel."""
    channels = ["light", "heavy"] if plex == 2 else ["light", "medium", "heavy"]
    rows = []
    for mixture in (1, 2):
        for replicate in (2, 4) if layout == "fractionated" else (1,):
            for fraction in (1, 2) if layout == "fractionated" else (1,):
                raw = f"mix{mixture}_rep{replicate}_f{fraction}.raw"
                for channel in channels:
                    sample = f"sample{mixture}_{channel}"
                    if layout == "reference" and channel == "heavy":
                        sample = "reference"
                    elif layout == "swap":
                        sample_channel = channels[::-1][channels.index(channel)] if mixture == 2 else channel
                        sample = f"sample_{sample_channel}"
                    rows.append(
                        {
                            "source name": sample,
                            "assay name": raw.removesuffix(".raw"),
                            "comment[data file]": raw,
                            "comment[label]": f"SILAC {channel}",
                            "comment[cleavage agent details]": "NT=Trypsin;AC=MS:1001251",
                            "comment[fraction identifier]": str(fraction),
                            "comment[technical replicate]": str(replicate),
                            "factor value[condition]": sample,
                        }
                    )
    return pd.DataFrame(rows)


def convert_design(
    sdrf: pd.DataFrame, directory: Path, one_table: bool, legacy: bool, split: bool = False
) -> list[tuple[Path, pd.DataFrame]]:
    sdrf_path = directory / "silac.sdrf.tsv"
    sdrf.to_csv(sdrf_path, sep="\t", index=False)
    OpenMS().openms_convert(
        str(sdrf_path),
        one_table=one_table,
        legacy=legacy,
        extension_convert="raw:mzML",
        split_by_columns="[factor value[condition]]" if split else None,
    )
    designs = []
    for path in sorted(directory.glob("experimental_design.tsv*")):
        sections = path.read_text().strip().split("\n\n")
        files = pd.read_csv(StringIO(sections[0]), sep="\t", dtype=str)
        assert "Sample" in files.columns
        if len(sections) == 2:
            samples = pd.read_csv(StringIO(sections[1]), sep="\t", dtype=str)
            files = files.merge(samples, on="Sample", validate="many_to_one")
        designs.append((path, files))
    assert designs
    return designs


def assert_design_mapping(sdrf: pd.DataFrame, design: pd.DataFrame, plex: int) -> None:
    expected_labels = {"SILAC light": "1", "SILAC heavy": "2"}
    if plex == 3:
        expected_labels = {"SILAC light": "1", "SILAC medium": "2", "SILAC heavy": "3"}
    expected = {
        (row["comment[data file]"].removesuffix(".raw") + ".mzML", expected_labels[row["comment[label]"]]): (
            row["source name"],
            row["comment[fraction identifier]"],
        )
        for _, row in sdrf.iterrows()
    }
    actual = {(row.Spectra_Filepath, row.Label): (row.MSstats_Condition, row.Fraction) for row in design.itertuples()}
    assert actual == expected
    assert len(design) == len(expected)
    assert not design.duplicated(["Spectra_Filepath", "Label"]).any()
    assert not design.duplicated(["Fraction_Group", "Fraction", "Label"]).any()
    assert design.groupby("Spectra_Filepath")["Fraction_Group"].nunique().eq(1).all()
    assert design.groupby("MSstats_Condition")["Sample"].nunique().eq(1).all()
    assert design.groupby("Sample")["MSstats_Condition"].nunique().eq(1).all()
    assert sorted(design.Fraction_Group.astype(int).unique()) == list(range(1, design.Fraction_Group.nunique() + 1))


@pytest.mark.parametrize("plex", [2, 3])
@pytest.mark.parametrize("encoding", ["plain", "nt", "mixed"])
@pytest.mark.parametrize("reverse", [False, True], ids=["forward", "reverse"])
@pytest.mark.parametrize("one_table,legacy", FORMATS, ids=FORMAT_IDS)
def test_silac_channels(
    plex: int, encoding: str, reverse: bool, one_table: bool, legacy: bool, on_tmpdir: Path
) -> None:
    expected = make_silac_sdrf(plex)
    sdrf = expected.copy()
    # Plain labels and NT syntax may occur in the same SDRF.
    for i in sdrf.index:
        if encoding == "nt" or (encoding == "mixed" and i % 2):
            sdrf.loc[i, "comment[label]"] = f"NT={sdrf.loc[i, 'comment[label]']};"
    if reverse:
        sdrf = sdrf.iloc[::-1]
    _, design = convert_design(sdrf, on_tmpdir, one_table, legacy)[0]
    assert_design_mapping(expected, design, plex)
    assert design.Fraction_Group.nunique() == 2
    settings = pd.read_csv(on_tmpdir / "openms.tsv", sep="\t")
    assert settings.Label.tolist() == ["SILAC", "SILAC"]


@pytest.mark.parametrize("plex", [2, 3])
@pytest.mark.parametrize("layout", ["reference", "fractionated", "swap"])
@pytest.mark.parametrize("one_table,legacy", FORMATS, ids=FORMAT_IDS)
def test_silac_mixtures(plex: int, layout: str, one_table: bool, legacy: bool, on_tmpdir: Path) -> None:
    sdrf = make_silac_sdrf(plex, layout)
    # Interleave fractions and mixtures instead of relying on adjacent rows for a raw file.
    shuffled = sdrf.sample(frac=1, random_state=17)
    _, design = convert_design(shuffled, on_tmpdir, one_table, legacy)[0]
    assert_design_mapping(sdrf, design, plex)
    expected_groups = 4 if layout == "fractionated" else 2
    assert design.Fraction_Group.nunique() == expected_groups
    if layout == "fractionated":
        assert design.groupby("Fraction_Group")["Fraction"].nunique().eq(2).all()
        # Fractions of the same mixture and replicate belong together, and only those do.
        run = design.Spectra_Filepath.str.replace(r"_f\d+\.mzML$", "", regex=True)
        assert design.groupby(run)["Fraction_Group"].nunique().eq(1).all()


@pytest.mark.parametrize("one_table,legacy", FORMATS, ids=FORMAT_IDS)
def test_split_silac_preserves_channels_and_reference_mixtures(one_table: bool, legacy: bool, on_tmpdir: Path) -> None:
    sdrf = make_silac_sdrf(3, "reference")
    designs = convert_design(sdrf, on_tmpdir, one_table, legacy, split=True)
    assert len(designs) == 5
    for _, design in designs:
        condition = design.MSstats_Condition.iloc[0]
        assert_design_mapping(sdrf[sdrf["factor value[condition]"] == condition], design, 3)
        if condition == "reference":
            assert design.Fraction_Group.nunique() == 2


@pytest.mark.parametrize("one_table,legacy", FORMATS, ids=FORMAT_IDS)
def test_missing_medium_keeps_three_plex_channel_numbers(one_table: bool, legacy: bool, on_tmpdir: Path) -> None:
    sdrf = make_silac_sdrf(3)
    sdrf = sdrf[sdrf["source name"] != "sample2_medium"]
    _, design = convert_design(sdrf, on_tmpdir, one_table, legacy)[0]
    assert_design_mapping(sdrf, design, 3)


@pytest.mark.parametrize("plex", [2, 3])
@pytest.mark.parametrize("layout", ["pairs", "reference", "fractionated", "swap"])
@pytest.mark.parametrize("one_table,legacy", FORMATS, ids=FORMAT_IDS)
def test_openms_loads_silac_design(plex: int, layout: str, one_table: bool, legacy: bool, on_tmpdir: Path) -> None:
    oms = pytest.importorskip("pyopenms")
    sdrf = make_silac_sdrf(plex, layout)
    path, table = convert_design(sdrf, on_tmpdir, one_table, legacy)[0]
    design = oms.ExperimentalDesignFile().load(str(path), False)
    assert design.getNumberOfLabels() == plex
    assert design.getNumberOfSamples() == sdrf["source name"].nunique()
    assert design.getNumberOfMSFiles() == sdrf["comment[data file]"].nunique()
    assert design.getNumberOfFractionGroups() == (4 if layout == "fractionated" else 2)
    samples = design.getSampleSection()
    sample_map = {}
    for entry in design.getMSFileSection():
        filepath = entry.path.decode() if isinstance(entry.path, bytes) else entry.path
        sample_map[(filepath, entry.label)] = entry.sample
    for row in table.itertuples():
        sample = sample_map[(row.Spectra_Filepath, int(str(row.Label)))]
        # The 3.5 binding requires an OpenMS String; 3.6 accepts Python strings.
        factor = oms.String("MSstats_Condition") if oms.__version__.startswith("3.5") else "MSstats_Condition"
        condition = samples.getFactorValue(sample, factor)
        if isinstance(condition, bytes):
            condition = condition.decode()
        assert condition == row.MSstats_Condition


@pytest.mark.parametrize("problem", ["duplicate_channel", "duplicate_acquisition", "fraction", "replicate", "mixed"])
def test_reject_inconsistent_silac_metadata(problem: str, on_tmpdir: Path) -> None:
    sdrf = make_silac_sdrf(2)
    if problem == "duplicate_channel":
        sdrf = pd.concat([sdrf, sdrf.iloc[[0]]], ignore_index=True)
    elif problem == "duplicate_acquisition":
        repeated = sdrf.iloc[:2].copy()
        repeated["comment[data file]"] = "repeat.raw"
        sdrf = pd.concat([sdrf, repeated], ignore_index=True)
    elif problem == "fraction":
        sdrf.loc[0, "comment[fraction identifier]"] = "2"
    elif problem == "replicate":
        sdrf.loc[0, "comment[technical replicate]"] = "2"
    else:
        sdrf.loc[sdrf["comment[data file]"].str.startswith("mix2"), "comment[label]"] = "label free sample"
    with pytest.raises(ValueError):
        convert_design(sdrf, on_tmpdir, False, False)
    assert not (on_tmpdir / "experimental_design.tsv").exists()


def test_existing_silac_cv_fixture(on_tmpdir: Path) -> None:
    sdrf = pd.read_csv(Path(__file__).parent / "data/diann/silac_2plex.sdrf.tsv", sep="\t", dtype=str)
    # The repository fixture has full NT/AC annotations and no fraction/replicate columns.
    _, design = convert_design(sdrf, on_tmpdir, False, False)[0]
    assert design.Label.tolist() == ["1", "2"]
    assert design.Sample.tolist() == ["1", "2"]
    assert design.Fraction.tolist() == ["1", "1"]
    assert design.Fraction_Group.tolist() == ["1", "1"]


@pytest.mark.parametrize("labels", [("TMT126", "TMT127"), ("ITRAQ114", "ITRAQ115")])
@pytest.mark.parametrize("one_table,legacy", FORMATS, ids=FORMAT_IDS)
def test_other_multiplex_cv_labels(labels: tuple[str, str], one_table: bool, legacy: bool, on_tmpdir: Path) -> None:
    sdrf = make_silac_sdrf(2)
    sdrf["comment[label]"] = [f"NT={labels[0]};", f"NT={labels[1]};"] * 2
    sdrf = sdrf.iloc[::-1]
    _, design = convert_design(sdrf, on_tmpdir, one_table, legacy)[0]
    assert design.Label.tolist() == ["2", "1", "2", "1"]
    assert design.MSstats_Condition.tolist() == sdrf["factor value[condition]"].tolist()
