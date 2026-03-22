"""Tests for DIA-NN SDRF converter."""

from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner

from sdrf_pipelines.converters.diann.diann import DiaNN
from sdrf_pipelines.parse_sdrf import cli


@pytest.fixture
def diann_data_dir():
    return Path(__file__).parent / "data" / "diann"


class TestDiannLabelFree:
    def test_label_free_generates_config(self, diann_data_dir, on_tmpdir):
        sdrf_file = str(diann_data_dir / "label_free.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        config_path = on_tmpdir / "diann_config.cfg"
        assert config_path.exists()
        content = config_path.read_text()
        assert "--cut K*,R*" in content
        assert "--fixed-mod Carbamidomethyl" in content
        assert "--var-mod Oxidation" in content
        # No plexDIA flags for label-free
        assert "--channels" not in content
        assert "--original-mods" not in content
        # Global max tolerances across runs (sample1: 10/20 ppm, sample2: 5/15 ppm)
        assert "--mass-acc-ms1 10.0" in content
        assert "--mass-acc 20.0" in content

    def test_label_free_generates_filemap(self, diann_data_dir, on_tmpdir):
        sdrf_file = str(diann_data_dir / "label_free.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        filemap_path = on_tmpdir / "diann_design.tsv"
        assert filemap_path.exists()
        import pandas as pd

        df = pd.read_csv(filemap_path, sep="\t")
        assert len(df) == 2
        assert "sample1.raw" in df["Filename"].values
        assert "sample2.raw" in df["Filename"].values

    def test_per_run_tolerances(self, diann_data_dir, on_tmpdir):
        sdrf_file = str(diann_data_dir / "label_free.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        import pandas as pd

        df = pd.read_csv(on_tmpdir / "diann_design.tsv", sep="\t")
        row1 = df[df["Filename"] == "sample1.raw"].iloc[0]
        row2 = df[df["Filename"] == "sample2.raw"].iloc[0]
        # Tolerances are strings from parse_tolerance
        assert str(row1["PrecursorMassTolerance"]) == "10"
        assert str(row1["FragmentMassTolerance"]) == "20"
        assert str(row2["PrecursorMassTolerance"]) == "5"
        assert str(row2["FragmentMassTolerance"]) == "15"


class TestDiannMtraq:
    def test_mtraq_generates_channels(self, diann_data_dir, on_tmpdir):
        sdrf_file = str(diann_data_dir / "mtraq_3plex.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        config_path = on_tmpdir / "diann_config.cfg"
        content = config_path.read_text()
        assert "--channels" in content
        assert "mTRAQ,0,nK,0:0" in content
        assert "mTRAQ,4,nK,4.0070994:4.0070994" in content
        assert "--fixed-mod mTRAQ,140.0949630177,nK" in content
        assert "--lib-fixed-mod mTRAQ" in content
        assert "--original-mods" in content

    def test_mtraq_filemap_has_labels(self, diann_data_dir, on_tmpdir):
        sdrf_file = str(diann_data_dir / "mtraq_3plex.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        import pandas as pd

        df = pd.read_csv(on_tmpdir / "diann_design.tsv", sep="\t")
        assert len(df) == 3
        assert set(df["Label"]) == {"MTRAQ0", "MTRAQ4", "MTRAQ8"}
        assert all(df["LabelType"] == "mtraq")


class TestDiannSilac:
    def test_silac_has_label_suffix(self, diann_data_dir, on_tmpdir):
        sdrf_file = str(diann_data_dir / "silac_2plex.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        config_path = on_tmpdir / "diann_config.cfg"
        content = config_path.read_text()
        assert "--fixed-mod SILAC,0.0,KR,label" in content
        assert "--channels" in content
        assert "SILAC,L,KR,0:0" in content
        assert "SILAC,H,KR,8.014199:10.008269" in content
        assert "--lib-fixed-mod SILAC" in content
        assert "--original-mods" in content


class TestDiannScanRanges:
    def test_scan_range_from_interval_columns(self, diann_data_dir, on_tmpdir):
        sdrf_file = str(diann_data_dir / "scan_range_interval.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        config_path = on_tmpdir / "diann_config.cfg"
        content = config_path.read_text()
        # Global min across runs: min(400,350)=350, max across runs: max(1200,1400)=1400
        assert "--min-pr-mz 350.0" in content
        assert "--max-pr-mz 1400.0" in content
        # MS2: min(100,150)=100, max(1800,2000)=2000
        assert "--min-fr-mz 100.0" in content
        assert "--max-fr-mz 2000.0" in content

    def test_scan_range_from_discrete_columns(self, diann_data_dir, on_tmpdir):
        sdrf_file = str(diann_data_dir / "scan_range_discrete.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        config_path = on_tmpdir / "diann_config.cfg"
        content = config_path.read_text()
        assert "--min-pr-mz 350.0" in content
        assert "--max-pr-mz 1400.0" in content
        assert "--min-fr-mz 100.0" in content
        assert "--max-fr-mz 2000.0" in content

    def test_scan_range_interval_preferred_over_discrete(self, diann_data_dir, on_tmpdir):
        sdrf_file = str(diann_data_dir / "scan_range_both.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        config_path = on_tmpdir / "diann_config.cfg"
        content = config_path.read_text()
        # Should use interval values (400, 1200) not discrete (300, 1500)
        assert "--min-pr-mz 400.0" in content
        assert "--max-pr-mz 1200.0" in content
        # MS2: should use interval (100, 1800) not discrete (50, 2000)
        assert "--min-fr-mz 100.0" in content
        assert "--max-fr-mz 1800.0" in content
        # Should have warnings about both columns present
        assert any("interval" in w.lower() and "discrete" in w.lower() for w in converter.warnings)

    def test_scan_range_in_filemap(self, diann_data_dir, on_tmpdir):
        sdrf_file = str(diann_data_dir / "scan_range_interval.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        import pandas as pd

        df = pd.read_csv(on_tmpdir / "diann_design.tsv", sep="\t")
        row1 = df[df["Filename"] == "sample1.raw"].iloc[0]
        row2 = df[df["Filename"] == "sample2.raw"].iloc[0]
        assert row1["MS1MinMz"] == 400.0
        assert row1["MS1MaxMz"] == 1200.0
        assert row1["MS2MinMz"] == 100.0
        assert row1["MS2MaxMz"] == 1800.0
        assert row2["MS1MinMz"] == 350.0
        assert row2["MS1MaxMz"] == 1400.0

    def test_no_scan_range_columns_no_flags(self, diann_data_dir, on_tmpdir):
        sdrf_file = str(diann_data_dir / "label_free.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        config_path = on_tmpdir / "diann_config.cfg"
        content = config_path.read_text()
        assert "--min-pr-mz" not in content
        assert "--max-pr-mz" not in content
        assert "--min-fr-mz" not in content
        assert "--max-fr-mz" not in content

    def test_compact_mz_format(self, diann_data_dir, on_tmpdir):
        """Verify that compact format without spaces (400m/z) also works."""

        # Create a temp SDRF with compact format
        tmp = on_tmpdir / "compact.sdrf.tsv"
        tmp.write_text(
            "source name\tcharacteristics[organism]\tassay name\tcomment[label]\t"
            "comment[instrument]\tcomment[cleavage agent details]\t"
            "comment[modification parameters]\tcomment[modification parameters]\t"
            "comment[precursor mass tolerance]\tcomment[fragment mass tolerance]\t"
            "comment[data file]\tcomment[ms1 scan range]\n"
            "Sample 1\tHomo sapiens\trun 1\tAC=MS:1002038;NT=label free sample\t"
            "AC=MS:1001742;NT=LTQ Orbitrap Velos\tNT=Trypsin/P;AC=MS:1001313\t"
            "NT=Carbamidomethyl;TA=C;MT=fixed;AC=UNIMOD:4\t"
            "NT=Oxidation;MT=variable;TA=M;AC=UNIMOD:35\t10 ppm\t20 ppm\t"
            "sample1.raw\t400m/z-1200m/z\n"
        )
        converter = DiaNN()
        converter.diann_convert(str(tmp))

        config_path = on_tmpdir / "diann_config.cfg"
        content = config_path.read_text()
        assert "--min-pr-mz 400.0" in content
        assert "--max-pr-mz 1200.0" in content


class TestDiannCli:
    def test_cli_convert_diann_label_free(self, diann_data_dir, on_tmpdir):
        runner = CliRunner()
        sdrf_file = str(diann_data_dir / "label_free.sdrf.tsv")
        result = runner.invoke(cli, ["convert-diann", "--sdrf", sdrf_file])
        assert result.exit_code == 0
        assert (on_tmpdir / "diann_config.cfg").exists()
        assert (on_tmpdir / "diann_design.tsv").exists()

    def test_cli_convert_diann_missing_sdrf(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["convert-diann"])
        assert result.exit_code != 0


class TestDiannUnifiedDesign:
    """Tests for the unified diann_design.tsv output with experimental design columns."""

    def test_design_file_has_all_columns(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "label_free_design.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        expected_cols = {
            "Filename",
            "URI",
            "Sample",
            "FractionGroup",
            "Fraction",
            "Label",
            "LabelType",
            "AcquisitionMethod",
            "DissociationMethod",
            "Condition",
            "BioReplicate",
            "Enzyme",
            "FixedModifications",
            "VariableModifications",
            "PrecursorMassTolerance",
            "PrecursorMassToleranceUnit",
            "FragmentMassTolerance",
            "FragmentMassToleranceUnit",
            "MS1MinMz",
            "MS1MaxMz",
            "MS2MinMz",
            "MS2MaxMz",
        }
        assert expected_cols.issubset(set(df.columns)), f"Missing: {expected_cols - set(df.columns)}"

    def test_design_file_row_count(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "label_free_design.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        assert len(df) == 2

    def test_condition_from_factor_values(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "label_free_design.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        conditions = df["Condition"].tolist()
        assert "treatment_A" in conditions
        assert "treatment_B" in conditions

    def test_bioreplicate_assignment(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "label_free_design.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        assert len(set(df["BioReplicate"].tolist())) == 2

    def test_sample_ids_assigned(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "label_free_design.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        samples = df["Sample"].tolist()
        assert all(isinstance(s, (int, float)) for s in samples)
        assert len(set(samples)) == 2

    def test_fraction_group_and_fraction(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "label_free_design.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        assert all(df["Fraction"] == 1)
        assert len(df["FractionGroup"].unique()) == 2

    def test_acquisition_method_column(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "label_free_design.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        assert all(df["AcquisitionMethod"].str.contains("Independent", case=False))

    def test_dissociation_method_column(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "label_free_design.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        assert all(df["DissociationMethod"] == "HCD")

    def test_per_file_scan_ranges_preserved(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "label_free_design.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        row1 = df[df["Filename"] == "sample1.raw"].iloc[0]
        assert row1["MS1MinMz"] == 400.0
        assert row1["MS1MaxMz"] == 600.0
        row2 = df[df["Filename"] == "sample2.raw"].iloc[0]
        assert row2["MS1MinMz"] == 600.0
        assert row2["MS1MaxMz"] == 800.0

    def test_design_file_from_minimal_sdrf(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "label_free.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        assert len(df) == 2
        assert all(df["Fraction"] == 1)
        assert "Sample" in df.columns
        assert "Condition" in df.columns
        conditions = df["Condition"].tolist()
        assert "Sample 1" in conditions
        assert "Sample 2" in conditions


class TestDiannGpfDesign:
    def test_gpf_row_count(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "gpf_fractions.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        assert len(df) == 6

    def test_gpf_per_file_scan_ranges(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "gpf_fractions.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        gpf1 = df[df["Filename"] == "sample1_gpf1.raw"].iloc[0]
        gpf2 = df[df["Filename"] == "sample1_gpf2.raw"].iloc[0]
        gpf3 = df[df["Filename"] == "sample1_gpf3.raw"].iloc[0]
        assert gpf1["MS1MinMz"] == 400.0 and gpf1["MS1MaxMz"] == 600.0
        assert gpf2["MS1MinMz"] == 600.0 and gpf2["MS1MaxMz"] == 800.0
        assert gpf3["MS1MinMz"] == 800.0 and gpf3["MS1MaxMz"] == 1000.0

    def test_gpf_fraction_numbers(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "gpf_fractions.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        sample1 = df[df["Filename"].str.startswith("sample1")].sort_values("Filename")
        assert sample1["Fraction"].tolist() == [1, 2, 3]

    def test_gpf_fraction_groups(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "gpf_fractions.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        sample1 = df[df["Filename"].str.startswith("sample1")]
        sample2 = df[df["Filename"].str.startswith("sample2")]
        assert sample1["FractionGroup"].nunique() == 1
        assert sample2["FractionGroup"].nunique() == 1
        assert sample1["FractionGroup"].iloc[0] != sample2["FractionGroup"].iloc[0]

    def test_gpf_conditions(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "gpf_fractions.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        sample1 = df[df["Filename"].str.startswith("sample1")]
        sample2 = df[df["Filename"].str.startswith("sample2")]
        assert all(sample1["Condition"] == "control")
        assert all(sample2["Condition"] == "treated")

    def test_gpf_global_scan_range_in_config(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "gpf_fractions.sdrf.tsv"))
        config = Path("diann_config.cfg").read_text()
        assert "--min-pr-mz 400" in config
        assert "--max-pr-mz 1000" in config

    def test_gpf_bioreplicates(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "gpf_fractions.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        sample1 = df[df["Filename"].str.startswith("sample1")]
        assert sample1["BioReplicate"].nunique() == 1


class TestDiannPlexDesign:
    def test_mtraq_per_channel_conditions(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "mtraq_design.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        assert len(df) == 3
        ch0 = df[df["Label"] == "MTRAQ0"].iloc[0]
        ch4 = df[df["Label"] == "MTRAQ4"].iloc[0]
        ch8 = df[df["Label"] == "MTRAQ8"].iloc[0]
        assert ch0["Condition"] == "control"
        assert ch4["Condition"] == "low_dose"
        assert ch8["Condition"] == "high_dose"

    def test_mtraq_per_channel_bioreplicates(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "mtraq_design.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        assert len(set(df["BioReplicate"].tolist())) == 3

    def test_mtraq_per_channel_samples(self, diann_data_dir, on_tmpdir):
        DiaNN().diann_convert(str(diann_data_dir / "mtraq_design.sdrf.tsv"))
        df = pd.read_csv("diann_design.tsv", sep="\t")
        assert len(set(df["Sample"].tolist())) == 3


class TestDiannScanRangeValidation:
    def test_inverted_scan_range_raises_error(self, diann_data_dir, on_tmpdir):
        with pytest.raises(ValueError, match="[Ii]nverted|[Mm]in.*greater.*max"):
            DiaNN().diann_convert(str(diann_data_dir / "scan_range_inverted.sdrf.tsv"))
