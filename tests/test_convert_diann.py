"""Tests for DIA-NN SDRF converter."""

from pathlib import Path

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

    def test_label_free_generates_filemap(self, diann_data_dir, on_tmpdir):
        sdrf_file = str(diann_data_dir / "label_free.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        filemap_path = on_tmpdir / "diann_filemap.tsv"
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

        df = pd.read_csv(on_tmpdir / "diann_filemap.tsv", sep="\t")
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

        df = pd.read_csv(on_tmpdir / "diann_filemap.tsv", sep="\t")
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


class TestDiannCli:
    def test_cli_convert_diann_label_free(self, diann_data_dir, on_tmpdir):
        runner = CliRunner()
        sdrf_file = str(diann_data_dir / "label_free.sdrf.tsv")
        result = runner.invoke(cli, ["convert-diann", "--sdrf", sdrf_file])
        assert result.exit_code == 0
        assert (on_tmpdir / "diann_config.cfg").exists()
        assert (on_tmpdir / "diann_filemap.tsv").exists()

    def test_cli_convert_diann_missing_sdrf(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["convert-diann"])
        assert result.exit_code != 0
