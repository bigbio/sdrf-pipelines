"""Tests for the MHCquant SDRF converter."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from sdrf_pipelines.converters.mhcquant.mhcquant import MHCquant

DATA_DIR = Path(__file__).parent / "data" / "mhcquant"
BASIC_SDRF = DATA_DIR / "PXD009749.sdrf.tsv"
FULL_SDRF = DATA_DIR / "PXD012308.sdrf.tsv"


@pytest.fixture
def converter():
    return MHCquant()


@pytest.fixture
def tmpdir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


class TestBasicConversion:
    """Test conversion with PXD009749 SDRF (no mz/charge range columns → fallback preset)."""

    def test_samplesheet_and_presets(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(BASIC_SDRF), str(ss), output_presets=str(presets))

        df = pd.read_csv(ss, sep="\t")
        assert list(df.columns) == ["ID", "Sample", "Condition", "ReplicateFileName", "SearchPreset"]
        assert len(df) == 3
        assert list(df["ID"]) == [1, 2, 3]
        assert all(df["Sample"] == "07H103")
        assert all(df["Condition"] == 1)
        assert list(df["ReplicateFileName"]) == ["MHC_07H103_Rep1.raw", "MHC_07H103_Rep2.raw", "MHC_07H103_Rep3.raw"]
        # Falls back to default qe_class1 because SDRF lacks DDA columns
        assert all(df["SearchPreset"] == "qe_class1")

        preset_df = pd.read_csv(presets, sep="\t")
        assert len(preset_df) == 1
        assert preset_df.iloc[0]["PresetName"] == "qe_class1"


class TestFullConversion:
    """Test conversion with PXD012308 SDRF (full DDA columns → custom presets)."""

    def test_samplesheet_and_presets(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(FULL_SDRF), str(ss), output_presets=str(presets))

        df = pd.read_csv(ss, sep="\t")
        assert len(df) == 4
        assert list(df["Sample"]) == ["meningioma", "meningioma", "normal", "normal"]
        assert all(df["Condition"] == 1)

        preset_df = pd.read_csv(presets, sep="\t")
        assert len(preset_df) == 2


class TestLowResEnforcement:
    """Test that low-res detection forces correct fragment tolerance and bin offset."""

    @pytest.mark.parametrize(
        "instrument,analyzer,frag_tol",
        [
            ("LTQ Orbitrap XL", "ion trap", "0.5 Da"),
            ("Q Exactive", "orbitrap", "0.6 Da"),
        ],
    )
    def test_low_res_forces_values(self, converter, instrument, analyzer, frag_tol):
        row = pd.Series(
            {
                "source name": "patient_1",
                "characteristics[mhc protein complex]": "MHC class I protein complex",
                "comment[precursor mass tolerance]": "5 ppm",
                "comment[fragment mass tolerance]": frag_tol,
                "comment[instrument]": f"NT={instrument};AC=MS:0000000",
                "comment[ms2 mass analyzer]": f"NT={analyzer};AC=MS:0000000",
                "comment[dissociation method]": "NT=HCD;AC=MS:1000422",
            }
        )
        params = converter._extract_search_params(row)
        assert params["fragment_mass_tolerance"] == 0.50025
        assert params["fragment_bin_offset"] == 0.4
        assert params["instrument_resolution"] == "low_res"

    def test_high_res_halves_frag_tol(self, converter):
        row = pd.Series(
            {
                "source name": "patient_1",
                "characteristics[mhc protein complex]": "MHC class I protein complex",
                "comment[precursor mass tolerance]": "5 ppm",
                "comment[fragment mass tolerance]": "0.02 Da",
                "comment[instrument]": "NT=Q Exactive;AC=MS:1001911",
                "comment[ms2 mass analyzer]": "NT=orbitrap;AC=MS:1000484",
                "comment[dissociation method]": "NT=HCD;AC=MS:1000422",
            }
        )
        params = converter._extract_search_params(row)
        assert params["fragment_mass_tolerance"] == 0.01
        assert params["fragment_bin_offset"] == 0.0

    def test_ppm_fragment_tolerance_converted_to_da(self, converter):
        """Fragment tolerance in ppm is converted to Da (MHCquant only supports Da)."""
        row = pd.Series(
            {
                "source name": "patient_1",
                "characteristics[mhc protein complex]": "MHC class I protein complex",
                "comment[precursor mass tolerance]": "5 ppm",
                "comment[fragment mass tolerance]": "20 ppm",
                "comment[instrument]": "NT=Q Exactive;AC=MS:1001911",
                "comment[ms2 mass analyzer]": "NT=orbitrap;AC=MS:1000484",
                "comment[dissociation method]": "NT=HCD;AC=MS:1000422",
            }
        )
        params = converter._extract_search_params(row)
        # 20 ppm at 1000 Da reference = 0.02 Da, then halved for high-res Comet = 0.01
        assert params["fragment_mass_tolerance"] == pytest.approx(0.01)
        assert params["fragment_bin_offset"] == 0.0
        assert any("ppm" in w for w in converter.warnings)


class TestErrorHandling:
    """Test error handling for invalid SDRF files."""

    def test_missing_source_name_raises(self, converter, tmpdir):
        sdrf_path = tmpdir / "bad.sdrf.tsv"
        sdrf_path.write_text("comment[data file]\tfactor value[disease]\nrun1.raw\tnormal\n")
        with pytest.raises(ValueError, match="source name"):
            converter.convert(str(sdrf_path), str(tmpdir / "ss.tsv"), output_presets=str(tmpdir / "p.tsv"))

    def test_missing_data_file_raises(self, converter, tmpdir):
        sdrf_path = tmpdir / "bad.sdrf.tsv"
        sdrf_path.write_text("source name\tfactor value[disease]\npatient_1\tnormal\n")
        with pytest.raises(ValueError, match="data file"):
            converter.convert(str(sdrf_path), str(tmpdir / "ss.tsv"), output_presets=str(tmpdir / "p.tsv"))

    def test_missing_factor_value_raises(self, converter, tmpdir):
        sdrf_path = tmpdir / "bad.sdrf.tsv"
        sdrf_path.write_text("source name\tcomment[data file]\npatient_1\trun1.raw\n")
        with pytest.raises(ValueError, match="No factor value columns"):
            converter.convert(str(sdrf_path), str(tmpdir / "ss.tsv"), output_presets=str(tmpdir / "p.tsv"))
