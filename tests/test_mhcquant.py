"""Tests for the MHCquant SDRF converter."""

import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from sdrf_pipelines.converters.mhcquant.mhcquant import MHCquant
from sdrf_pipelines.converters.mhcquant.constants import load_default_presets

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
    """Test basic conversion with PXD009749-like SDRF (no mz/charge range columns)."""

    def test_convert_produces_output_files(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(BASIC_SDRF), str(ss), str(presets))
        assert ss.exists()
        assert presets.exists()

    def test_samplesheet_columns(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(BASIC_SDRF), str(ss), str(presets))

        df = pd.read_csv(ss, sep="\t")
        assert list(df.columns) == ["ID", "Sample", "Condition", "ReplicateFileName", "SearchPreset"]

    def test_samplesheet_row_count(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(BASIC_SDRF), str(ss), str(presets))

        df = pd.read_csv(ss, sep="\t")
        assert len(df) == 3  # PXD009749 has 3 data rows

    def test_samplesheet_ids_autoincrement(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(BASIC_SDRF), str(ss), str(presets))

        df = pd.read_csv(ss, sep="\t")
        assert list(df["ID"]) == [1, 2, 3]

    def test_samplesheet_sample_from_factor_value(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(BASIC_SDRF), str(ss), str(presets))

        df = pd.read_csv(ss, sep="\t")
        # PXD009749: factor value[source name] = "07H103" for all rows
        assert all(df["Sample"] == "07H103")

    def test_samplesheet_condition_from_source_name(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(BASIC_SDRF), str(ss), str(presets))

        df = pd.read_csv(ss, sep="\t")
        # PXD009749: single source name "07H103" → condition 1
        assert all(df["Condition"] == 1)

    def test_samplesheet_replicate_filenames(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(BASIC_SDRF), str(ss), str(presets))

        df = pd.read_csv(ss, sep="\t")
        expected = ["MHC_07H103_Rep1.raw", "MHC_07H103_Rep2.raw", "MHC_07H103_Rep3.raw"]
        assert list(df["ReplicateFileName"]) == expected

    def test_raw_file_prefix(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(BASIC_SDRF), str(ss), str(presets), raw_file_prefix="/data/raw/")

        df = pd.read_csv(ss, sep="\t")
        assert all(f.startswith("/data/raw/") for f in df["ReplicateFileName"])


class TestFallbackPreset:
    """Test preset fallback when mz/charge range columns are missing."""

    def test_falls_back_to_default_preset(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(BASIC_SDRF), str(ss), str(presets))

        df = pd.read_csv(ss, sep="\t")
        # PXD009749: Q Exactive + MHC class I → qe_class1
        assert all(df["SearchPreset"] == "qe_class1")

    def test_presets_file_contains_only_used_presets(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(BASIC_SDRF), str(ss), str(presets))

        preset_df = pd.read_csv(presets, sep="\t")
        assert len(preset_df) == 1
        assert preset_df.iloc[0]["PresetName"] == "qe_class1"

    def test_presets_file_columns(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(BASIC_SDRF), str(ss), str(presets))

        preset_df = pd.read_csv(presets, sep="\t")
        expected_cols = [
            "PresetName", "PeptideMinLength", "PeptideMaxLength",
            "PrecursorMassRange", "PrecursorCharge", "PrecursorMassTolerance",
            "PrecursorErrorUnit", "FragmentMassTolerance", "FragmentBinOffset",
            "MS2PIPModel", "ActivationMethod", "Instrument", "NumberMods",
            "FixedMods", "VariableMods",
        ]
        assert list(preset_df.columns) == expected_cols

    def test_default_preset_values_match(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(BASIC_SDRF), str(ss), str(presets))

        preset_df = pd.read_csv(presets, sep="\t")
        row = preset_df.iloc[0]
        expected = load_default_presets()["qe_class1"]
        assert row["PeptideMinLength"] == expected["PeptideMinLength"]
        assert row["PeptideMaxLength"] == expected["PeptideMaxLength"]
        assert row["PrecursorMassRange"] == expected["PrecursorMassRange"]
        assert row["MS2PIPModel"] == expected["MS2PIPModel"]
        assert row["Instrument"] == expected["Instrument"]


class TestCustomPreset:
    """Test custom preset generation when all DDA columns are present."""

    def test_full_sdrf_generates_custom_presets(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(FULL_SDRF), str(ss), str(presets))

        preset_df = pd.read_csv(presets, sep="\t")
        # Full SDRF has 2 different parameter combos (QE class2 vs Lumos class1)
        assert len(preset_df) == 2

    def test_full_sdrf_samplesheet(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(FULL_SDRF), str(ss), str(presets))

        df = pd.read_csv(ss, sep="\t")
        assert len(df) == 4
        assert list(df["Sample"]) == ["meningioma", "meningioma", "normal", "normal"]

    def test_full_sdrf_conditions(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(FULL_SDRF), str(ss), str(presets))

        df = pd.read_csv(ss, sep="\t")
        # patient_1 → 1, patient_2 → 2, patient_3 → 3
        assert list(df["Condition"]) == [1, 1, 2, 3]

    def test_custom_preset_has_correct_mz_range(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(FULL_SDRF), str(ss), str(presets))

        preset_df = pd.read_csv(presets, sep="\t")
        # All presets should have their specific mz ranges
        preset_names = list(preset_df["PresetName"])
        for _, row in preset_df.iterrows():
            assert ":" in str(row["PrecursorMassRange"])

    def test_custom_preset_has_correct_charge_range(self, converter, tmpdir):
        ss = tmpdir / "samplesheet.tsv"
        presets = tmpdir / "presets.tsv"
        converter.convert(str(FULL_SDRF), str(ss), str(presets))

        preset_df = pd.read_csv(presets, sep="\t")
        for _, row in preset_df.iterrows():
            assert ":" in str(row["PrecursorCharge"])


class TestInstrumentMapping:
    """Test instrument name → preset prefix mapping."""

    def test_q_exactive(self, converter):
        params = {"instrument_name": "Q Exactive", "mhc_class": "class1"}
        name, _ = converter._map_to_default_preset(params, load_default_presets())
        assert name == "qe_class1"

    def test_lumos(self, converter):
        params = {"instrument_name": "Orbitrap Fusion Lumos", "mhc_class": "class1"}
        name, _ = converter._map_to_default_preset(params, load_default_presets())
        assert name == "lumos_class1"

    def test_timstof(self, converter):
        params = {"instrument_name": "timsTOF Pro", "mhc_class": "class2"}
        name, _ = converter._map_to_default_preset(params, load_default_presets())
        assert name == "timstof_class2"

    def test_astral(self, converter):
        params = {"instrument_name": "Orbitrap Astral", "mhc_class": "class1"}
        name, _ = converter._map_to_default_preset(params, load_default_presets())
        assert name == "astral_class1"

    def test_xl(self, converter):
        params = {"instrument_name": "LTQ Orbitrap XL", "mhc_class": "class2"}
        name, _ = converter._map_to_default_preset(params, load_default_presets())
        assert name == "xl_class2"

    def test_exploris(self, converter):
        params = {"instrument_name": "Orbitrap Exploris 480", "mhc_class": "class1"}
        name, _ = converter._map_to_default_preset(params, load_default_presets())
        assert name == "lumos_class1"

    def test_eclipse(self, converter):
        params = {"instrument_name": "Orbitrap Eclipse", "mhc_class": "class2"}
        name, _ = converter._map_to_default_preset(params, load_default_presets())
        assert name == "lumos_class2"

    def test_unrecognized_instrument_falls_back_to_qe(self, converter):
        params = {"instrument_name": "Unknown Instrument", "mhc_class": "class1"}
        name, _ = converter._map_to_default_preset(params, load_default_presets())
        assert name == "qe_class1"
        assert any("Unrecognized instrument" in w for w in converter.warnings)


class TestModificationParsing:
    """Test extraction of modification parameters."""

    def test_variable_mods(self, converter):
        row = pd.Series({
            "comment[modification parameters]": "NT=Oxidation;MT=Variable;TA=M;AC=UNIMOD:35;PP=Anywhere",
        })
        columns = pd.Index(["comment[modification parameters]"])
        fixed, variable = converter._extract_modifications(row, columns)
        assert "Oxidation (M)" in variable
        assert fixed == ""

    def test_fixed_mods(self, converter):
        row = pd.Series({
            "comment[modification parameters]": "NT=Carbamidomethyl;MT=Fixed;TA=C;AC=UNIMOD:4;PP=Anywhere",
        })
        columns = pd.Index(["comment[modification parameters]"])
        fixed, variable = converter._extract_modifications(row, columns)
        assert "Carbamidomethyl (C)" in fixed
        assert variable == ""


class TestMHCClassParsing:
    """Test MHC class determination."""

    def test_class_i_from_protein_complex(self, converter):
        row = pd.Series({
            "characteristics[mhc protein complex]": "MHC class I protein complex",
        })
        columns = pd.Index(["characteristics[mhc protein complex]"])
        assert converter._determine_mhc_class(row, columns) == "class1"

    def test_class_ii_from_protein_complex(self, converter):
        row = pd.Series({
            "characteristics[mhc protein complex]": "MHC class II protein complex",
        })
        columns = pd.Index(["characteristics[mhc protein complex]"])
        assert converter._determine_mhc_class(row, columns) == "class2"

    def test_class_from_mhc_class_column(self, converter):
        row = pd.Series({
            "characteristics[mhc class]": "MHC class I",
        })
        columns = pd.Index(["characteristics[mhc class]"])
        assert converter._determine_mhc_class(row, columns) == "class1"

    def test_missing_mhc_class_defaults_to_class1(self, converter):
        row = pd.Series({"characteristics[organism]": "Homo sapiens"})
        columns = pd.Index(["characteristics[organism]"])
        assert converter._determine_mhc_class(row, columns) == "class1"
        assert any("Missing MHC class" in w for w in converter.warnings)


class TestColumnAliases:
    """Test backward-compatible column name resolution."""

    def test_new_column_name_preferred(self, converter):
        row = pd.Series({
            "comment[ms min mz]": "300m/z",
            "comment[precursor min mz]": "400m/z",
        })
        columns = pd.Index(["comment[ms min mz]", "comment[precursor min mz]"])
        val = converter._resolve_column(row, columns, "comment[ms min mz]")
        assert val == "300m/z"

    def test_old_column_name_fallback(self, converter):
        row = pd.Series({
            "comment[precursor min mz]": "400m/z",
        })
        columns = pd.Index(["comment[precursor min mz]"])
        val = converter._resolve_column(row, columns, "comment[ms min mz]")
        assert val == "400m/z"


class TestToleranceParsing:
    """Test tolerance string parsing."""

    def test_ppm(self, converter):
        val, unit = converter._parse_tolerance("10 ppm")
        assert val == 10.0
        assert unit == "ppm"

    def test_da(self, converter):
        val, unit = converter._parse_tolerance("0.01 Da")
        assert val == 0.01
        assert unit == "da"

    def test_strip_unit_mz(self, converter):
        assert converter._strip_unit("300m/z") == "300"
        assert converter._strip_unit("1650m/z") == "1650"

    def test_nt_value_extraction(self, converter):
        assert converter._extract_nt_value("NT=HCD;AC=MS:1000422") == "HCD"
        assert converter._extract_nt_value("NT=Q Exactive;AC=MS:1001911") == "Q Exactive"


class TestResolution:
    """Test instrument resolution determination."""

    def test_orbitrap_is_high_res(self, converter):
        assert converter._determine_resolution("Q Exactive", "orbitrap") == "high_res"

    def test_ion_trap_is_low_res(self, converter):
        assert converter._determine_resolution("LTQ", "ion trap") == "low_res"

    def test_xl_without_orbitrap_ms2_is_low_res(self, converter):
        assert converter._determine_resolution("LTQ Orbitrap XL", "linear trap") == "low_res"

    def test_high_fragment_tolerance_triggers_low_res(self, converter):
        # 0.5 Da fragment tolerance → low_res even if analyzer says orbitrap
        assert converter._determine_resolution("Q Exactive", "orbitrap", fragment_mass_tolerance=0.5) == "low_res"

    def test_borderline_fragment_tolerance_triggers_low_res(self, converter):
        # 0.1 Da is the threshold
        assert converter._determine_resolution("Q Exactive", "orbitrap", fragment_mass_tolerance=0.1) == "low_res"

    def test_small_fragment_tolerance_stays_high_res(self, converter):
        assert converter._determine_resolution("Q Exactive", "orbitrap", fragment_mass_tolerance=0.02) == "high_res"


class TestLowResEnforcement:
    """Test that low-res detection forces correct fragment tolerance and bin offset."""

    def test_low_res_forces_fragment_tolerance(self, converter):
        """When low-res is detected, fragment tolerance must be exactly 0.50025."""
        row = pd.Series({
            "source name": "patient_1",
            "characteristics[mhc protein complex]": "MHC class I protein complex",
            "comment[precursor mass tolerance]": "5 ppm",
            "comment[fragment mass tolerance]": "0.5 Da",
            "comment[instrument]": "NT=LTQ Orbitrap XL;AC=MS:1000556",
            "comment[ms2 mass analyzer]": "NT=ion trap;AC=MS:1000264",
            "comment[dissociation method]": "NT=CID;AC=MS:1000133",
        })
        columns = row.index
        params = converter._extract_search_params(row, columns)
        assert params["fragment_mass_tolerance"] == 0.50025
        assert params["fragment_bin_offset"] == 0.4
        assert params["instrument_resolution"] == "low_res"

    def test_low_res_from_high_frag_tol_forces_values(self, converter):
        """Even with orbitrap analyzer, high frag tolerance → low_res with forced values."""
        row = pd.Series({
            "source name": "patient_1",
            "characteristics[mhc protein complex]": "MHC class I protein complex",
            "comment[precursor mass tolerance]": "10 ppm",
            "comment[fragment mass tolerance]": "0.6 Da",
            "comment[instrument]": "NT=Q Exactive;AC=MS:1001911",
            "comment[ms2 mass analyzer]": "NT=orbitrap;AC=MS:1000484",
            "comment[dissociation method]": "NT=HCD;AC=MS:1000422",
        })
        columns = row.index
        params = converter._extract_search_params(row, columns)
        assert params["fragment_mass_tolerance"] == 0.50025
        assert params["fragment_bin_offset"] == 0.4
        assert params["instrument_resolution"] == "low_res"

    def test_high_res_halves_frag_tol(self, converter):
        """High-res should halve fragment tolerance due to Comet binning."""
        row = pd.Series({
            "source name": "patient_1",
            "characteristics[mhc protein complex]": "MHC class I protein complex",
            "comment[precursor mass tolerance]": "5 ppm",
            "comment[fragment mass tolerance]": "0.02 Da",
            "comment[instrument]": "NT=Q Exactive;AC=MS:1001911",
            "comment[ms2 mass analyzer]": "NT=orbitrap;AC=MS:1000484",
            "comment[dissociation method]": "NT=HCD;AC=MS:1000422",
        })
        columns = row.index
        params = converter._extract_search_params(row, columns)
        assert params["fragment_mass_tolerance"] == 0.01
        assert params["fragment_bin_offset"] == 0.0
        assert params["instrument_resolution"] == "high_res"


class TestMS2PIPModel:
    """Test MS2PIP model determination."""

    def test_hcd_orbitrap(self, converter):
        assert converter._determine_ms2pip_model("HCD", "Q Exactive", "orbitrap") == "Immuno-HCD"

    def test_timstof(self, converter):
        assert converter._determine_ms2pip_model("CID", "timsTOF Pro", "") == "timsTOF"

    def test_cid_ion_trap(self, converter):
        assert converter._determine_ms2pip_model("CID", "LTQ", "ion trap") == "CIDch2"


class TestErrorHandling:
    """Test error handling for invalid SDRF files."""

    def test_missing_source_name_raises(self, converter, tmpdir):
        sdrf_path = tmpdir / "bad.sdrf.tsv"
        sdrf_path.write_text("comment[data file]\nrun1.raw\n")
        with pytest.raises(ValueError, match="source name"):
            converter.convert(str(sdrf_path), str(tmpdir / "ss.tsv"), str(tmpdir / "p.tsv"))

    def test_missing_data_file_raises(self, converter, tmpdir):
        sdrf_path = tmpdir / "bad.sdrf.tsv"
        sdrf_path.write_text("source name\npatient_1\n")
        with pytest.raises(ValueError, match="data file"):
            converter.convert(str(sdrf_path), str(tmpdir / "ss.tsv"), str(tmpdir / "p.tsv"))
