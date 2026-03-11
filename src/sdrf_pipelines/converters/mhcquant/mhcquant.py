"""MHCquant SDRF converter.

Converts SDRF files to nf-core/mhcquant samplesheet and search presets files.
"""

import re

import pandas as pd

from sdrf_pipelines.converters.base import BaseConverter
from sdrf_pipelines.converters.mhcquant.constants import (
    INSTRUMENT_PRESET_MAP,
    MHC_CLASS_PEPTIDE_LENGTHS,
    PRESET_COLUMNS,
    load_default_presets,
)
from sdrf_pipelines.converters.openms.modifications import ModificationConverter
from sdrf_pipelines.converters.openms.utils import parse_tolerance

_EMPTY_VALUES = {"nan", "", "not available"}


class MHCquant(BaseConverter):
    """Converter from SDRF to nf-core/mhcquant samplesheet and search presets."""

    def __init__(self):
        super().__init__()
        self._mod_converter = ModificationConverter()

    def convert(
        self,
        sdrf_file: str,
        output_samplesheet: str = "mhcquant_samplesheet.tsv",
        output_presets: str = "search_presets.tsv",
        default_presets_file: str | None = None,
    ) -> None:
        """Convert an SDRF file to mhcquant samplesheet and search presets."""
        sdrf = self.load_sdrf(sdrf_file)

        for col in ("source name", "comment[data file]"):
            if col not in sdrf.columns:
                raise ValueError(f"SDRF file is missing required column: '{col}'")

        defaults = load_default_presets(default_presets_file)
        factor_cols = self.get_factor_columns(sdrf)
        if not factor_cols:
            raise ValueError(
                "No factor value columns found in SDRF. "
                "At least one 'factor value[...]' column is required."
            )

        rows_data = []
        preset_params_map: dict[str, dict] = {}
        custom_presets_counter = 0

        for _, row in sdrf.iterrows():
            source_name = str(row["source name"])
            data_file = str(row["comment[data file]"])
            sample = self._get_sample_name(row, factor_cols, source_name)

            search_params = self._extract_search_params(row)
            preset_name, preset_dict = self._determine_preset(
                search_params, defaults, preset_params_map, custom_presets_counter
            )

            if preset_name not in preset_params_map:
                preset_params_map[preset_name] = preset_dict
                if preset_name.startswith("custom_"):
                    custom_presets_counter += 1

            rows_data.append({
                "Sample": sample,
                "Condition": 1,
                "ReplicateFileName": data_file,
                "SearchPreset": preset_name,
            })

        self._write_samplesheet(rows_data, output_samplesheet)
        self._write_presets(preset_params_map, output_presets)
        self.report_warnings()

    @staticmethod
    def _get_column_value(row: pd.Series, column_name: str) -> str | None:
        """Get a column value, returning None if missing or empty."""
        if column_name not in row.index:
            return None
        val = str(row[column_name])
        if val.lower() in _EMPTY_VALUES:
            return None
        return val

    @staticmethod
    def _get_sample_name(
        row: pd.Series, factor_cols: list[str], source_name: str
    ) -> str:
        """Extract sample name from factor value columns."""
        if factor_cols:
            val = str(row[factor_cols[0]])
            if val.lower() not in _EMPTY_VALUES:
                return val.strip().replace(" ", "_")
        return source_name.strip().replace(" ", "_")

    def _extract_search_params(self, row: pd.Series) -> dict:
        """Extract search-relevant parameters from an SDRF row."""
        params: dict = {}

        params["mhc_class"] = self._determine_mhc_class(row)

        tol_str = self._get_column_value(row, "comment[precursor mass tolerance]")
        if tol_str:
            tol_val, tol_unit = parse_tolerance(tol_str)
            if tol_val is not None:
                params["precursor_mass_tolerance"] = float(tol_val)
                params["precursor_error_unit"] = tol_unit

        frag_str = self._get_column_value(row, "comment[fragment mass tolerance]")
        if frag_str:
            frag_val, _ = parse_tolerance(frag_str)
            if frag_val is not None:
                params["fragment_mass_tolerance"] = float(frag_val)

        min_mz = self._get_column_value(row, "comment[ms min mz]")
        max_mz = self._get_column_value(row, "comment[ms max mz]")
        if min_mz and max_mz:
            params["precursor_mass_range"] = f"{self._strip_unit(min_mz)}:{self._strip_unit(max_mz)}"

        min_charge = self._get_column_value(row, "comment[ms min charge]")
        max_charge = self._get_column_value(row, "comment[ms max charge]")
        if min_charge and max_charge:
            params["precursor_charge"] = f"{self._strip_unit(min_charge)}:{self._strip_unit(max_charge)}"

        diss = self._get_column_value(row, "comment[dissociation method]")
        if diss:
            params["activation_method"] = self._extract_nt_value(diss).upper()

        instrument_str = self._get_column_value(row, "comment[instrument]")
        ms2_analyzer = self._get_column_value(row, "comment[ms2 mass analyzer]")
        if instrument_str:
            params["instrument_name"] = self._extract_nt_value(instrument_str)
        if ms2_analyzer:
            params["ms2_analyzer"] = self._extract_nt_value(ms2_analyzer)

        resolution = self._determine_resolution(
            params.get("instrument_name", ""),
            params.get("ms2_analyzer", ""),
            params.get("fragment_mass_tolerance"),
        )
        params["instrument_resolution"] = resolution
        if "fragment_mass_tolerance" in params:
            params["fragment_mass_tolerance"], params["fragment_bin_offset"] = (
                self._adjust_fragment_tolerance(resolution, params["fragment_mass_tolerance"])
            )

        params["ms2pip_model"] = self._determine_ms2pip_model(
            params.get("activation_method", ""),
            params.get("instrument_name", ""),
        )

        fixed_mods, variable_mods = self._extract_modifications(row)
        params["fixed_mods"] = fixed_mods
        params["variable_mods"] = variable_mods
        params["number_mods"] = max(3, len(variable_mods.split(",")) if variable_mods else 0)

        return params

    def _determine_mhc_class(self, row: pd.Series) -> str:
        """Determine MHC class from SDRF row."""
        val = self._get_column_value(row, "characteristics[mhc protein complex]")
        if not val:
            raise ValueError(
                "Missing 'characteristics[mhc protein complex]' column or value. "
                "MHC class cannot be determined."
            )
        return self._parse_mhc_class(val)

    @staticmethod
    def _parse_mhc_class(value: str) -> str:
        """Parse MHC class from a string value."""
        lower = value.lower().replace(" ", "")
        if "classii" in lower or "class2" in lower:
            return "class2"
        if "classi" in lower or "class1" in lower:
            return "class1"
        raise ValueError(f"Unrecognized MHC class '{value}'. Expected 'class I' or 'class II'.")

    @staticmethod
    def _strip_unit(value: str) -> str:
        """Strip unit suffix from a value like '300m/z' → '300'."""
        match = re.match(r"([\d.]+)", value.strip())
        return match.group(1) if match else value.strip()

    @staticmethod
    def _extract_nt_value(value: str) -> str:
        """Extract the NT= value from an SDRF field like 'NT=HCD;AC=MS:1000422'."""
        match = re.search(r"NT=([^;]+)", value)
        return match.group(1).strip() if match else value.strip()

    @staticmethod
    def _determine_resolution(
        instrument_name: str,
        ms2_analyzer: str,
        fragment_mass_tolerance: float | None = None,
    ) -> str:
        """Determine instrument resolution (high_res/low_res).

        Low-res is detected by any of:
        - MS2 analyzer is ion trap / linear trap
        - Instrument contains "xl" or "ltq" without orbitrap MS2 analyzer
        - Fragment mass tolerance >= 0.1 Da
        """
        lower_analyzer = ms2_analyzer.lower()
        lower_instrument = instrument_name.lower()

        if "ion trap" in lower_analyzer or "linear trap" in lower_analyzer:
            return "low_res"
        if ("xl" in lower_instrument or "ltq" in lower_instrument) and "orbitrap" not in lower_analyzer:
            return "low_res"
        if fragment_mass_tolerance is not None and fragment_mass_tolerance >= 0.1:
            return "low_res"
        return "high_res"

    @staticmethod
    def _adjust_fragment_tolerance(resolution: str, fragment_mass_tolerance: float) -> tuple[float, float]:
        """Adjust fragment tolerance and bin offset for Comet search engine.

        Low-res: Comet requires fixed 0.50025 Da / 0.4 offset for low-res
        ion trap searches (standard Comet low-res settings).
        High-res: Comet bins fragment ions at 2x the specified tolerance,
        so we halve the SDRF value to get the effective search window / 0.0 offset.
        """
        if resolution == "low_res":
            return 0.50025, 0.4
        return fragment_mass_tolerance / 2, 0.0

    def _determine_ms2pip_model(self, activation_method: str, instrument_name: str) -> str:
        """Determine MS2PIP model based on instrument and activation method.

        timsTOF → 'timsTOF', CID → 'CIDch2', HCD → 'Immuno-HCD', else warn + ''
        """
        lower_instrument = instrument_name.lower()
        if "timstof" in lower_instrument or "tims tof" in lower_instrument:
            return "timsTOF"

        upper_method = activation_method.upper()
        if upper_method == "CID":
            return "CIDch2"
        if upper_method == "HCD":
            return "Immuno-HCD"

        self.add_warning(
            f"Unknown activation method '{activation_method}' for MS2PIP model selection. "
            f"MS2PIP model left empty."
        )
        return ""

    def _extract_modifications(self, row: pd.Series) -> tuple[str, str]:
        """Extract fixed and variable modifications using shared ModificationConverter."""
        mod_columns = [c for c in row.index if c.startswith("comment[modification parameters]")]
        fixed_raw = []
        variable_raw = []

        for col in mod_columns:
            val = str(row[col])
            if val.lower() in _EMPTY_VALUES:
                continue
            if "mt=fixed" in val.lower():
                fixed_raw.append(val)
            else:
                variable_raw.append(val)

        fixed_str = self._mod_converter.openms_ify_mods(fixed_raw) if fixed_raw else ""
        variable_str = self._mod_converter.openms_ify_mods(variable_raw) if variable_raw else ""
        self.warnings.update(self._mod_converter.warnings)

        return fixed_str, variable_str

    def _determine_preset(
        self,
        search_params: dict,
        defaults: dict[str, dict],
        existing_presets: dict[str, dict],
        custom_counter: int,
    ) -> tuple[str, dict]:
        """Determine the preset name and dict for a row's search params.

        If all DDA fields are present in the SDRF, build a custom preset.
        Otherwise, fall back to the default preset for the device-HLA class.
        """
        custom_fields = (
            "precursor_mass_tolerance", "precursor_error_unit",
            "fragment_mass_tolerance", "precursor_mass_range",
            "precursor_charge", "activation_method",
        )
        can_build_custom = all(
            search_params.get(f) is not None for f in custom_fields
        )

        if not can_build_custom:
            return self._map_to_default_preset(search_params, defaults)

        preset_dict = self._build_custom_preset(search_params)
        match = self._find_matching_preset(preset_dict, existing_presets, defaults)
        if match:
            return match
        name = f"custom_{custom_counter + 1}"
        preset_dict["PresetName"] = name
        return name, preset_dict

    @staticmethod
    def _find_matching_preset(
        preset_dict: dict,
        existing_presets: dict[str, dict],
        defaults: dict[str, dict],
    ) -> tuple[str, dict] | None:
        """Find a matching preset in existing presets or defaults."""
        for name, existing in existing_presets.items():
            if MHCquant._presets_match(preset_dict, existing):
                return name, existing
        for name, default in defaults.items():
            if MHCquant._presets_match(preset_dict, default):
                return name, default
        return None

    @staticmethod
    def _build_custom_preset(params: dict) -> dict:
        """Build a custom preset dict from extracted search params."""
        min_len, max_len = MHC_CLASS_PEPTIDE_LENGTHS[params["mhc_class"]]
        return {
            "PresetName": "",
            "PeptideMinLength": min_len,
            "PeptideMaxLength": max_len,
            "PrecursorMassRange": params["precursor_mass_range"],
            "PrecursorCharge": params["precursor_charge"],
            "PrecursorMassTolerance": params["precursor_mass_tolerance"],
            "PrecursorErrorUnit": params["precursor_error_unit"],
            "FragmentMassTolerance": params["fragment_mass_tolerance"],
            "FragmentBinOffset": params["fragment_bin_offset"],
            "MS2PIPModel": params["ms2pip_model"],
            "ActivationMethod": params["activation_method"],
            "Instrument": params["instrument_resolution"],
            "NumberMods": params["number_mods"],
            "FixedMods": params["fixed_mods"],
            "VariableMods": params["variable_mods"],
        }

    @staticmethod
    def _normalize_preset_value(value: str) -> str:
        """Normalize a preset value for comparison."""
        value = value.strip()
        try:
            return str(float(value))
        except ValueError:
            return value

    @staticmethod
    def _presets_match(preset_a: dict, preset_b: dict) -> bool:
        """Check if two presets have the same parameter values (ignoring name)."""
        keys = [k for k in PRESET_COLUMNS if k != "PresetName"]
        return all(
            MHCquant._normalize_preset_value(str(preset_a.get(k, "")))
            == MHCquant._normalize_preset_value(str(preset_b.get(k, "")))
            for k in keys
        )

    def _map_to_default_preset(
        self, search_params: dict, defaults: dict[str, dict]
    ) -> tuple[str, dict]:
        """Map instrument name + MHC class to a default preset."""
        instrument = search_params.get("instrument_name", "").lower()
        mhc_class = search_params.get("mhc_class", "class1")

        prefix = "qe"
        for patterns, candidate in INSTRUMENT_PRESET_MAP:
            if any(p in instrument for p in patterns):
                prefix = candidate
                break
        else:
            self.add_warning(
                f"Unrecognized instrument '{search_params.get('instrument_name', '')}'. "
                f"Falling back to 'qe' preset."
            )

        name = f"{prefix}_{mhc_class}"
        if name in defaults:
            return name, defaults[name]

        self.add_warning(f"Default preset '{name}' not found. Using qe_class1.")
        return "qe_class1", defaults["qe_class1"]

    @staticmethod
    def _write_samplesheet(rows_data: list[dict], output_path: str) -> None:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("ID\tSample\tCondition\tReplicateFileName\tSearchPreset\n")
            for i, row in enumerate(rows_data, start=1):
                f.write(
                    f"{i}\t{row['Sample']}\t{row['Condition']}\t"
                    f"{row['ReplicateFileName']}\t{row['SearchPreset']}\n"
                )

    @staticmethod
    def _write_presets(presets: dict[str, dict], output_path: str) -> None:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\t".join(PRESET_COLUMNS) + "\n")
            for preset_dict in presets.values():
                values = []
                for col in PRESET_COLUMNS:
                    val = str(preset_dict.get(col, ""))
                    if col in ("FixedMods", "VariableMods") and val.strip() == "":
                        val = " "
                    values.append(val)
                f.write("\t".join(values) + "\n")
