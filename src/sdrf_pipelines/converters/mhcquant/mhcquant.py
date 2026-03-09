"""MHCquant SDRF converter.

Converts SDRF files to nf-core/mhcquant samplesheet and search presets files.
"""

import re

import pandas as pd

from sdrf_pipelines.converters.base import BaseConverter
from sdrf_pipelines.converters.mhcquant.constants import (
    COLUMN_ALIASES,
    INSTRUMENT_PRESET_MAP,
    MHC_CLASS_PEPTIDE_LENGTHS,
    PRESET_COLUMNS,
    REQUIRED_PRESET_FIELDS,
    load_default_presets,
)
from sdrf_pipelines.converters.openms.modifications import ModificationConverter
from sdrf_pipelines.converters.openms.utils import parse_tolerance


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
        raw_file_prefix: str = "",
        default_presets_file: str | None = None,
    ) -> None:
        """Convert an SDRF file to mhcquant samplesheet and search presets."""
        sdrf = self.load_sdrf(sdrf_file)

        # Validate required columns
        for col in ("source name", "comment[data file]"):
            if col not in sdrf.columns:
                raise ValueError(f"SDRF file is missing required column: '{col}'")

        defaults = load_default_presets(default_presets_file)
        factor_cols = self.get_factor_columns(sdrf)
        if not factor_cols:
            self.add_warning("No factor value columns found. Using source name as Sample.")

        condition_map = self._build_condition_map(sdrf)

        rows_data = []
        preset_params_map: dict[str, dict] = {}
        custom_counter = 0

        for _, row in sdrf.iterrows():
            source_name = str(row["source name"])
            data_file = str(row["comment[data file]"])
            sample = self._get_sample_name(row, factor_cols, source_name)
            replicate_file = raw_file_prefix + data_file if raw_file_prefix else data_file

            search_params = self._extract_search_params(row, sdrf.columns)
            preset_name, preset_dict = self._determine_preset(
                search_params, defaults, preset_params_map, custom_counter
            )

            if preset_name not in preset_params_map:
                preset_params_map[preset_name] = preset_dict
                if preset_name.startswith("custom_"):
                    custom_counter += 1

            rows_data.append({
                "Sample": sample,
                "Condition": condition_map[source_name],
                "ReplicateFileName": replicate_file,
                "SearchPreset": preset_name,
            })

        self._write_samplesheet(rows_data, output_samplesheet)
        self._write_presets(preset_params_map, output_presets)
        self.report_warnings()

    # --- Column resolution ---

    def _resolve_column(
        self, row: pd.Series, columns: pd.Index, primary_name: str
    ) -> str | None:
        """Resolve a column value, trying primary name then aliases."""
        for name in self._column_candidates(primary_name):
            if name in columns:
                val = str(row[name])
                if val.lower() not in ("nan", "", "not available"):
                    return val
        return None

    @staticmethod
    def _column_candidates(primary_name: str) -> list[str]:
        """Return candidate column names: primary, forward alias, reverse alias."""
        candidates = [primary_name]
        alias = COLUMN_ALIASES.get(primary_name)
        if alias:
            candidates.append(alias)
        for new_name, old_name in COLUMN_ALIASES.items():
            if old_name == primary_name:
                candidates.append(new_name)
        return candidates

    # --- Sample / condition helpers ---

    def _get_sample_name(
        self, row: pd.Series, factor_cols: list[str], source_name: str
    ) -> str:
        """Extract sample name from factor value columns."""
        if factor_cols:
            val = str(row[factor_cols[0]])
            if val.lower() not in ("nan", "", "not available"):
                return val.strip().replace(" ", "_")
        return source_name.strip().replace(" ", "_")

    def _build_condition_map(self, sdrf: pd.DataFrame) -> dict[str, int]:
        """Build mapping of unique source names to auto-incremented condition integers."""
        condition_map: dict[str, int] = {}
        counter = 1
        for source_name in sdrf["source name"]:
            source_name = str(source_name)
            if source_name not in condition_map:
                condition_map[source_name] = counter
                counter += 1
        return condition_map

    # --- Search parameter extraction ---

    def _extract_search_params(self, row: pd.Series, columns: pd.Index) -> dict:
        """Extract search-relevant parameters from an SDRF row."""
        params: dict = {}

        # MHC class
        params["mhc_class"] = self._determine_mhc_class(row, columns)

        # Precursor mass tolerance
        tol_str = self._resolve_column(row, columns, "comment[precursor mass tolerance]")
        if tol_str:
            tol_val, tol_unit = parse_tolerance(tol_str)
            if tol_val is not None:
                params["precursor_mass_tolerance"] = float(tol_val)
                params["precursor_error_unit"] = tol_unit

        # Fragment mass tolerance
        frag_str = self._resolve_column(row, columns, "comment[fragment mass tolerance]")
        if frag_str:
            frag_val, _ = parse_tolerance(frag_str)
            if frag_val is not None:
                params["fragment_mass_tolerance"] = float(frag_val)

        # Precursor mz range
        min_mz = self._resolve_column(row, columns, "comment[ms min mz]")
        max_mz = self._resolve_column(row, columns, "comment[ms max mz]")
        if min_mz and max_mz:
            params["precursor_mass_range"] = f"{self._strip_unit(min_mz)}:{self._strip_unit(max_mz)}"

        # Precursor charge range
        min_charge = self._resolve_column(row, columns, "comment[ms min charge]")
        max_charge = self._resolve_column(row, columns, "comment[ms max charge]")
        if min_charge and max_charge:
            params["precursor_charge"] = f"{self._strip_unit(min_charge)}:{self._strip_unit(max_charge)}"

        # Dissociation method
        diss = self._resolve_column(row, columns, "comment[dissociation method]")
        if diss:
            params["activation_method"] = self._extract_nt_value(diss).upper()

        # Instrument and MS2 analyzer
        instrument_str = self._resolve_column(row, columns, "comment[instrument]")
        ms2_analyzer = self._resolve_column(row, columns, "comment[ms2 mass analyzer]")
        if instrument_str:
            params["instrument_name"] = self._extract_nt_value(instrument_str)
        if ms2_analyzer:
            params["ms2_analyzer"] = self._extract_nt_value(ms2_analyzer)

        # Resolution and fragment tolerance adjustment
        resolution = self._determine_resolution(
            params.get("instrument_name", ""),
            params.get("ms2_analyzer", ""),
            params.get("fragment_mass_tolerance"),
        )
        params["instrument_resolution"] = resolution
        params["fragment_mass_tolerance"], params["fragment_bin_offset"] = (
            self._adjust_fragment_tolerance(resolution, params.get("fragment_mass_tolerance"))
        )

        # MS2PIP model
        params["ms2pip_model"] = self._determine_ms2pip_model(
            params.get("activation_method", ""),
            params.get("instrument_name", ""),
            params.get("ms2_analyzer", ""),
        )

        # Modifications
        fixed_mods, variable_mods = self._extract_modifications(row, columns)
        params["fixed_mods"] = fixed_mods
        params["variable_mods"] = variable_mods
        params["number_mods"] = max(3, len(variable_mods.split(",")) if variable_mods else 0)

        return params

    # --- MHC class ---

    def _determine_mhc_class(self, row: pd.Series, columns: pd.Index) -> str:
        """Determine MHC class from SDRF row."""
        for col in ("characteristics[mhc class]", "characteristics[mhc protein complex]"):
            val = self._resolve_column(row, columns, col)
            if val:
                return self._parse_mhc_class(val)
        self.add_warning("Missing MHC class information. Defaulting to class I.")
        return "class1"

    def _parse_mhc_class(self, value: str) -> str:
        """Parse MHC class from a string value."""
        lower = value.lower()
        if "class ii" in lower or "class 2" in lower:
            return "class2"
        if "class i" in lower or "class 1" in lower:
            return "class1"
        self.add_warning(f"Unrecognized MHC class '{value}'. Defaulting to class I.")
        return "class1"

    # --- Value parsing helpers ---

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

    # --- Instrument resolution ---

    @staticmethod
    def _determine_resolution(
        instrument_name: str,
        ms2_analyzer: str,
        fragment_mass_tolerance: float | None = None,
    ) -> str:
        """Determine instrument resolution (high_res/low_res).

        Low-res is detected by any of:
        - MS2 analyzer is ion trap / linear trap
        - Instrument contains "xl"/"ltq" without orbitrap MS2
        - Fragment mass tolerance >= 0.1 Da
        """
        lower_analyzer = ms2_analyzer.lower()
        lower_instrument = instrument_name.lower()

        if "ion trap" in lower_analyzer or "linear trap" in lower_analyzer:
            return "low_res"
        if ("orbitrap xl" in lower_instrument or "ltq" in lower_instrument) and "orbitrap" not in lower_analyzer:
            return "low_res"
        if fragment_mass_tolerance is not None and fragment_mass_tolerance >= 0.1:
            return "low_res"
        return "high_res"

    @staticmethod
    def _adjust_fragment_tolerance(
        resolution: str, fragment_mass_tolerance: float | None
    ) -> tuple[float, float]:
        """Adjust fragment tolerance and bin offset for Comet search engine.

        Low-res: forced to 0.50025 Da / 0.4 offset (required by mhcquant).
        High-res: halved due to Comet tolerance binning / 0.0 offset.
        """
        if resolution == "low_res":
            return 0.50025, 0.4
        if fragment_mass_tolerance is not None:
            return fragment_mass_tolerance / 2, 0.0
        return 0.01, 0.0

    # --- MS2PIP model ---

    @staticmethod
    def _determine_ms2pip_model(
        activation_method: str, instrument_name: str, ms2_analyzer: str
    ) -> str:
        """Determine MS2PIP model: timsTOF → 'timsTOF', CID+ion trap → 'CIDch2', else 'Immuno-HCD'."""
        lower_instrument = instrument_name.lower()
        lower_analyzer = ms2_analyzer.lower()

        if "timstof" in lower_instrument or "tims tof" in lower_instrument:
            return "timsTOF"
        if activation_method.upper() == "CID" and (
            "ion trap" in lower_analyzer or "linear trap" in lower_analyzer
        ):
            return "CIDch2"
        return "Immuno-HCD"

    # --- Modifications ---

    def _extract_modifications(
        self, row: pd.Series, columns: pd.Index
    ) -> tuple[str, str]:
        """Extract fixed and variable modifications using shared ModificationConverter."""
        mod_columns = [c for c in columns if c.startswith("comment[modification parameters]")]
        fixed_raw = []
        variable_raw = []

        for col in mod_columns:
            val = str(row[col])
            if val.lower() in ("nan", "", "not available"):
                continue
            if "mt=fixed" in val.lower():
                fixed_raw.append(val)
            else:
                variable_raw.append(val)

        fixed_str = self._mod_converter.openms_ify_mods(fixed_raw) if fixed_raw else ""
        variable_str = self._mod_converter.openms_ify_mods(variable_raw) if variable_raw else ""
        self.warnings.update(self._mod_converter.warnings)

        return fixed_str, variable_str

    # --- Preset determination ---

    def _determine_preset(
        self,
        search_params: dict,
        defaults: dict[str, dict],
        existing_presets: dict[str, dict],
        custom_counter: int,
    ) -> tuple[str, dict]:
        """Determine the preset name and dict for a row's search params."""
        has_all_required = all(
            search_params.get(field) is not None
            for field in REQUIRED_PRESET_FIELDS
        )

        if has_all_required:
            preset_dict = self._build_custom_preset(search_params)
            match = self._find_matching_preset(preset_dict, existing_presets, defaults)
            if match:
                return match
            preset_name = f"custom_{custom_counter + 1}"
            preset_dict["PresetName"] = preset_name
            return preset_name, preset_dict

        # Fall back to default preset based on instrument + MHC class,
        # but always override modifications from the SDRF.
        # Keep the default's NumberMods (class-appropriate).
        preset_name, preset_dict = self._map_to_default_preset(search_params, defaults)
        preset_dict = dict(preset_dict)
        preset_dict["FixedMods"] = search_params.get("fixed_mods", "")
        preset_dict["VariableMods"] = search_params.get("variable_mods", "Oxidation (M)")

        # Check mapped default first, then existing/other defaults
        if self._presets_match(preset_dict, defaults[preset_name]):
            return preset_name, defaults[preset_name]
        match = self._find_matching_preset(preset_dict, existing_presets, defaults)
        if match:
            return match

        new_name = f"custom_{custom_counter + 1}"
        preset_dict["PresetName"] = new_name
        return new_name, preset_dict

    def _find_matching_preset(
        self,
        preset_dict: dict,
        existing_presets: dict[str, dict],
        defaults: dict[str, dict],
    ) -> tuple[str, dict] | None:
        """Find a matching preset in existing presets or defaults."""
        for name, existing in existing_presets.items():
            if self._presets_match(preset_dict, existing):
                return name, existing
        for name, default in defaults.items():
            if self._presets_match(preset_dict, default):
                return name, default
        return None

    def _build_custom_preset(self, params: dict) -> dict:
        """Build a custom preset dict from extracted search params."""
        mhc_class = params.get("mhc_class", "class1")
        min_len, max_len = MHC_CLASS_PEPTIDE_LENGTHS.get(mhc_class, (8, 14))

        return {
            "PresetName": "",
            "PeptideMinLength": min_len,
            "PeptideMaxLength": max_len,
            "PrecursorMassRange": params.get("precursor_mass_range", "800:2500"),
            "PrecursorCharge": params.get("precursor_charge", "2:3"),
            "PrecursorMassTolerance": params.get("precursor_mass_tolerance", 5),
            "PrecursorErrorUnit": params.get("precursor_error_unit", "ppm"),
            "FragmentMassTolerance": params.get("fragment_mass_tolerance", 0.01),
            "FragmentBinOffset": params.get("fragment_bin_offset", 0.0),
            "MS2PIPModel": params.get("ms2pip_model", "Immuno-HCD"),
            "ActivationMethod": params.get("activation_method", "HCD"),
            "Instrument": params.get("instrument_resolution", "high_res"),
            "NumberMods": params.get("number_mods", 3),
            "FixedMods": params.get("fixed_mods", ""),
            "VariableMods": params.get("variable_mods", "Oxidation (M)"),
        }

    @staticmethod
    def _presets_match(preset_a: dict, preset_b: dict) -> bool:
        """Check if two presets have the same parameter values (ignoring name)."""
        keys = [k for k in PRESET_COLUMNS if k != "PresetName"]
        return all(
            str(preset_a.get(k, "")).strip() == str(preset_b.get(k, "")).strip()
            for k in keys
        )

    def _map_to_default_preset(
        self, search_params: dict, defaults: dict[str, dict]
    ) -> tuple[str, dict]:
        """Map instrument name + MHC class to a default preset."""
        instrument_name = search_params.get("instrument_name", "").lower()
        mhc_class = search_params.get("mhc_class", "class1")

        prefix = None
        for patterns, preset_prefix in INSTRUMENT_PRESET_MAP:
            if any(p in instrument_name for p in patterns):
                prefix = preset_prefix
                break

        if prefix is None:
            self.add_warning(
                f"Unrecognized instrument '{search_params.get('instrument_name', '')}'. "
                f"Falling back to 'qe' preset."
            )
            prefix = "qe"

        preset_name = f"{prefix}_{mhc_class}"
        if preset_name in defaults:
            return preset_name, defaults[preset_name]

        self.add_warning(f"Default preset '{preset_name}' not found. Using qe_class1.")
        return "qe_class1", defaults["qe_class1"]

    # --- Output writers ---

    @staticmethod
    def _write_samplesheet(rows_data: list[dict], output_path: str) -> None:
        """Write the mhcquant samplesheet TSV."""
        with open(output_path, "w") as f:
            f.write("ID\tSample\tCondition\tReplicateFileName\tSearchPreset\n")
            for i, row in enumerate(rows_data, start=1):
                f.write(
                    f"{i}\t{row['Sample']}\t{row['Condition']}\t"
                    f"{row['ReplicateFileName']}\t{row['SearchPreset']}\n"
                )

    @staticmethod
    def _write_presets(presets: dict[str, dict], output_path: str) -> None:
        """Write the search presets TSV."""
        with open(output_path, "w") as f:
            f.write("\t".join(PRESET_COLUMNS) + "\n")
            for preset_dict in presets.values():
                values = []
                for col in PRESET_COLUMNS:
                    val = str(preset_dict.get(col, ""))
                    if col in ("FixedMods", "VariableMods") and val.strip() == "":
                        val = " "
                    values.append(val)
                f.write("\t".join(values) + "\n")
