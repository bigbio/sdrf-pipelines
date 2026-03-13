"""MHCquant SDRF converter.

Converts SDRF files to nf-core/mhcquant samplesheet and search presets files.
"""

import pandas as pd

from sdrf_pipelines.converters.base import BaseConverter
from sdrf_pipelines.converters.mhcquant.constants import (
    EMPTY_VALUES,
    INSTRUMENT_PRESET_MAP,
    load_default_presets,
)
from sdrf_pipelines.converters.mhcquant.utils import (
    SearchParams,
    extract_nt_value,
    find_matching_preset,
    get_column_value,
    get_sample_name,
    overlay_params_on_preset,
    parse_mhc_class,
    ppm_to_da,
    resolve_fragment_tolerance,
    strip_unit,
    write_presets,
)
from sdrf_pipelines.converters.openms.modifications import ModificationConverter
from sdrf_pipelines.converters.openms.utils import parse_tolerance


class MHCquant(BaseConverter):
    """Converter from SDRF to nf-core/mhcquant samplesheet and search presets."""

    def __init__(self):
        super().__init__()
        self._mod_converter = ModificationConverter()

    def convert(self, sdrf_file: str, output_path: str = "mhcquant_samplesheet.tsv", **kwargs) -> None:
        """Convert an SDRF file to mhcquant samplesheet and search presets."""
        output_samplesheet = output_path
        output_presets: str = kwargs.get("output_presets", "search_presets.tsv")
        default_presets_file: str | None = kwargs.get("default_presets_file")

        sdrf = self.load_sdrf(sdrf_file)

        for col in ("source name", "comment[data file]"):
            if col not in sdrf.columns:
                raise ValueError(f"SDRF file is missing required column: '{col}'")

        defaults = load_default_presets(default_presets_file)
        factor_cols = self.get_factor_columns(sdrf)
        if not factor_cols:
            raise ValueError(
                "No factor value columns found in SDRF. At least one 'factor value[...]' column is required."
            )

        rows_data = []
        preset_params_map: dict[str, dict] = {}
        custom_presets_counter = 0

        for _, row in sdrf.iterrows():
            source_name = str(row["source name"])
            data_file = str(row["comment[data file]"])
            sample = get_sample_name(row, factor_cols, source_name)

            search_params = self._extract_search_params(row)
            preset_name, preset_dict = self._determine_preset(
                search_params, defaults, preset_params_map, custom_presets_counter
            )

            if preset_name not in preset_params_map:
                preset_params_map[preset_name] = preset_dict
                if preset_name.startswith("custom_"):
                    custom_presets_counter += 1

            rows_data.append(
                {
                    "Sample": sample,
                    "Condition": 1,
                    "ReplicateFileName": data_file,
                    "SearchPreset": preset_name,
                }
            )

        # Write samplesheet
        df = pd.DataFrame(rows_data)
        df.insert(0, "ID", range(1, len(df) + 1))
        df.to_csv(output_samplesheet, sep="\t", index=False)
        # Write presets
        write_presets(preset_params_map, output_presets)
        self.report_warnings()

    def _extract_search_params(self, row: pd.Series) -> SearchParams:
        """Extract search-relevant parameters from an SDRF row."""
        params = SearchParams()

        params["mhc_class"] = self._determine_mhc_class(row)

        tol_str = get_column_value(row, "comment[precursor mass tolerance]")
        if tol_str:
            tol_val, tol_unit = parse_tolerance(tol_str)
            if tol_val is not None and tol_unit is not None:
                params["precursor_mass_tolerance"] = float(tol_val)
                params["precursor_error_unit"] = tol_unit

        frag_str = get_column_value(row, "comment[fragment mass tolerance]")
        if frag_str:
            frag_val, frag_unit = parse_tolerance(frag_str)
            if frag_val is not None:
                frag_val_float = float(frag_val)
                if frag_unit == "ppm":
                    frag_val_float = ppm_to_da(frag_val_float)
                    self.add_warning(
                        f"Fragment mass tolerance '{frag_str.strip()}' is in ppm. "
                        f"Converted to {frag_val_float:.4f} Da using reference mass 1000.0 Da. "
                        "MHCquant only supports Da for fragment mass tolerance."
                    )
                params["fragment_mass_tolerance"] = frag_val_float

        min_mz = get_column_value(row, "comment[ms min mz]")
        max_mz = get_column_value(row, "comment[ms max mz]")
        min_charge = get_column_value(row, "comment[ms min charge]")
        max_charge = get_column_value(row, "comment[ms max charge]")

        if min_charge and max_charge:
            params["precursor_charge"] = f"{strip_unit(min_charge)}:{strip_unit(max_charge)}"

        if min_mz and max_mz:
            min_mz_val = float(strip_unit(min_mz))
            max_mz_val = float(strip_unit(max_mz))
            if min_charge and max_charge:
                min_charge_val = int(strip_unit(min_charge))
                max_charge_val = int(strip_unit(max_charge))
                params["precursor_mass_range"] = (
                    f"{int(min_mz_val * min_charge_val)}:{int(max_mz_val * max_charge_val)}"
                )
            else:
                params["precursor_mass_range"] = f"{int(min_mz_val)}:{int(max_mz_val)}"

        diss = get_column_value(row, "comment[dissociation method]")
        if diss:
            params["activation_method"] = extract_nt_value(diss).upper()

        instrument_str = get_column_value(row, "comment[instrument]")
        ms2_analyzer = get_column_value(row, "comment[ms2 mass analyzer]")
        if instrument_str:
            params["instrument_name"] = extract_nt_value(instrument_str)
        if ms2_analyzer:
            params["ms2_analyzer"] = extract_nt_value(ms2_analyzer)

        if "fragment_mass_tolerance" in params:
            resolution, adjusted_tol, bin_offset = resolve_fragment_tolerance(
                params.get("instrument_name", ""),
                params.get("ms2_analyzer", ""),
                params["fragment_mass_tolerance"],
            )
            params["instrument_resolution"] = resolution
            params["fragment_mass_tolerance"] = adjusted_tol
            params["fragment_bin_offset"] = bin_offset
        else:
            params["instrument_resolution"] = "high_res"

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
        val = get_column_value(row, "characteristics[mhc protein complex]")
        if not val:
            raise ValueError(
                "Missing 'characteristics[mhc protein complex]' column or value. MHC class cannot be determined."
            )
        return parse_mhc_class(val)

    def _determine_ms2pip_model(self, activation_method: str, instrument_name: str) -> str:
        """Determine MS2PIP model based on instrument and activation method.

        timsTOF -> 'timsTOF', CID -> 'CIDch2', HCD -> 'Immuno-HCD', else warn + ''
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
            f"Unknown activation method '{activation_method}' for MS2PIP model selection. MS2PIP model left empty."
        )
        return ""

    def _extract_modifications(self, row: pd.Series) -> tuple[str, str]:
        """Extract fixed and variable modifications using shared ModificationConverter."""
        mod_columns = [c for c in row.index if c.startswith("comment[modification parameters]")]
        fixed_raw = []
        variable_raw = []

        for col in mod_columns:
            val = str(row[col])
            if val.lower() in EMPTY_VALUES:
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
        search_params: SearchParams,
        defaults: dict[str, dict],
        existing_presets: dict[str, dict],
        custom_counter: int,
    ) -> tuple[str, dict]:
        """Determine the preset name and dict for a row's search params.

        Starts from the default preset for the instrument/MHC class, then
        overlays any SDRF-extracted values. If the result matches an existing
        preset, reuse it; otherwise create a custom preset.
        """
        default_name, default_preset = self._map_to_default_preset(search_params, defaults)
        preset_dict = overlay_params_on_preset(default_preset, search_params)

        # If nothing changed from the default, reuse it
        match = find_matching_preset(preset_dict, {default_name: default_preset}, {})
        if match:
            return match

        # Check if it matches any other existing or default preset
        match = find_matching_preset(preset_dict, existing_presets, defaults)
        if match:
            return match

        name = f"custom_{custom_counter + 1}"
        preset_dict["PresetName"] = name
        return name, preset_dict

    def _map_to_default_preset(self, search_params: SearchParams, defaults: dict[str, dict]) -> tuple[str, dict]:
        """Map instrument name + MHC class to a default preset."""
        instrument = search_params.get("instrument_name", "").lower()
        mhc_class = search_params.get("mhc_class", "class1")

        for patterns, candidate in INSTRUMENT_PRESET_MAP:
            if any(p in instrument for p in patterns):
                prefix = candidate
                break
        else:
            raise ValueError(
                f"Unrecognized instrument '{search_params.get('instrument_name', '')}'. "
                "Cannot determine default search preset. "
                "Please provide full DDA columns in the SDRF or use a known instrument."
            )

        name = f"{prefix}_{mhc_class}"
        if name in defaults:
            return name, defaults[name]

        raise ValueError(f"Default preset '{name}' not found in presets file.")
