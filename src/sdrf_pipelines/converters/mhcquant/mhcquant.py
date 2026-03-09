"""MHCquant SDRF converter.

Converts SDRF files to nf-core/mhcquant samplesheet and search presets files.
"""

import re

import pandas as pd

from sdrf_pipelines.converters.mhcquant.constants import (
    COLUMN_ALIASES,
    DEFAULT_PRESETS,
    INSTRUMENT_PRESET_MAP,
    MHC_CLASS_PEPTIDE_LENGTHS,
    PRESET_COLUMNS,
    REQUIRED_PRESET_FIELDS,
)


class MHCquant:
    """Converter from SDRF to nf-core/mhcquant samplesheet and search presets."""

    def __init__(self):
        self.warnings: dict[str, int] = {}

    def convert(
        self,
        sdrf_file: str,
        output_samplesheet: str = "mhcquant_samplesheet.tsv",
        output_presets: str = "search_presets.tsv",
        raw_file_prefix: str = "",
        default_presets_file: str | None = None,
    ) -> None:
        """Convert an SDRF file to mhcquant samplesheet and search presets.

        Args:
            sdrf_file: Path to the input SDRF file.
            output_samplesheet: Output path for the samplesheet TSV.
            output_presets: Output path for the search presets TSV.
            raw_file_prefix: Optional prefix for raw file paths.
            default_presets_file: Optional path to a custom default presets TSV.
        """
        sdrf = self._load_sdrf(sdrf_file)

        # Load default presets (built-in or user-provided)
        defaults = self._load_default_presets(default_presets_file)

        # Get factor value columns
        factor_cols = [c for c in sdrf.columns if c.startswith("factor value[")]
        if not factor_cols:
            self._add_warning("No factor value columns found. Using source name as Sample.")

        # Build condition mapping: unique source name → auto-increment integer
        condition_map = self._build_condition_map(sdrf)

        # Process each row: extract search params and determine preset
        rows_data = []
        preset_params_map: dict[str, dict] = {}  # preset_name → preset dict
        custom_counter = 0

        for _, row in sdrf.iterrows():
            # Extract sample info
            source_name = str(row.get("source name", ""))
            if not source_name or source_name.lower() in ("nan", ""):
                raise ValueError("Missing 'source name' column in SDRF.")

            data_file = self._resolve_column(row, sdrf.columns, "comment[data file]")
            if not data_file:
                raise ValueError("Missing 'comment[data file]' column in SDRF.")

            # Sample name from factor value
            sample = self._get_sample_name(row, factor_cols, source_name)

            # Condition from source name
            condition = condition_map[source_name]

            # File path
            replicate_file = raw_file_prefix + data_file if raw_file_prefix else data_file

            # Extract search params from SDRF row
            search_params = self._extract_search_params(row, sdrf.columns)

            # Determine preset name
            preset_name, preset_dict = self._determine_preset(
                search_params, defaults, preset_params_map, custom_counter
            )

            # Store preset if not already stored
            if preset_name not in preset_params_map:
                preset_params_map[preset_name] = preset_dict
                if preset_name.startswith("custom_"):
                    custom_counter += 1

            rows_data.append({
                "Sample": sample,
                "Condition": condition,
                "ReplicateFileName": replicate_file,
                "SearchPreset": preset_name,
            })

        # Write outputs
        self._write_samplesheet(rows_data, output_samplesheet)
        self._write_presets(preset_params_map, output_presets)
        self._report_warnings()

    def _load_sdrf(self, sdrf_file: str) -> pd.DataFrame:
        """Load and preprocess SDRF file."""
        sdrf = pd.read_csv(sdrf_file, sep="\t")
        sdrf = sdrf.astype(str)
        sdrf.columns = sdrf.columns.str.lower()

        # Validate required columns
        if "source name" not in sdrf.columns:
            raise ValueError("SDRF file is missing required column: 'source name'")

        has_data_file = any(
            c for c in sdrf.columns
            if c in ("comment[data file]",)
        )
        if not has_data_file:
            raise ValueError("SDRF file is missing required column: 'comment[data file]'")

        return sdrf

    def _load_default_presets(self, presets_file: str | None) -> dict[str, dict]:
        """Load default presets from file or use built-in defaults."""
        if presets_file is None:
            return dict(DEFAULT_PRESETS)

        df = pd.read_csv(presets_file, sep="\t")
        presets = {}
        for _, row in df.iterrows():
            name = str(row["PresetName"])
            presets[name] = {col: row[col] for col in PRESET_COLUMNS if col in row.index}
        return presets

    def _resolve_column(
        self, row: pd.Series, columns: pd.Index, primary_name: str
    ) -> str | None:
        """Resolve a column value, trying primary name then aliases."""
        # Direct match
        if primary_name in columns:
            val = str(row[primary_name])
            if val.lower() not in ("nan", "", "not available"):
                return val

        # Check aliases
        alias = COLUMN_ALIASES.get(primary_name)
        if alias and alias in columns:
            val = str(row[alias])
            if val.lower() not in ("nan", "", "not available"):
                return val

        # Reverse alias check (if primary_name is an old name)
        for new_name, old_name in COLUMN_ALIASES.items():
            if old_name == primary_name and new_name in columns:
                val = str(row[new_name])
                if val.lower() not in ("nan", "", "not available"):
                    return val

        return None

    def _get_sample_name(
        self, row: pd.Series, factor_cols: list[str], source_name: str
    ) -> str:
        """Extract sample name from factor value columns."""
        if factor_cols:
            # Use first factor value column
            val = str(row[factor_cols[0]])
            if val.lower() not in ("nan", "", "not available"):
                return self._sanitize_name(val)

        # Fallback to source name
        return self._sanitize_name(source_name)

    def _sanitize_name(self, name: str) -> str:
        """Sanitize a name for use as sample identifier (spaces → underscores)."""
        return name.strip().replace(" ", "_")

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

    def _extract_search_params(self, row: pd.Series, columns: pd.Index) -> dict:
        """Extract search-relevant parameters from an SDRF row."""
        params: dict = {}

        # MHC class
        mhc_class = self._determine_mhc_class(row, columns)
        params["mhc_class"] = mhc_class

        # Precursor mass tolerance
        tol_str = self._resolve_column(row, columns, "comment[precursor mass tolerance]")
        if tol_str:
            tol_val, tol_unit = self._parse_tolerance(tol_str)
            params["precursor_mass_tolerance"] = tol_val
            params["precursor_error_unit"] = tol_unit

        # Fragment mass tolerance
        frag_str = self._resolve_column(row, columns, "comment[fragment mass tolerance]")
        if frag_str:
            frag_val, _ = self._parse_tolerance(frag_str)
            params["fragment_mass_tolerance"] = frag_val

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

        # Instrument and MS2 analyzer → resolution
        instrument_str = self._resolve_column(row, columns, "comment[instrument]")
        ms2_analyzer = self._resolve_column(row, columns, "comment[ms2 mass analyzer]")
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

        # For low-res: override fragment tolerance and bin offset to mhcquant's
        # required values. Using anything else produces bad results.
        if resolution == "low_res":
            params["fragment_mass_tolerance"] = 0.50025
            params["fragment_bin_offset"] = 0.4
        else:
            params["fragment_bin_offset"] = 0.0

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

    def _determine_mhc_class(self, row: pd.Series, columns: pd.Index) -> str:
        """Determine MHC class from SDRF row."""
        # Check characteristics[mhc class]
        mhc_val = self._resolve_column(row, columns, "characteristics[mhc class]")
        if mhc_val:
            return self._parse_mhc_class(mhc_val)

        # Check characteristics[mhc protein complex]
        mhc_complex = self._resolve_column(row, columns, "characteristics[mhc protein complex]")
        if mhc_complex:
            return self._parse_mhc_class(mhc_complex)

        self._add_warning("Missing MHC class information. Defaulting to class I.")
        return "class1"

    def _parse_mhc_class(self, value: str) -> str:
        """Parse MHC class from a string value."""
        lower = value.lower()
        if "class ii" in lower or "class 2" in lower:
            return "class2"
        if "class i" in lower or "class 1" in lower:
            return "class1"
        self._add_warning(f"Unrecognized MHC class '{value}'. Defaulting to class I.")
        return "class1"

    def _parse_tolerance(self, tol_str: str) -> tuple[float, str]:
        """Parse a tolerance string like '10 ppm' or '0.01 Da'."""
        match = re.match(r"([\d.]+)\s*(ppm|da)", tol_str.strip(), re.IGNORECASE)
        if match:
            return float(match.group(1)), match.group(2).lower()
        # Try just a number
        try:
            return float(tol_str.strip()), "da"
        except ValueError:
            self._add_warning(f"Could not parse tolerance: '{tol_str}'. Using 0.")
            return 0.0, "da"

    def _strip_unit(self, value: str) -> str:
        """Strip unit suffix from a value like '300m/z' → '300'."""
        match = re.match(r"([\d.]+)", value.strip())
        return match.group(1) if match else value.strip()

    def _extract_nt_value(self, value: str) -> str:
        """Extract the NT= value from an SDRF field like 'NT=HCD;AC=MS:1000422'."""
        match = re.search(r"NT=([^;]+)", value)
        return match.group(1).strip() if match else value.strip()

    def _determine_resolution(
        self,
        instrument_name: str,
        ms2_analyzer: str,
        fragment_mass_tolerance: float | None = None,
    ) -> str:
        """Determine instrument resolution (high_res/low_res).

        Low-res is detected by any of:
        - MS2 analyzer is ion trap / linear trap
        - Instrument name contains "xl" (LTQ Orbitrap XL) without orbitrap MS2
        - Fragment mass tolerance >= 0.1 Da (indicates low-res detector)
        """
        lower_analyzer = ms2_analyzer.lower()
        lower_instrument = instrument_name.lower()

        # Ion trap / linear trap MS2 → low_res
        if "ion trap" in lower_analyzer or "linear trap" in lower_analyzer:
            return "low_res"

        # Orbitrap XL with non-orbitrap MS2 → low_res
        if ("orbitrap xl" in lower_instrument or "ltq" in lower_instrument) and "orbitrap" not in lower_analyzer:
            return "low_res"

        # High fragment mass tolerance indicates low-res MS2
        if fragment_mass_tolerance is not None and fragment_mass_tolerance >= 0.1:
            return "low_res"

        return "high_res"

    def _determine_ms2pip_model(
        self, activation_method: str, instrument_name: str, ms2_analyzer: str
    ) -> str:
        """Determine MS2PIP model from dissociation method and instrument."""
        lower_instrument = instrument_name.lower()
        lower_analyzer = ms2_analyzer.lower()
        upper_activation = activation_method.upper()

        # timsTOF instruments
        if "timstof" in lower_instrument or "tims tof" in lower_instrument:
            return "timsTOF"

        # CID with low-res (ion trap) MS2
        if upper_activation == "CID" and (
            "ion trap" in lower_analyzer or "linear trap" in lower_analyzer
        ):
            return "CIDch2"

        # HCD with orbitrap MS2 (default for high-res)
        return "Immuno-HCD"

    def _extract_modifications(
        self, row: pd.Series, columns: pd.Index
    ) -> tuple[str, str]:
        """Extract fixed and variable modifications from SDRF row.

        Returns:
            Tuple of (fixed_mods_string, variable_mods_string).
        """
        mod_columns = [c for c in columns if c.startswith("comment[modification parameters]")]
        fixed_mods = []
        variable_mods = []

        for col in mod_columns:
            val = str(row[col])
            if val.lower() in ("nan", "", "not available"):
                continue
            mod_type = self._extract_field(val, "MT")
            mod_name = self._extract_nt_value(val)
            target_aa = self._extract_field(val, "TA")
            position = self._extract_field(val, "PP")

            if not mod_name:
                continue

            # Format modification name with residue info
            mod_label = self._format_mod_label(mod_name, target_aa, position)

            if mod_type and mod_type.lower() == "fixed":
                fixed_mods.append(mod_label)
            else:
                variable_mods.append(mod_label)

        return ", ".join(fixed_mods), ", ".join(variable_mods)

    def _format_mod_label(self, mod_name: str, target_aa: str | None, position: str | None) -> str:
        """Format a modification label like 'Oxidation (M)' or 'Acetyl (Protein N-term)'."""
        if target_aa and target_aa.lower() not in ("nan", "", "not available"):
            return f"{mod_name} ({target_aa})"
        if position and position.lower() not in ("nan", "", "not available", "anywhere"):
            return f"{mod_name} ({position})"
        return mod_name

    def _extract_field(self, value: str, field: str) -> str | None:
        """Extract a field value from a semicolon-separated SDRF value.

        E.g., extract 'MT' from 'NT=Oxidation;MT=Variable;TA=M;AC=UNIMOD:35'
        """
        match = re.search(rf"{field}=([^;]+)", value)
        return match.group(1).strip() if match else None

    def _determine_preset(
        self,
        search_params: dict,
        defaults: dict[str, dict],
        existing_presets: dict[str, dict],
        custom_counter: int,
    ) -> tuple[str, dict]:
        """Determine the preset name and dict for a row's search params.

        Returns:
            Tuple of (preset_name, preset_dict).
        """
        # Check if all required fields are present for a custom preset
        has_all_required = all(
            search_params.get(field) is not None
            for field in REQUIRED_PRESET_FIELDS
        )

        if has_all_required:
            # Build custom preset
            preset_dict = self._build_custom_preset(search_params, "")
            # Check if this matches an existing preset
            for name, existing in existing_presets.items():
                if self._presets_match(preset_dict, existing):
                    return name, existing

            # Check if it matches a built-in default
            for name, default in defaults.items():
                if self._presets_match(preset_dict, default):
                    return name, default

            # Create new custom preset
            preset_name = f"custom_{custom_counter + 1}"
            preset_dict["PresetName"] = preset_name
            return preset_name, preset_dict

        # Fall back to default preset based on instrument + MHC class
        return self._map_to_default_preset(search_params, defaults)

    def _build_custom_preset(self, params: dict, preset_name: str) -> dict:
        """Build a custom preset dict from extracted search params."""
        mhc_class = params.get("mhc_class", "class1")
        min_len, max_len = MHC_CLASS_PEPTIDE_LENGTHS.get(mhc_class, (8, 14))

        return {
            "PresetName": preset_name,
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

    def _presets_match(self, preset_a: dict, preset_b: dict) -> bool:
        """Check if two presets have the same parameter values (ignoring name)."""
        keys = [k for k in PRESET_COLUMNS if k != "PresetName"]
        for key in keys:
            val_a = preset_a.get(key, "")
            val_b = preset_b.get(key, "")
            # Compare as strings to handle numeric/string mismatches
            if str(val_a) != str(val_b):
                return False
        return True

    def _map_to_default_preset(
        self, search_params: dict, defaults: dict[str, dict]
    ) -> tuple[str, dict]:
        """Map instrument name + MHC class to a default preset."""
        instrument_name = search_params.get("instrument_name", "").lower()
        mhc_class = search_params.get("mhc_class", "class1")

        # Find instrument prefix
        prefix = None
        for patterns, preset_prefix in INSTRUMENT_PRESET_MAP:
            if any(p in instrument_name for p in patterns):
                prefix = preset_prefix
                break

        if prefix is None:
            self._add_warning(
                f"Unrecognized instrument '{search_params.get('instrument_name', '')}'. "
                f"Falling back to 'qe' preset."
            )
            prefix = "qe"

        preset_name = f"{prefix}_{mhc_class}"
        if preset_name in defaults:
            return preset_name, defaults[preset_name]

        # Should not happen with built-in defaults, but handle gracefully
        self._add_warning(f"Default preset '{preset_name}' not found. Using qe_class1.")
        return "qe_class1", defaults.get("qe_class1", DEFAULT_PRESETS["qe_class1"])

    def _write_samplesheet(self, rows_data: list[dict], output_path: str) -> None:
        """Write the mhcquant samplesheet TSV."""
        with open(output_path, "w") as f:
            f.write("ID\tSample\tCondition\tReplicateFileName\tSearchPreset\n")
            for i, row in enumerate(rows_data, start=1):
                f.write(
                    f"{i}\t{row['Sample']}\t{row['Condition']}\t"
                    f"{row['ReplicateFileName']}\t{row['SearchPreset']}\n"
                )

    def _write_presets(self, presets: dict[str, dict], output_path: str) -> None:
        """Write the search presets TSV."""
        with open(output_path, "w") as f:
            f.write("\t".join(PRESET_COLUMNS) + "\n")
            for preset_dict in presets.values():
                values = [str(preset_dict.get(col, "")) for col in PRESET_COLUMNS]
                f.write("\t".join(values) + "\n")

    def _add_warning(self, message: str) -> None:
        """Add or increment a warning message."""
        self.warnings[message] = self.warnings.get(message, 0) + 1

    def _report_warnings(self) -> None:
        """Print all accumulated warnings."""
        for message, count in self.warnings.items():
            print(f'WARNING: "{message}" occurred {count} times.')
