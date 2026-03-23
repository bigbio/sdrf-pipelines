"""DIA-NN SDRF converter.

Converts SDRF files to DIA-NN configuration files:
- diann_config.cfg: DIA-NN command-line flags (enzyme, mods, channels, tolerances, scan ranges)
- diann_design.tsv: Per-file metadata (tolerances, labels, mods, scan ranges, experimental design)
"""

import logging
import re

import pandas as pd

from sdrf_pipelines.converters.base import BaseConverter, ConditionBuilder
from sdrf_pipelines.converters.diann.constants import ENZYME_NAME_MAPPINGS, ENZYME_SPECIFICITY
from sdrf_pipelines.converters.diann.modifications import DiannModificationConverter
from sdrf_pipelines.converters.diann.plexdia import (
    build_channels_flag,
    build_fixed_mod_flag,
    detect_plexdia_type,
)
from sdrf_pipelines.converters.openms.experimental_design import FractionGroupTracker
from sdrf_pipelines.converters.openms.utils import parse_tolerance

logger = logging.getLogger(__name__)

# Pattern for parsing m/z values like "400 m/z" or "400m/z"
_MZ_VALUE_PATTERN = re.compile(r"^(\d+(?:\.\d+)?)\s*m/z$", re.IGNORECASE)

# Pattern for parsing m/z range intervals like "400 m/z-1200 m/z" or "400m/z-1200m/z"
_MZ_RANGE_PATTERN = re.compile(r"^(\d+(?:\.\d+)?)\s*m/z\s*-\s*(\d+(?:\.\d+)?)\s*m/z$", re.IGNORECASE)

# Column names for scan range data (interval format)
_SCAN_RANGE_COLS = {
    "ms1": "comment[ms1 scan range]",
    "ms2": "comment[ms2 scan range]",
}

# Column names for discrete min/max m/z values
_DISCRETE_MZ_COLS = {
    "ms1": ("comment[ms min mz]", "comment[ms max mz]"),
    "ms2": ("comment[ms2 min mz]", "comment[ms2 max mz]"),
}


class DiaNN(BaseConverter):
    """Converts SDRF files to DIA-NN configuration format."""

    def __init__(self):
        super().__init__()
        self._mod_converter = DiannModificationConverter()

    def convert(self, sdrf_file: str, output_path: str, **kwargs) -> None:
        self.diann_convert(sdrf_file)

    def diann_convert(
        self, sdrf_file: str, mod_localization: str | None = None, diann_version: str | None = None
    ) -> None:
        """Convert SDRF to DIA-NN configuration files.

        Generates:
        - diann_config.cfg: DIA-NN CLI flags
        - diann_design.tsv: Per-file metadata

        Args:
            sdrf_file: Path to the SDRF file
            mod_localization: Comma-separated modifications for PTM site localization,
                e.g. 'Phospho (S),Phospho (T),Phospho (Y)' or 'UniMod:21'
            diann_version: DIA-NN version string (e.g. '1.8.1', '2.0'). Controls whether
                --monitor-mod flags are emitted. If None, defaults to emitting them.
        """
        sdrf = self.load_sdrf(sdrf_file)

        # Extract per-file data
        file_data = self._extract_file_data(sdrf)

        # Detect labeling strategy
        all_labels = set()
        for fd in file_data.values():
            all_labels.update(fd["labels"])
        plex_info = detect_plexdia_type(all_labels)

        # Get enzyme (must be consistent across experiment)
        enzymes = {fd["enzyme"] for fd in file_data.values()}
        if len(enzymes) > 1:
            raise ValueError(f"Multiple enzymes not supported: {enzymes}")
        enzyme = enzymes.pop()

        # Get modifications (must be consistent across experiment)
        fixed_mods_set = {tuple(fd["fixed_mods"]) for fd in file_data.values()}
        var_mods_set = {tuple(fd["var_mods"]) for fd in file_data.values()}
        if len(fixed_mods_set) > 1:
            raise ValueError("Inconsistent fixed modifications across files")
        if len(var_mods_set) > 1:
            raise ValueError("Inconsistent variable modifications across files")

        fixed_mods = list(fixed_mods_set.pop()) if fixed_mods_set else []
        var_mods = list(var_mods_set.pop()) if var_mods_set else []

        # Convert modifications to DIA-NN format
        diann_fixed, diann_var = self._mod_converter.convert_all_modifications(fixed_mods, var_mods)

        # Compute global min/max tolerances across all runs
        tolerance_summary = self._compute_global_tolerances(file_data)

        # Compute global scan ranges across all runs
        scan_range_summary = self._compute_global_scan_ranges(file_data)

        # Extract experimental design
        design_rows = self._extract_experimental_design(sdrf, file_data)

        # Resolve mod_localization to --monitor-mod flags (version-dependent)
        # DIA-NN 2.0+ handles localization automatically via --var-mod, no --monitor-mod needed
        monitor_mods: list[str] = []
        if mod_localization:
            needs_monitor_mod = True
            if diann_version:
                try:
                    major = int(diann_version.split(".")[0])
                    if major >= 2:
                        needs_monitor_mod = False
                        logger.info(
                            f"DIA-NN {diann_version}: PTM localization is automatic with --var-mod, "
                            "skipping --monitor-mod."
                        )
                except ValueError:
                    pass  # Unparseable version, default to emitting --monitor-mod
            if needs_monitor_mod:
                monitor_mods = self._resolve_monitor_mods(mod_localization)

        # Write config file
        self._write_config(
            enzyme, diann_fixed, diann_var, plex_info, tolerance_summary, scan_range_summary, monitor_mods
        )

        # Write filemap
        self._write_filemap(file_data, plex_info, design_rows)

        self.report_warnings()

    def _extract_file_data(self, sdrf: pd.DataFrame) -> dict:
        """Extract per-file metadata from SDRF rows.

        Returns:
            Dict mapping filename -> {labels, enzyme, fixed_mods, var_mods,
                                       precursor_tol, precursor_unit, fragment_tol, fragment_unit, uri}
        """
        file_data: dict[str, dict] = {}

        # Find modification columns
        mod_cols = [c for c in sdrf.columns if c.startswith("comment[modification parameters")]

        for _, row in sdrf.iterrows():
            raw = str(row.get("comment[data file]", "")).strip()
            if not raw:
                continue

            if raw not in file_data:
                file_data[raw] = {
                    "labels": [],
                    "enzyme": None,
                    "fixed_mods": [],
                    "var_mods": [],
                    "precursor_tol": None,
                    "precursor_unit": None,
                    "fragment_tol": None,
                    "fragment_unit": None,
                    "ms1_min_mz": None,
                    "ms1_max_mz": None,
                    "ms2_min_mz": None,
                    "ms2_max_mz": None,
                    "uri": "",
                }

            fd = file_data[raw]

            # Label
            label = self._extract_label(row)
            if label and label not in fd["labels"]:
                fd["labels"].append(label)

            # Enzyme (first row wins)
            if fd["enzyme"] is None:
                fd["enzyme"] = self._extract_enzyme(row)

            # Modifications (first row wins)
            if not fd["fixed_mods"] and not fd["var_mods"]:
                fixed, var = self._extract_modifications(row, mod_cols)
                fd["fixed_mods"] = fixed
                fd["var_mods"] = var

            # Tolerances (first row wins)
            if fd["precursor_tol"] is None:
                fd["precursor_tol"], fd["precursor_unit"] = self._extract_tolerance(
                    row, "comment[precursor mass tolerance]"
                )
            if fd["fragment_tol"] is None:
                fd["fragment_tol"], fd["fragment_unit"] = self._extract_tolerance(
                    row, "comment[fragment mass tolerance]"
                )

            # Scan ranges (first row wins)
            if fd["ms1_min_mz"] is None:
                ms1_min, ms1_max = self._extract_scan_range(row, "ms1")
                fd["ms1_min_mz"] = ms1_min
                fd["ms1_max_mz"] = ms1_max
            if fd["ms2_min_mz"] is None:
                ms2_min, ms2_max = self._extract_scan_range(row, "ms2")
                fd["ms2_min_mz"] = ms2_min
                fd["ms2_max_mz"] = ms2_max

            # URI
            if not fd["uri"]:
                uri_col = "comment[file uri]" if "comment[file uri]" in row.index else None
                if uri_col:
                    fd["uri"] = str(row[uri_col]).strip()

        return file_data

    def _compute_global_tolerances(self, file_data: dict) -> dict:
        """Compute global min and max precursor/fragment tolerances across all runs.

        For the in-silico library generation step, DIA-NN needs the broadest tolerance
        window (max) to cover all runs. Per-run analysis uses per-file values from the
        filemap. Only ppm tolerances are used since DIA-NN only supports ppm for
        --mass-acc and --mass-acc-ms1.

        Returns:
            Dict with keys: precursor_min, precursor_max, precursor_unit,
                           fragment_min, fragment_max, fragment_unit.
                           Values are None if no valid tolerances found or units are mixed.
        """
        result: dict[str, float | str | None] = {
            "precursor_min": None,
            "precursor_max": None,
            "precursor_unit": None,
            "fragment_min": None,
            "fragment_max": None,
            "fragment_unit": None,
        }

        precursor_vals = []
        precursor_units = set()
        fragment_vals = []
        fragment_units = set()

        for filename, fd in file_data.items():
            if fd["precursor_tol"] is not None and fd["precursor_unit"] is not None:
                try:
                    precursor_vals.append((float(fd["precursor_tol"]), fd["precursor_unit"]))
                    precursor_units.add(fd["precursor_unit"].lower())
                except ValueError:
                    self.add_warning(f"Invalid precursor tolerance value for {filename}: {fd['precursor_tol']}")

            if fd["fragment_tol"] is not None and fd["fragment_unit"] is not None:
                try:
                    fragment_vals.append((float(fd["fragment_tol"]), fd["fragment_unit"]))
                    fragment_units.add(fd["fragment_unit"].lower())
                except ValueError:
                    self.add_warning(f"Invalid fragment tolerance value for {filename}: {fd['fragment_tol']}")

        if precursor_vals:
            if len(precursor_units) > 1:
                self.add_warning(
                    f"Mixed precursor tolerance units across runs ({precursor_units}), cannot compute global min/max"
                )
            else:
                vals = [v for v, _ in precursor_vals]
                result["precursor_min"] = min(vals)
                result["precursor_max"] = max(vals)
                result["precursor_unit"] = precursor_vals[0][1]

        if fragment_vals:
            if len(fragment_units) > 1:
                self.add_warning(
                    f"Mixed fragment tolerance units across runs ({fragment_units}), cannot compute global min/max"
                )
            else:
                vals = [v for v, _ in fragment_vals]
                result["fragment_min"] = min(vals)
                result["fragment_max"] = max(vals)
                result["fragment_unit"] = fragment_vals[0][1]

        return result

    def _compute_global_scan_ranges(self, file_data: dict) -> dict:
        """Compute global min and max m/z scan ranges across all runs.

        For the in-silico library generation step, DIA-NN needs:
        - --min-pr-mz / --max-pr-mz: global min/max of MS1 scan ranges
        - --min-fr-mz / --max-fr-mz: global min/max of MS2 scan ranges

        The global minimum is the smallest lower bound across all runs,
        and the global maximum is the largest upper bound across all runs.

        Returns:
            Dict with keys: ms1_min, ms1_max, ms2_min, ms2_max.
            Values are None if no valid scan ranges found.
        """
        result: dict[str, float | None] = {"ms1_min": None, "ms1_max": None, "ms2_min": None, "ms2_max": None}

        for level in ("ms1", "ms2"):
            min_vals = []
            max_vals = []
            for fd in file_data.values():
                min_val = fd.get(f"{level}_min_mz")
                max_val = fd.get(f"{level}_max_mz")
                if min_val is not None:
                    min_vals.append(min_val)
                if max_val is not None:
                    max_vals.append(max_val)
            if min_vals:
                result[f"{level}_min"] = min(min_vals)
            if max_vals:
                result[f"{level}_max"] = max(max_vals)

        return result

    def _extract_label(self, row: pd.Series) -> str:
        """Extract label from comment[label] column."""
        if "comment[label]" not in row.index:
            return ""
        label_str = str(row["comment[label]"]).strip()

        # Try NT= format first
        nt_match = re.search(r"NT=(.+?)(;|$)", label_str)
        if nt_match:
            return nt_match.group(1).strip()

        return label_str

    def _extract_enzyme(self, row: pd.Series) -> str:
        """Extract enzyme from comment[cleavage agent details]."""
        if "comment[cleavage agent details]" not in row.index:
            raise ValueError("Missing comment[cleavage agent details] column")

        enzyme_str = str(row["comment[cleavage agent details]"]).strip()
        nt_match = re.search(r"NT=(.+?)(;|$)", enzyme_str)
        if nt_match:
            enzyme_name = nt_match.group(1).strip()
        else:
            enzyme_name = enzyme_str

        # Normalize
        normalized = ENZYME_NAME_MAPPINGS.get(enzyme_name.lower(), enzyme_name)
        return normalized

    def _extract_modifications(self, row: pd.Series, mod_cols: list[str]) -> tuple[list, list]:
        """Extract fixed and variable modifications from SDRF row."""
        fixed = []
        var = []
        for col in mod_cols:
            mod_str = str(row.get(col, "")).strip()
            if not mod_str or mod_str.lower() in ("nan", "not available", ""):
                continue
            # Normalize MT key-value to lowercase for consistent comparison
            normalized = re.sub(r"(?i)\bMT=\w+", lambda m: m.group().lower(), mod_str)
            mod_lower = normalized.lower()
            if "mt=fixed" in mod_lower:
                fixed.append(normalized)
            elif "mt=variable" in mod_lower:
                var.append(normalized)
        return fixed, var

    def _extract_tolerance(self, row: pd.Series, column: str) -> tuple:
        """Extract tolerance value and unit from an SDRF column."""
        if column not in row.index:
            return None, None
        tol_str = str(row[column]).strip()
        if not tol_str or tol_str.lower() in ("nan", "not available"):
            return None, None
        return parse_tolerance(tol_str)

    def _extract_scan_range(self, row: pd.Series, ms_level: str) -> tuple[float | None, float | None]:
        """Extract scan range (min/max m/z) for a given MS level.

        Supports two SDRF conventions:
        - Interval column: e.g. comment[ms1 scan range] = "400 m/z-1200 m/z"
        - Discrete columns: e.g. comment[ms min mz] = "400 m/z", comment[ms max mz] = "1200 m/z"

        If both are present, the interval column takes precedence and a warning is emitted.

        Returns:
            Tuple of (min_mz, max_mz) as floats, or (None, None) if not available.
        """
        range_col = _SCAN_RANGE_COLS.get(ms_level)
        discrete_cols = _DISCRETE_MZ_COLS.get(ms_level, (None, None))

        range_min, range_max = None, None
        discrete_min, discrete_max = None, None

        # Try interval column
        if range_col and range_col in row.index:
            val = str(row[range_col]).strip()
            if val and val.lower() not in ("nan", "not available"):
                match = _MZ_RANGE_PATTERN.match(val)
                if match:
                    range_min = float(match.group(1))
                    range_max = float(match.group(2))
                else:
                    self.add_warning(
                        f"Could not parse {ms_level} scan range interval: '{val}'. Expected format: '400 m/z-1200 m/z'"
                    )

        # Try discrete columns
        min_col, max_col = discrete_cols
        if min_col and min_col in row.index:
            val = str(row[min_col]).strip()
            if val and val.lower() not in ("nan", "not available"):
                match = _MZ_VALUE_PATTERN.match(val)
                if match:
                    discrete_min = float(match.group(1))
                else:
                    self.add_warning(f"Could not parse {ms_level} min m/z value: '{val}'. Expected format: '400 m/z'")
        if max_col and max_col in row.index:
            val = str(row[max_col]).strip()
            if val and val.lower() not in ("nan", "not available"):
                match = _MZ_VALUE_PATTERN.match(val)
                if match:
                    discrete_max = float(match.group(1))
                else:
                    self.add_warning(f"Could not parse {ms_level} max m/z value: '{val}'. Expected format: '1200 m/z'")

        # Resolve: range takes precedence over discrete
        if range_min is not None and range_max is not None:
            if range_min >= range_max:
                raise ValueError(
                    f"Inverted {ms_level} scan range: min ({range_min}) >= max ({range_max}). "
                    f"Check your SDRF annotation."
                )
            if discrete_min is not None or discrete_max is not None:
                self.add_warning(
                    f"Both interval ('{range_col}') and discrete min/max columns found for {ms_level}. "
                    "Using interval column values."
                )
            return range_min, range_max

        # Fall back to discrete
        min_mz, max_mz = discrete_min, discrete_max
        if min_mz is not None and max_mz is not None and min_mz >= max_mz:
            raise ValueError(
                f"Inverted {ms_level} scan range: min ({min_mz}) >= max ({max_mz}). Check your SDRF annotation."
            )
        return min_mz, max_mz

    @staticmethod
    def _extract_acquisition_method(row: pd.Series) -> str:
        col = "comment[proteomics data acquisition method]"
        if col in row.index:
            value = str(row[col]).strip()
            if value.lower() not in ("", "nan", "not available"):
                # Extract NT= value if present (e.g. "NT=Data-Independent Acquisition;AC=NCIT:C161786")
                if "NT=" in value:
                    nt_match = re.search(r"NT=([^;]+)", value)
                    if nt_match:
                        value = nt_match.group(1).strip()
                return value
        return ""

    @staticmethod
    def _extract_dissociation_method(row: pd.Series) -> str:
        col = "comment[dissociation method]"
        if col in row.index:
            value = str(row[col]).strip()
            if value.lower() not in ("", "nan", "not available"):
                # Extract NT= value if present (e.g. "NT=HCD;AC=PRIDE:0000590" -> "HCD")
                if "NT=" in value:
                    nt_match = re.search(r"NT=([^;]+)", value)
                    if nt_match:
                        value = nt_match.group(1).strip()
                mapping = {
                    "collision-induced dissociation": "CID",
                    "beam-type collision-induced dissociation": "HCD",
                    "higher energy beam-type collision-induced dissociation": "HCD",
                    "electron transfer dissociation": "ETD",
                    "electron capture dissociation": "ECD",
                }
                return mapping.get(value.lower(), value)
        return ""

    def _extract_experimental_design(self, sdrf: pd.DataFrame, file_data: dict) -> list[dict]:
        """Extract experimental design metadata from SDRF.

        Returns a list of dicts, one per SDRF row. For plexDIA, each channel row
        produces its own entry with its own Condition/BioReplicate.
        """
        factor_cols = [c for c in sdrf.columns if c.startswith("factor value[")]
        condition_builder = ConditionBuilder(factor_cols)
        fraction_tracker = FractionGroupTracker()

        source_name_list: list[str] = []
        source_name2n_reps: dict[str, int] = {}
        for _, row in sdrf.iterrows():
            sn = str(row["source name"])
            tech_rep = str(row.get("comment[technical replicate]", "1"))
            if tech_rep.lower() in ("", "nan", "not available"):
                tech_rep = "1"
            if sn not in source_name_list:
                source_name_list.append(sn)
                source_name2n_reps[sn] = int(tech_rep)
            else:
                source_name2n_reps[sn] = max(source_name2n_reps[sn], int(tech_rep))

        source_to_sample: dict[str, int] = {}
        source_to_biorep: dict[str, int] = {}
        for i, sn in enumerate(source_name_list, start=1):
            source_to_sample[sn] = i
            source_to_biorep[sn] = i

        design_rows: list[dict] = []
        seen_files: set[str] = set()

        for _, row in sdrf.iterrows():
            filename = str(row["comment[data file]"])
            sn = str(row["source name"])
            tech_rep = str(row.get("comment[technical replicate]", "1"))
            if tech_rep.lower() in ("", "nan", "not available"):
                tech_rep = "1"

            if filename not in seen_files:
                seen_files.add(filename)
                fraction = self.get_fraction_identifier(row)
                source_idx = source_name_list.index(sn)
                offset = sum(source_name2n_reps[source_name_list[i]] for i in range(source_idx))
                raw_frac_group = offset + int(tech_rep)
                frac_group = fraction_tracker.get_fraction_group(filename, raw_frac_group)
            else:
                fraction = self.get_fraction_identifier(row)
                frac_group = fraction_tracker.fraction_groups[filename]

            condition = condition_builder.add_from_row(row, fallback=sn)

            design_rows.append(
                {
                    "filename": filename,
                    "label": self._extract_label(row),
                    "sample": source_to_sample[sn],
                    "fraction_group": frac_group,
                    "fraction": int(fraction),
                    "condition": condition,
                    "bioreplicate": source_to_biorep[sn],
                    "acquisition_method": self._extract_acquisition_method(row),
                    "dissociation_method": self._extract_dissociation_method(row),
                }
            )

        return design_rows

    def _resolve_monitor_mods(self, mod_localization: str) -> list[str]:
        """Resolve mod_localization string to unique UniMod accessions for --monitor-mod.

        Accepts comma-separated modification names (e.g. 'Phospho (S),Phospho (T)')
        or UniMod accessions (e.g. 'UniMod:21'). Names are mapped via the UniMod database.
        Site annotations in parentheses are stripped before lookup.

        Returns:
            List of unique UniMod accession strings, e.g. ['UniMod:21', 'UniMod:1']
        """
        unimod_ids: list[str] = []
        for mod in mod_localization.split(","):
            mod = mod.strip()
            if not mod:
                continue
            # Direct UniMod accession — normalize to uppercase UNIMOD:
            if mod.lower().startswith("unimod:"):
                normalized = "UNIMOD:" + mod.split(":", 1)[1]
                if normalized not in unimod_ids:
                    unimod_ids.append(normalized)
                continue
            # Strip site annotation: "Phospho (S)" -> "Phospho"
            base_name = re.sub(r"\s*\(.*\)", "", mod).strip()
            # Look up in the modification converter's UniMod database
            unimod_id = self._mod_converter.find_unimod_by_name(base_name)
            if unimod_id and unimod_id not in unimod_ids:
                unimod_ids.append(unimod_id)
            elif not unimod_id:
                self.add_warning(
                    f"Could not resolve '{mod}' to a UniMod accession for --monitor-mod. "
                    "Use 'UniMod:XX' format directly if the name is not recognized."
                )
        return unimod_ids

    def _write_config(
        self,
        enzyme: str,
        fixed_mods: list[str],
        var_mods: list[str],
        plex_info: dict | None,
        tolerance_summary: dict | None = None,
        scan_range_summary: dict | None = None,
        monitor_mods: list[str] | None = None,
    ) -> None:
        """Write diann_config.cfg."""
        parts = []

        # Enzyme cut rule
        cut_rule = ENZYME_SPECIFICITY.get(enzyme)
        if cut_rule:
            parts.append(f"--cut {cut_rule}")
        else:
            self.add_warning(f"Unknown enzyme '{enzyme}', no --cut rule generated")

        # Standard fixed modifications
        for mod in fixed_mods:
            parts.append(f"--fixed-mod {mod}")

        # plexDIA modifications
        if plex_info is not None:
            plexdia_fixed = build_fixed_mod_flag(plex_info)
            parts.append(f"--fixed-mod {plexdia_fixed}")

        # Variable modifications
        for mod in var_mods:
            parts.append(f"--var-mod {mod}")

        # plexDIA channel flags
        if plex_info is not None:
            channels = build_channels_flag(plex_info)
            parts.append(f"--channels {channels}")

            from sdrf_pipelines.converters.diann.constants import PLEXDIA_REGISTRY

            mod_name = PLEXDIA_REGISTRY[plex_info["type"]]["fixed_mod"]["name"]
            parts.append(f"--lib-fixed-mod {mod_name}")
            parts.append("--original-mods")

        # Global mass accuracy tolerances (max across all runs for in-silico library generation)
        if tolerance_summary:
            precursor_max = tolerance_summary.get("precursor_max")
            precursor_unit = tolerance_summary.get("precursor_unit")
            fragment_max = tolerance_summary.get("fragment_max")
            fragment_unit = tolerance_summary.get("fragment_unit")

            if precursor_max is not None and precursor_unit is not None:
                if precursor_unit.lower() == "ppm":
                    parts.append(f"--mass-acc-ms1 {precursor_max}")
                else:
                    logger.warning(
                        "DIA-NN only supports ppm for --mass-acc-ms1. "
                        f"Skipping precursor tolerance ({precursor_max} {precursor_unit})."
                    )

            if fragment_max is not None and fragment_unit is not None:
                if fragment_unit.lower() == "ppm":
                    parts.append(f"--mass-acc {fragment_max}")
                else:
                    logger.warning(
                        "DIA-NN only supports ppm for --mass-acc. "
                        f"Skipping fragment tolerance ({fragment_max} {fragment_unit})."
                    )

        # Global scan range flags for in-silico library generation
        if scan_range_summary:
            for key, flag in [
                ("ms1_min", "--min-pr-mz"),
                ("ms1_max", "--max-pr-mz"),
                ("ms2_min", "--min-fr-mz"),
                ("ms2_max", "--max-fr-mz"),
            ]:
                val = scan_range_summary.get(key)
                if val is not None:
                    parts.append(f"{flag} {val}")

        # PTM site localization (--monitor-mod)
        if monitor_mods:
            for mod_ac in monitor_mods:
                parts.append(f"--monitor-mod {mod_ac}")

        with open("diann_config.cfg", "w", encoding="utf-8") as f:
            f.write(" ".join(parts))

    def _write_filemap(self, file_data: dict, plex_info: dict | None, design_rows: list[dict] | None = None) -> None:
        """Write diann_design.tsv (unified design file)."""
        rows = []
        label_type = plex_info["type"] if plex_info else "label free"

        design_lookup: dict[tuple[str, str], dict] = {}
        if design_rows:
            for d in design_rows:
                design_lookup[(d["filename"], d["label"])] = d

        for filename, fd in file_data.items():
            if plex_info is not None:
                for label in fd["labels"]:
                    design = design_lookup.get((filename, label))
                    rows.append(self._filemap_row(filename, fd, label, label_type, design))
            else:
                label = fd["labels"][0] if fd["labels"] else "label free sample"
                design = design_lookup.get((filename, label))
                rows.append(self._filemap_row(filename, fd, label, label_type, design))

        df = pd.DataFrame(rows)
        df.to_csv("diann_design.tsv", sep="\t", index=False, encoding="utf-8")

    def _filemap_row(self, filename: str, fd: dict, label: str, label_type: str, design: dict | None = None) -> dict:
        """Build a single design file row."""
        return {
            "Filename": filename,
            "URI": fd.get("uri", ""),
            "Sample": design["sample"] if design else "",
            "FractionGroup": design["fraction_group"] if design else "",
            "Fraction": design["fraction"] if design else 1,
            "Label": label,
            "LabelType": label_type,
            "AcquisitionMethod": design["acquisition_method"] if design else "",
            "DissociationMethod": design["dissociation_method"] if design else "",
            "Condition": design["condition"] if design else "",
            "BioReplicate": design["bioreplicate"] if design else "",
            "Enzyme": fd["enzyme"],
            "FixedModifications": ";".join(fd["fixed_mods"]),
            "VariableModifications": ";".join(fd["var_mods"]),
            "PrecursorMassTolerance": fd["precursor_tol"] or "",
            "PrecursorMassToleranceUnit": fd["precursor_unit"] or "",
            "FragmentMassTolerance": fd["fragment_tol"] or "",
            "FragmentMassToleranceUnit": fd["fragment_unit"] or "",
            "MS1MinMz": fd.get("ms1_min_mz") if fd.get("ms1_min_mz") is not None else "",
            "MS1MaxMz": fd.get("ms1_max_mz") if fd.get("ms1_max_mz") is not None else "",
            "MS2MinMz": fd.get("ms2_min_mz") if fd.get("ms2_min_mz") is not None else "",
            "MS2MaxMz": fd.get("ms2_max_mz") if fd.get("ms2_max_mz") is not None else "",
        }
