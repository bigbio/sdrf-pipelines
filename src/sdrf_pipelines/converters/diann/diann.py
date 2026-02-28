"""DIA-NN SDRF converter.

Converts SDRF files to DIA-NN configuration files:
- diann_config.cfg: DIA-NN command-line flags (enzyme, mods, channels)
- diann_filemap.tsv: Per-file metadata (tolerances, labels, mods)
"""

import re

import pandas as pd

from sdrf_pipelines.converters.base import BaseConverter
from sdrf_pipelines.converters.diann.constants import ENZYME_NAME_MAPPINGS, ENZYME_SPECIFICITY
from sdrf_pipelines.converters.diann.modifications import DiannModificationConverter
from sdrf_pipelines.converters.diann.plexdia import (
    build_channels_flag,
    build_fixed_mod_flag,
    detect_plexdia_type,
)
from sdrf_pipelines.converters.openms.utils import parse_tolerance


class DiaNN(BaseConverter):
    """Converts SDRF files to DIA-NN configuration format."""

    def __init__(self):
        super().__init__()
        self._mod_converter = DiannModificationConverter()

    def convert(self, sdrf_file: str, output_path: str, **kwargs) -> None:
        self.diann_convert(sdrf_file)

    def diann_convert(self, sdrf_file: str) -> None:
        """Convert SDRF to DIA-NN configuration files.

        Generates:
        - diann_config.cfg: DIA-NN CLI flags
        - diann_filemap.tsv: Per-file metadata

        Args:
            sdrf_file: Path to the SDRF file
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

        # Write config file
        self._write_config(enzyme, diann_fixed, diann_var, plex_info)

        # Write filemap
        self._write_filemap(file_data, plex_info)

        self.report_warnings()

    def _extract_file_data(self, sdrf: pd.DataFrame) -> dict:
        """Extract per-file metadata from SDRF rows.

        Returns:
            Dict mapping filename -> {labels, enzyme, fixed_mods, var_mods,
                                       precursor_tol, precursor_unit, fragment_tol, fragment_unit, uri}
        """
        file_data = {}

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

            # URI
            if not fd["uri"]:
                uri_col = "comment[file uri]" if "comment[file uri]" in row.index else None
                if uri_col:
                    fd["uri"] = str(row[uri_col]).strip()

        return file_data

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
            if "MT=fixed" in mod_str or "mt=fixed" in mod_str:
                fixed.append(mod_str)
            elif "MT=variable" in mod_str or "mt=variable" in mod_str:
                var.append(mod_str)
        return fixed, var

    def _extract_tolerance(self, row: pd.Series, column: str) -> tuple:
        """Extract tolerance value and unit from an SDRF column."""
        if column not in row.index:
            return None, None
        tol_str = str(row[column]).strip()
        if not tol_str or tol_str.lower() in ("nan", "not available"):
            return None, None
        return parse_tolerance(tol_str)

    def _write_config(
        self,
        enzyme: str,
        fixed_mods: list[str],
        var_mods: list[str],
        plex_info: dict | None,
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

        with open("diann_config.cfg", "w") as f:
            f.write(" ".join(parts))

    def _write_filemap(self, file_data: dict, plex_info: dict | None) -> None:
        """Write diann_filemap.tsv."""
        rows = []
        label_type = plex_info["type"] if plex_info else "label free"

        for filename, fd in file_data.items():
            if plex_info is not None:
                # For plexDIA: one row per channel per file
                for label in fd["labels"]:
                    rows.append(self._filemap_row(filename, fd, label, label_type))
            else:
                # Label-free: one row per file
                label = fd["labels"][0] if fd["labels"] else "label free sample"
                rows.append(self._filemap_row(filename, fd, label, label_type))

        df = pd.DataFrame(rows)
        df.to_csv("diann_filemap.tsv", sep="\t", index=False)

    def _filemap_row(self, filename: str, fd: dict, label: str, label_type: str) -> dict:
        """Build a single filemap row."""
        return {
            "Filename": filename,
            "URI": fd.get("uri", ""),
            "Label": label,
            "LabelType": label_type,
            "Enzyme": fd["enzyme"],
            "FixedModifications": ";".join(fd["fixed_mods"]),
            "VariableModifications": ";".join(fd["var_mods"]),
            "PrecursorMassTolerance": fd["precursor_tol"] or "",
            "PrecursorMassToleranceUnit": fd["precursor_unit"] or "",
            "FragmentMassTolerance": fd["fragment_tol"] or "",
            "FragmentMassToleranceUnit": fd["fragment_unit"] or "",
        }
