"""Relink SDRF converter.

Converts SDRF files to relink pipeline configuration:
- relink_design.tsv: Per-file metadata (filename, sample, fraction, replicate)
- Engine-specific config files:
  - xiSEARCH: xi_linear.conf + xi_crosslinking.conf
  - Scout: search_params.json + filter_params.json
"""

import json
import logging
import re

import pandas as pd

from sdrf_pipelines.converters.base import BaseConverter
from sdrf_pipelines.converters.openms.utils import parse_tolerance

logger = logging.getLogger(__name__)

# Regex for parsing key=value pairs in SDRF cells (e.g., NT=DSSO;AC=XLMOD:02010)
_KV_PATTERN = re.compile(r"(\w+)=([^;]+)")


def _parse_sdrf_cell(cell: str) -> dict[str, str]:
    """Parse semicolon-separated key=value pairs from an SDRF cell."""
    return dict(_KV_PATTERN.findall(str(cell))) if pd.notna(cell) else {}


class Relink(BaseConverter):
    """Converts SDRF files to relink pipeline configuration format."""

    def __init__(self):
        super().__init__()

    def convert(self, sdrf_file: str, output_path: str, **kwargs) -> None:
        self.relink_convert(sdrf_file, engine=kwargs.get("engine", "xisearch"))

    def relink_convert(self, sdrf_file: str, engine: str = "xisearch") -> None:
        """Convert SDRF to relink configuration files.

        Generates:
        - relink_design.tsv: Per-file metadata for Nextflow channel creation
        - Engine-specific config files (xiSEARCH or Scout)

        Args:
            sdrf_file: Path to the SDRF file
            engine: Search engine ('xisearch' or 'scout')
        """
        sdrf = self.load_sdrf(sdrf_file)

        # Extract per-file data
        file_data = self._extract_file_data(sdrf)

        # Extract global parameters (from first row — assumed consistent)
        global_params = self._extract_global_params(sdrf)

        # Write per-file design
        self._write_design(file_data)

        # Write engine-specific config
        if engine == "xisearch":
            self._write_xisearch_config(global_params)
        elif engine == "scout":
            self._write_scout_config(global_params)
        else:
            raise ValueError(f"Unknown engine: {engine}. Use 'xisearch' or 'scout'.")

        # Report warnings
        for msg, count in self.warnings.items():
            logger.warning(f"{msg} (occurred {count} times)")

    def _extract_file_data(self, sdrf: pd.DataFrame) -> list[dict]:
        """Extract per-file metadata from SDRF rows."""
        rows = []
        for _, row in sdrf.iterrows():
            entry = {
                "filename": row.get("comment[data file]", ""),
                "sample_id": row.get("source name", ""),
                "fraction": row.get("comment[fraction identifier]", "1"),
                "technical_replicate": row.get("comment[technical replicate]", "1"),
            }
            rows.append(entry)
        return rows

    def _extract_global_params(self, sdrf: pd.DataFrame) -> dict:
        """Extract global search parameters from the SDRF (first row).

        Assumes all rows share the same enzyme, mods, tolerances, and crosslinker.
        """
        row = sdrf.iloc[0]
        params = {}

        # Enzymes
        enzyme_cols = [c for c in sdrf.columns if c.startswith("comment[cleavage agent details]")]
        enzymes = []
        for ec in enzyme_cols:
            parsed = _parse_sdrf_cell(row.get(ec, ""))
            if "NT" in parsed:
                enzymes.append(parsed["NT"])
        params["enzymes"] = enzymes if enzymes else ["Trypsin"]

        # Modifications
        mod_cols = [c for c in sdrf.columns if c.startswith("comment[modification parameters]")]
        fixed_mods = []
        variable_mods = []
        for mc in mod_cols:
            val = str(row.get(mc, ""))
            parsed = _parse_sdrf_cell(val)
            mt = parsed.get("MT", "").lower()
            name = parsed.get("NT", "")
            site = parsed.get("TA", "")
            ac = parsed.get("AC", "")
            if not name:
                continue
            mod_entry = {"name": name, "site": site, "accession": ac}
            if mt == "fixed":
                fixed_mods.append(mod_entry)
            elif mt == "variable":
                variable_mods.append(mod_entry)
        params["fixed_modifications"] = fixed_mods
        params["variable_modifications"] = variable_mods

        # Tolerances
        prec_tol_str = row.get("comment[precursor mass tolerance]", "10 ppm")
        frag_tol_str = row.get("comment[fragment mass tolerance]", "20 ppm")
        prec_val, prec_unit = parse_tolerance(str(prec_tol_str))
        frag_val, frag_unit = parse_tolerance(str(frag_tol_str))
        params["precursor_tolerance"] = {"value": prec_val or "10", "unit": prec_unit or "ppm"}
        params["fragment_tolerance"] = {"value": frag_val or "20", "unit": frag_unit or "ppm"}

        # Dissociation method
        diss_parsed = _parse_sdrf_cell(row.get("comment[dissociation method]", ""))
        params["dissociation_method"] = diss_parsed.get("NT", "HCD")

        # Collision energy
        params["collision_energy"] = row.get("comment[collision energy]", "")

        # Crosslinker
        cl_parsed = _parse_sdrf_cell(row.get("comment[cross-linker]", ""))
        params["crosslinker"] = {
            "name": cl_parsed.get("NT", ""),
            "accession": cl_parsed.get("AC", ""),
            "sites": cl_parsed.get("TA", ""),
            "mass_heavy": cl_parsed.get("MH", ""),
            "mass_light": cl_parsed.get("ML", ""),
            "cleavable": cl_parsed.get("CL", ""),
        }

        # Other XL-MS columns
        params["experiment_type"] = row.get("comment[chemical cross-linking coupled with ms]", "")
        params["enrichment_method"] = row.get("comment[crosslink enrichment method]", "")

        return params

    def _write_design(self, file_data: list[dict]) -> None:
        """Write relink_design.tsv — per-file metadata for Nextflow channels."""
        df = pd.DataFrame(file_data)
        df.to_csv("relink_design.tsv", sep="\t", index=False)
        logger.info(f"Wrote relink_design.tsv with {len(df)} entries")

    def _write_xisearch_config(self, params: dict) -> None:
        """Write xiSEARCH config files (xi_linear.conf + xi_crosslinking.conf)."""
        prec = params["precursor_tolerance"]
        frag = params["fragment_tolerance"]
        cl = params["crosslinker"]

        # Build shared config lines
        lines = [
            f"tolerance:precursor:{prec['value']}{prec['unit']}",
            f"tolerance:fragment:{frag['value']}{frag['unit']}",
        ]
        for enzyme in params["enzymes"]:
            lines.append(f"digestion:{enzyme}")
        for mod in params["fixed_modifications"]:
            lines.append(f"modification:fixed:{mod['name']} ({mod['site']})")
        for mod in params["variable_modifications"]:
            lines.append(f"modification:variable:{mod['name']} ({mod['site']})")

        # Linear config (no crosslinker)
        with open("xi_linear.conf", "w", encoding="utf-8") as f:
            f.write("## xiSEARCH linear config (auto-generated from SDRF)\n")
            f.write("\n".join(lines) + "\n")
        logger.info("Wrote xi_linear.conf")

        # Crosslinking config (adds crosslinker)
        cl_lines = list(lines)
        if cl["name"]:
            cl_lines.append(f"crosslinker:name:{cl['name']}")
        if cl["sites"]:
            cl_lines.append(f"crosslinker:sites:{cl['sites']}")
        if cl["mass_heavy"]:
            cl_lines.append(f"crosslinker:mass_heavy:{cl['mass_heavy']}")
        if cl["mass_light"]:
            cl_lines.append(f"crosslinker:mass_light:{cl['mass_light']}")

        with open("xi_crosslinking.conf", "w", encoding="utf-8") as f:
            f.write("## xiSEARCH crosslinking config (auto-generated from SDRF)\n")
            f.write("\n".join(cl_lines) + "\n")
        logger.info("Wrote xi_crosslinking.conf")

    def _write_scout_config(self, params: dict) -> None:
        """Write Scout config files (search_params.json + filter_params.json)."""
        prec = params["precursor_tolerance"]
        frag = params["fragment_tolerance"]
        cl = params["crosslinker"]

        search_params = {
            "enzymes": params["enzymes"],
            "fixed_modifications": [
                {"name": m["name"], "site": m["site"], "accession": m["accession"]}
                for m in params["fixed_modifications"]
            ],
            "variable_modifications": [
                {"name": m["name"], "site": m["site"], "accession": m["accession"]}
                for m in params["variable_modifications"]
            ],
            "precursor_tolerance": {"value": float(prec["value"]), "unit": prec["unit"]},
            "fragment_tolerance": {"value": float(frag["value"]), "unit": frag["unit"]},
            "crosslinker": {
                "name": cl["name"],
                "reactive_sites": [s.strip() for s in cl["sites"].split(",")] if cl["sites"] else [],
                "mass_heavy": float(cl["mass_heavy"]) if cl["mass_heavy"] else 0,
                "mass_light": float(cl["mass_light"]) if cl["mass_light"] else 0,
                "cleavable": cl["cleavable"].lower() == "yes" if cl["cleavable"] else False,
            },
        }

        filter_params = {
            "fdr_threshold": 0.05,
            "level": "residue_pair",
        }

        with open("search_params.json", "w", encoding="utf-8") as f:
            json.dump(search_params, f, indent=2)
        logger.info("Wrote search_params.json")

        with open("filter_params.json", "w", encoding="utf-8") as f:
            json.dump(filter_params, f, indent=2)
        logger.info("Wrote filter_params.json")
