"""Relink SDRF converter.

Converts SDRF files to relink pipeline configuration:
- relink_design.tsv: Per-file metadata (filename, sample, fraction, replicate)
- Engine-specific config files generated from templates + SDRF-derived parameters:
  - xiSEARCH: xi_linear.conf + xi_crosslinking.conf
  - Scout: search_params.json + filter_params.json
"""

import json
import logging
import os
import re
from typing import Any

import pandas as pd

from sdrf_pipelines.converters.base import BaseConverter
from sdrf_pipelines.converters.openms.utils import parse_tolerance

logger = logging.getLogger(__name__)

# Regex for parsing key=value pairs in SDRF cells (e.g., NT=DSSO;AC=XLMOD:02010)
_KV_PATTERN = re.compile(r"(\w+)=([^;]+)")

# UNIMOD accession to delta mass mapping (common modifications)
UNIMOD_MASSES = {
    "UNIMOD:4": 57.021464,     # Carbamidomethyl
    "UNIMOD:35": 15.99491463,  # Oxidation
    "UNIMOD:1": 42.010565,     # Acetyl
    "UNIMOD:21": 79.966331,    # Phospho
    "UNIMOD:7": 0.984016,      # Deamidated
    "UNIMOD:28": -17.026549,   # Glu->pyro-Glu
    "UNIMOD:27": -18.010565,   # Dehydrated
    "UNIMOD:34": 14.015650,    # Methyl
    "UNIMOD:36": 28.031300,    # Dimethyl
}

# Modification name to symbol extension for xiSEARCH
MOD_SYMBOLS = {
    "Carbamidomethyl": "cm",
    "Oxidation": "ox",
    "Acetyl": "ac",
    "Phospho": "ph",
    "Deamidated": "de",
    "Methyl": "me",
}

# Enzyme name to xiSEARCH digestion format
ENZYME_XISEARCH = {
    "Trypsin": "PostAAConstrainedDigestion:DIGESTED:K,R;ConstrainingAminoAcids:P;NAME=Trypsin",
    "Lys-C": "PostAAConstrainedDigestion:DIGESTED:K;ConstrainingAminoAcids:P;NAME=Lys-C",
    "Asp-N": "PostAAConstrainedDigestion:DIGESTED:D;ConstrainingAminoAcids:;NAME=Asp-N",
    "Chymotrypsin": "PostAAConstrainedDigestion:DIGESTED:F,W,Y,L;ConstrainingAminoAcids:P;NAME=Chymotrypsin",
    "Arg-C": "PostAAConstrainedDigestion:DIGESTED:R;ConstrainingAminoAcids:P;NAME=Arg-C",
}

# Enzyme name to Scout enzyme object
ENZYME_SCOUT = {
    "Trypsin": {"Name": "Trypsin", "CTerminus": True, "Sites": "KR", "BlockedBy": "P"},
    "Lys-C": {"Name": "Lys-C", "CTerminus": True, "Sites": "K", "BlockedBy": "P"},
    "Asp-N": {"Name": "Asp-N", "CTerminus": False, "Sites": "D", "BlockedBy": ""},
    "Chymotrypsin": {"Name": "Chymotrypsin", "CTerminus": True, "Sites": "FWYL", "BlockedBy": "P"},
    "Arg-C": {"Name": "Arg-C", "CTerminus": True, "Sites": "R", "BlockedBy": "P"},
}

# Known crosslinker properties (full mass + stub masses)
CROSSLINKER_DB = {
    "DSSO": {
        "xisearch_class": "SymetricSingleAminoAcidRestrictedCrossLinker",
        "mass": 158.0037648,
        "light_fragment": 54.0105647,
        "heavy_fragment": 85.9826354,
        "stubs": "A,54.0105647,S,103.9932001,T,85.9826354",
        "linked_aa_default": "K(0),S(0.2),T(0.2),Y(0.2),nterm(0)",
        "scout_name": "DSSO_KSYT",
        "scout_alpha_targets": "KSYT",
        "scout_beta_targets": "KSYT",
        "scout_target_nterm": True,
    },
    "DSBU": {
        "xisearch_class": "SymetricSingleAminoAcidRestrictedCrossLinker",
        "mass": 196.0848,
        "light_fragment": 85.0528,
        "heavy_fragment": 111.0320,
        "stubs": "A,85.0528,S,111.0320,T,196.0848",
        "linked_aa_default": "K(0),S(0.2),T(0.2),Y(0.2),nterm(0)",
        "scout_name": "DSBU_KSYT",
        "scout_alpha_targets": "KSYT",
        "scout_beta_targets": "KSYT",
        "scout_target_nterm": True,
    },
}


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
        file_data = self._extract_file_data(sdrf)
        global_params = self._extract_global_params(sdrf)

        self._write_design(file_data)

        if engine == "xisearch":
            self._write_xisearch_config(global_params)
        elif engine == "scout":
            self._write_scout_config(global_params)
        else:
            raise ValueError(f"Unknown engine: {engine}. Use 'xisearch' or 'scout'.")

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
                "uri": row.get("comment[associated file uri]", ""),
            }
            rows.append(entry)
        return rows

    def _extract_global_params(self, sdrf: pd.DataFrame) -> dict[str, Any]:
        """Extract global search parameters from the SDRF (first row)."""
        row = sdrf.iloc[0]
        params: dict[str, Any] = {}

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
            delta_mass = UNIMOD_MASSES.get(ac, 0.0)
            mod_entry = {"name": name, "site": site, "accession": ac, "delta_mass": delta_mass}
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

        return params

    def _write_design(self, file_data: list[dict]) -> None:
        """Write relink_design.tsv."""
        df = pd.DataFrame(file_data)
        df.to_csv("relink_design.tsv", sep="\t", index=False)
        logger.info(f"Wrote relink_design.tsv with {len(df)} entries")

    # =========================================================================
    # xiSEARCH config generation
    # =========================================================================

    def _write_xisearch_config(self, params: dict) -> None:
        """Write xiSEARCH config files matching the real xiSEARCH format."""
        prec = params["precursor_tolerance"]
        frag = params["fragment_tolerance"]
        cl_name = params["crosslinker"]["name"]
        cl_info = CROSSLINKER_DB.get(cl_name, {})

        # Build common config sections
        common_lines = []
        common_lines.append("####################")
        common_lines.append("##Tolerances")
        common_lines.append(f"tolerance:precursor:{prec['value']}{prec['unit']}")
        common_lines.append(f"tolerance:fragment:{frag['value']}{frag['unit']}")
        common_lines.append("")
        common_lines.append("####################")
        common_lines.append("## include linear matches")
        common_lines.append("EVALUATELINEARS:true")
        common_lines.append("")

        # Fixed modifications
        common_lines.append("##========================")
        common_lines.append("##--Fixed Modifications")
        for mod in params["fixed_modifications"]:
            symbol = MOD_SYMBOLS.get(mod["name"], mod["name"][:2].lower())
            common_lines.append(
                f"modification:fixed::SYMBOLEXT:{symbol};MODIFIED:{mod['site']};DELTAMASS:{mod['delta_mass']}"
            )
        common_lines.append("")

        # Variable modifications
        common_lines.append("##========================")
        common_lines.append("##--Variable Modifications")
        for mod in params["variable_modifications"]:
            symbol = MOD_SYMBOLS.get(mod["name"], mod["name"][:2].lower())
            common_lines.append(
                f"modification:variable::SYMBOLEXT:{symbol};MODIFIED:{mod['site']};DELTAMASS:{mod['delta_mass']}"
            )
        common_lines.append("")

        # Digestion
        common_lines.append("###################")
        common_lines.append("## Digest")
        for enzyme in params["enzymes"]:
            digest_str = ENZYME_XISEARCH.get(enzyme)
            if digest_str:
                common_lines.append(f"digestion:{digest_str}")
            else:
                self.add_warning(f"Unknown enzyme for xiSEARCH: {enzyme}")
                common_lines.append(f"## WARNING: unknown enzyme {enzyme}")
        common_lines.append("")

        # Standard fragment/scoring settings (same as real configs)
        scoring_lines = [
            "####################",
            "## Non-Lossy Fragments to consider",
            "fragment:BIon",
            "fragment:YIon",
            "fragment:PeptideIon",
            "",
            "###################",
            "## Losses",
            "loss:AminoAcidRestrictedLoss:NAME:H20;aminoacids:S,T,D,E;MASS:18.01056027;cterm",
            "loss:AminoAcidRestrictedLoss:NAME:NH3;aminoacids:R,K,N,Q;MASS:17.02654493;nterm",
            "loss:AIonLoss",
            "",
            "ConservativeLosses:3",
            "IsotopPattern:Averagin",
            "MATCH_MISSING_MONOISOTOPIC:true",
            "missing_isotope_peaks:2",
            "",
            "####################",
            "## Search settings",
            "mgcpeaks:10",
            "topmgchits:150",
            "topmgxhits:10",
            "missedcleavages:3",
            "MINIMUM_PEPTIDE_LENGTH:6",
            "",
            "#####################",
            "## IO-settings",
            "BufferInput:100",
            "BufferOutput:100",
            "",
            "MAXPEPTIDEMASS:4000",
            "MAX_MODIFICATION_PER_PEPTIDE:3",
            "MAX_MODIFIED_PEPTIDES_PER_PEPTIDE:20",
            "FRAGMENTTREE:FU",
        ]

        # --- Linear config ---
        linear_lines = list(common_lines)
        linear_lines.append("#################")
        linear_lines.append("## Cross Linker + associated modifications")
        linear_lines.append("crosslinker:LinearCrosslinker:NAME:linear")
        linear_lines.append("")
        linear_lines.extend(scoring_lines)
        linear_lines.append("TOPMATCHESONLY:false")

        with open("xi_linear.conf", "w", encoding="utf-8") as f:
            f.write("\n".join(linear_lines) + "\n")

        # --- Crosslinking config ---
        crosslink_lines = list(common_lines)
        crosslink_lines.append("#################")
        crosslink_lines.append("## Cross Linker + associated modifications")
        if cl_info:
            # Use default linked amino acids with penalty weights from CROSSLINKER_DB
            linked_aa = cl_info["linked_aa_default"]

            crosslink_lines.append(
                f"crosslinker:{cl_info['xisearch_class']}:Name:{cl_name};"
                f"MASS:{cl_info['mass']};LINKEDAMINOACIDS:{linked_aa};"
                f"STUBS:{cl_info['stubs']}"
            )
        else:
            self.add_warning(f"Unknown crosslinker: {cl_name}. Add it to CROSSLINKER_DB.")
            crosslink_lines.append(f"## WARNING: unknown crosslinker {cl_name} - configure manually")
        crosslink_lines.append("crosslinker:NonCovalentBound:Name:NonCovalent")
        crosslink_lines.append("")

        # Add DSSO-specific variable mods (Tris quenching)
        if cl_name == "DSSO":
            crosslink_lines.append("## DSSO quenching modifications")
            crosslink_lines.append("modification:variable::SYMBOLEXT:dsso_tris;MODIFIED:K,nterm;DELTAMASS:279.077658")
            crosslink_lines.append("modification:variable::SYMBOLEXT:dsso_tris;MODIFIED:S,T,Y;DELTAMASS:279.077658")
            crosslink_lines.append("")

        crosslink_lines.extend(scoring_lines)
        crosslink_lines.append("TOPMATCHESONLY:true")

        with open("xi_crosslinking.conf", "w", encoding="utf-8") as f:
            f.write("\n".join(crosslink_lines) + "\n")

        logger.info("Wrote xi_linear.conf and xi_crosslinking.conf")

    # =========================================================================
    # Scout config generation
    # =========================================================================

    def _write_scout_config(self, params: dict) -> None:
        """Write Scout config files matching the real Scout JSON format."""
        prec = params["precursor_tolerance"]
        frag = params["fragment_tolerance"]
        cl_name = params["crosslinker"]["name"]
        cl_info = CROSSLINKER_DB.get(cl_name, {})

        # Use first enzyme (Scout supports one enzyme)
        enzyme_name = params["enzymes"][0] if params["enzymes"] else "Trypsin"
        enzyme_obj = ENZYME_SCOUT.get(enzyme_name, ENZYME_SCOUT["Trypsin"])

        # Build static modifications
        static_mods = []
        for i, mod in enumerate(params["fixed_modifications"]):
            static_mods.append({
                "MassShift": mod["delta_mass"],
                "Name": mod["name"],
                "IsCTerm": False,
                "IsNTerm": False,
                "IsVariable": False,
                "TargetResidues": mod["site"],
                "ModIndex": i + 1,
            })

        # Build variable modifications
        variable_mods = []
        for i, mod in enumerate(params["variable_modifications"]):
            variable_mods.append({
                "MassShift": mod["delta_mass"],
                "Name": mod["name"],
                "IsCTerm": False,
                "IsNTerm": False,
                "IsVariable": True,
                "TargetResidues": mod["site"],
                "ModIndex": len(static_mods) + i + 1,
            })

        # Build crosslinker reagent
        if cl_info:
            cxl_reagent = {
                "Name": cl_info["scout_name"],
                "LightTag": "Light",
                "LightFragment": cl_info["light_fragment"],
                "HeavyTag": "Heavy",
                "HeavyFragment": cl_info["heavy_fragment"],
                "WholeTag": "Full",
                "WholeMass": cl_info["mass"],
                "DeltaShift": float(cl_info["heavy_fragment"]) - float(cl_info["light_fragment"]),
                "AlphaTargets": cl_info["scout_alpha_targets"],
                "BetaTargets": cl_info["scout_beta_targets"],
                "TargetNTerm": cl_info["scout_target_nterm"],
            }
        else:
            self.add_warning(f"Unknown crosslinker for Scout: {cl_name}")
            cxl_reagent = {"Name": cl_name}

        search_params = {
            "BDP_Mode": False,
            "MSFileExtension": ".mzML",
            "PPMMS1Tolerance": float(prec["value"]),
            "PPMMS2Tolerance": float(frag["value"]),
            "PerformShotgunSearch": False,
            "PerformCleaveXLSearch": True,
            "SaveSpectraToResults": False,
            "MaxQueryResults": 4,
            "MinPepLength": 6,
            "MaxPepLength": 60,
            "MinPepMass": 500.0,
            "MaxPepMass": 6000.0,
            "FastaFile": "",
            "RawPath": "",
            "OutputFolder": "",
            "AddMinusOneIsotope": False,
            "CarbonIsotopeShift": 1.0033548,
            "IsotopicPossibilitiesPrecursor": 1,
            "Enzyme": enzyme_obj,
            "EnzymeSpecificity": 0,
            "MiscleavageNum": 3,
            "StaticModifications": static_mods,
            "VariableModifications": variable_mods,
            "MaximumVariableModsPerPeptide": 2,
            "CXLReagent": cxl_reagent,
            "SearchLoopLinks": True,
            "IonPairMaxCharge": 2,
            "FullPairsOnly": False,
            "PairFinderPPM": 10.0,
            "AddXLasVariableMod": False,
            "AddDecoys": True,
            "AddContaminants": True,
            "DontShowContaminants": True,
            "DecoyTag": "Reverse",
            "DecoyGenerationMode": 0,
            "FastaBatchSize": 30000,
            "MergeDatabase": True,
            "MethionineInitiator": True,
            "ParallelPSMs": True,
        }

        filter_params = {
            "CSM_FDR": 0.01,
            "ResPair_FDR": 0.01,
            "PPI_FDR": 0.01,
            "UniquePPIsOnly": False,
            "UsePythonModels": True,
            "ApplyPostProcessingFilters": True,
            "FDRMode": 1,
        }

        with open("search_params.json", "w", encoding="utf-8") as f:
            json.dump(search_params, f, indent=2)

        with open("filter_params.json", "w", encoding="utf-8") as f:
            json.dump(filter_params, f, indent=2)

        logger.info("Wrote search_params.json and filter_params.json")
