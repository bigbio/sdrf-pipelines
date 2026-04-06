import re

import pandas as pd


class Relink:
    """Convert SDRF to relink pipeline configuration files."""

    def __init__(self) -> None:
        self.warnings: dict[str, int] = {}

    def convert_relink_config(self, sdrf_file: str, output_path: str) -> None:
        """Convert SDRF file to relink_config.tsv.

        Extracts standard proteomics columns and XL-MS specific columns
        (crosslinker, enrichment, etc.) into a per-file config TSV.
        """
        sdrf = pd.read_csv(sdrf_file, sep="\t")
        sdrf.columns = sdrf.columns.str.lower()

        rows = []
        for _, row in sdrf.iterrows():
            entry = {}

            # Standard columns
            entry["filename"] = row.get("comment[data file]", "")
            entry["sample_id"] = row.get("source name", "")
            entry["fraction"] = row.get("comment[fraction identifier]", "1")
            entry["technical_replicate"] = row.get("comment[technical replicate]", "1")

            # Enzyme(s) — may have multiple cleavage agent columns
            enzyme_cols = [c for c in sdrf.columns if c.startswith("comment[cleavage agent details]")]
            enzymes = []
            for ec in enzyme_cols:
                val = row.get(ec, "")
                if pd.notna(val) and val:
                    nt_match = re.search(r"NT=([^;]+)", str(val))
                    if nt_match:
                        enzymes.append(nt_match.group(1))
            entry["enzyme"] = ";".join(enzymes) if enzymes else "Trypsin"

            # Modifications
            mod_cols = [c for c in sdrf.columns if c.startswith("comment[modification parameters]")]
            fixed_mods = []
            variable_mods = []
            for mc in mod_cols:
                val = str(row.get(mc, ""))
                if "MT=Fixed" in val or "MT=fixed" in val:
                    nt_match = re.search(r"NT=([^;]+)", val)
                    ta_match = re.search(r"TA=([^;]+)", val)
                    if nt_match:
                        mod_name = nt_match.group(1)
                        mod_site = ta_match.group(1) if ta_match else ""
                        fixed_mods.append(f"{mod_name} ({mod_site})")
                elif "MT=Variable" in val or "MT=variable" in val:
                    nt_match = re.search(r"NT=([^;]+)", val)
                    ta_match = re.search(r"TA=([^;]+)", val)
                    if nt_match:
                        mod_name = nt_match.group(1)
                        mod_site = ta_match.group(1) if ta_match else ""
                        variable_mods.append(f"{mod_name} ({mod_site})")
            entry["fixed_modifications"] = ";".join(fixed_mods)
            entry["variable_modifications"] = ";".join(variable_mods)

            # Tolerances
            entry["precursor_mass_tolerance"] = row.get("comment[precursor mass tolerance]", "10 ppm")
            entry["fragment_mass_tolerance"] = row.get("comment[fragment mass tolerance]", "20 ppm")

            # Dissociation method
            diss_val = row.get("comment[dissociation method]", "")
            nt_match = re.search(r"NT=([^;]+)", str(diss_val))
            entry["dissociation_method"] = nt_match.group(1) if nt_match else "HCD"

            # Collision energy
            entry["collision_energy"] = row.get("comment[collision energy]", "")

            # XL-MS specific: crosslinker
            crosslinker_val = row.get("comment[cross-linker]", "")
            entry["crosslinker_raw"] = str(crosslinker_val)
            if pd.notna(crosslinker_val) and crosslinker_val:
                cl_str = str(crosslinker_val)
                nt_match = re.search(r"NT=([^;]+)", cl_str)
                ac_match = re.search(r"AC=([^;]+)", cl_str)
                ta_match = re.search(r"TA=([^;]+)", cl_str)
                mh_match = re.search(r"MH=([^;]+)", cl_str)
                ml_match = re.search(r"ML=([^;]+)", cl_str)
                cl_match = re.search(r"CL=([^;]+)", cl_str)
                entry["crosslinker_name"] = nt_match.group(1) if nt_match else ""
                entry["crosslinker_accession"] = ac_match.group(1) if ac_match else ""
                entry["crosslinker_sites"] = ta_match.group(1) if ta_match else ""
                entry["crosslinker_mass_heavy"] = mh_match.group(1) if mh_match else ""
                entry["crosslinker_mass_light"] = ml_match.group(1) if ml_match else ""
                entry["crosslinker_cleavable"] = cl_match.group(1) if cl_match else ""
            else:
                entry["crosslinker_name"] = ""
                entry["crosslinker_accession"] = ""
                entry["crosslinker_sites"] = ""
                entry["crosslinker_mass_heavy"] = ""
                entry["crosslinker_mass_light"] = ""
                entry["crosslinker_cleavable"] = ""

            # XL-MS specific: other columns
            entry["experiment_type"] = row.get("comment[chemical cross-linking coupled with ms]", "")
            entry["enrichment_method"] = row.get("comment[crosslink enrichment method]", "")
            entry["crosslinker_concentration"] = row.get("comment[crosslinker concentration]", "")
            entry["crosslink_distance"] = row.get("comment[crosslink distance]", "")

            rows.append(entry)

        df = pd.DataFrame(rows)
        df.to_csv(output_path, sep="\t", index=False)
        print(f"Wrote relink config with {len(df)} entries to {output_path}")

        for warning, count in self.warnings.items():
            print(f"WARNING: {warning} (occurred {count} times)")
