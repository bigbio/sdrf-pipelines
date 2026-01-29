"""Modification handling for OpenMS conversion."""

import re

from sdrf_pipelines.openms.unimod import UnimodDatabase


class ModificationConverter:
    """Converts SDRF modification strings to OpenMS notation."""

    def __init__(self):
        self._unimod_database = UnimodDatabase()
        self.warnings: dict[str, int] = {}

    def _extract_modification_name(self, mod_string: str) -> str:
        """Extract modification name from SDRF modification string.

        Args:
            mod_string: SDRF modification string with NT= and AC= fields

        Returns:
            Modification name from Unimod

        Raises:
            ValueError: If modification string is invalid or not found in Unimod
        """
        name_match = re.search("NT=(.+?)(;|$)", mod_string)
        if name_match:
            name = name_match.group(1)
        else:
            raise ValueError(f"Invalid modification string format (missing NT=): {mod_string}")

        accession = re.search("AC=(.+?)(;|$)", mod_string)
        if accession:
            ptm = self._unimod_database.get_by_accession(accession.group(1))
        else:
            ptm = None

        if ptm is None:
            ptm = self._unimod_database.get_by_name(name)

        if ptm is None:
            raise ValueError("only UNIMOD modifications supported. " + mod_string)
        else:
            return ptm.get_name()

    def _extract_position_preference(self, mod_string: str) -> str:
        """Extract position preference from modification string.

        Args:
            mod_string: SDRF modification string

        Returns:
            Position preference (e.g., "Anywhere", "Protein N-term")
        """
        pp_match = re.search("PP=(.+?)(;|$)", mod_string)
        if pp_match is None:
            return "Anywhere"
        return pp_match.group(1)

    def _extract_target_amino_acid(self, mod_string: str, pp: str) -> str:
        """Extract target amino acid from modification string.

        Args:
            mod_string: SDRF modification string
            pp: Position preference

        Returns:
            Target amino acid(s) or empty string if not found
        """
        ta_match = re.search("TA=(.+?)(;|$)", mod_string)

        if ta_match is None:
            warning_message = "Warning no TA= specified. Setting to N-term or C-term if possible."
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1

            if "C-term" in pp:
                return "C-term"
            elif "N-term" in pp:
                return "N-term"
            else:
                warning_message = "Reassignment not possible. Skipping."
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                return ""

        return ta_match.group(1)

    def _format_modification_entries(self, name: str, pp: str, aa_list: list[str]) -> list[str]:
        """Format modification entries for OpenMS.

        Args:
            name: Modification name
            pp: Position preference
            aa_list: List of target amino acids

        Returns:
            List of formatted modification strings
        """
        entries = []

        if pp in ("Protein N-term", "Protein C-term"):
            for aa in aa_list:
                if aa in ("C-term", "N-term"):
                    entries.append(f"{name} ({pp})")
                else:
                    entries.append(f"{name} ({pp} {aa})")
        elif pp in ("Any N-term", "Any C-term"):
            normalized_pp = pp.replace("Any ", "")
            for aa in aa_list:
                if aa in ("C-term", "N-term"):
                    entries.append(f"{name} ({normalized_pp})")
                else:
                    entries.append(f"{name} ({normalized_pp} {aa})")
        else:
            for aa in aa_list:
                entries.append(f"{name} ({aa})")

        return entries

    def openms_ify_mods(self, sdrf_mods: list[str]) -> str:
        """Convert SDRF modifications to OpenMS notation.

        Args:
            sdrf_mods: List of SDRF modification strings

        Returns:
            Comma-separated string of OpenMS modification names
        """
        oms_mods = []

        for m in sdrf_mods:
            name = self._extract_modification_name(m)
            pp = self._extract_position_preference(m)
            ta = self._extract_target_amino_acid(m, pp)

            if not ta:
                continue

            aa_list = ta.split(",")
            mod_entries = self._format_modification_entries(name, pp, aa_list)
            oms_mods.extend(mod_entries)

        return ",".join(oms_mods)
