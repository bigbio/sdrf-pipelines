"""Modification handling for DIA-NN conversion."""

import re

from sdrf_pipelines.converters.openms.unimod import UnimodDatabase

# Modification names that indicate isobaric/isotopic labels
_LABEL_MOD_PREFIXES = ("TMT", "iTRAQ", "Label")


class DiannModificationConverter:
    """Converts SDRF modification strings to DIA-NN notation.

    DIA-NN format: Name,DeltaMass,Site[,label]
    The optional 'label' suffix indicates isotopic labels that don't affect RT.
    """

    def __init__(self):
        self._unimod_db = UnimodDatabase()

    def convert_modification(self, mod_string: str, is_fixed: bool) -> str:
        """Convert a single SDRF modification string to DIA-NN format.

        Args:
            mod_string: SDRF mod string (e.g., "NT=Carbamidomethyl;TA=C;MT=fixed;AC=UNIMOD:4")
            is_fixed: Whether this is a fixed modification

        Returns:
            DIA-NN format string (e.g., "Carbamidomethyl,57.021464,C")

        Raises:
            ValueError: If modification not found in Unimod
        """
        name = self._extract_name(mod_string)
        site = self._extract_site(mod_string)
        delta_mass = self._get_delta_mass(name, mod_string)

        parts = [name, str(delta_mass), site]

        # Add 'label' suffix for isobaric label modifications
        if any(name.startswith(prefix) for prefix in _LABEL_MOD_PREFIXES):
            parts.append("label")

        return ",".join(parts)

    def convert_all_modifications(self, fixed_mods: list[str], var_mods: list[str]) -> tuple[list[str], list[str]]:
        """Convert lists of SDRF modifications to DIA-NN format.

        Args:
            fixed_mods: List of SDRF fixed modification strings
            var_mods: List of SDRF variable modification strings

        Returns:
            Tuple of (fixed_diann_mods, var_diann_mods)
        """
        fixed_result = []
        var_result = []

        for mod in fixed_mods:
            if mod.strip():
                fixed_result.append(self.convert_modification(mod, is_fixed=True))

        for mod in var_mods:
            if mod.strip():
                var_result.append(self.convert_modification(mod, is_fixed=False))

        return fixed_result, var_result

    def _extract_name(self, mod_string: str) -> str:
        """Extract and validate modification name via Unimod lookup."""
        name_match = re.search(r"NT=(.+?)(;|$)", mod_string)
        if not name_match:
            raise ValueError(f"Invalid modification string (missing NT=): {mod_string}")

        name = name_match.group(1)

        # Try accession first, then name
        accession_match = re.search(r"AC=(.+?)(;|$)", mod_string)
        ptm = None
        if accession_match:
            ptm = self._unimod_db.get_by_accession(accession_match.group(1))
        if ptm is None:
            ptm = self._unimod_db.get_by_name(name)
        if ptm is None:
            raise ValueError(f"only UNIMOD modifications supported: {mod_string}")

        return ptm.get_name()

    def _extract_site(self, mod_string: str) -> str:
        """Extract target site and convert to DIA-NN notation."""
        ta_match = re.search(r"TA=(.+?)(;|$)", mod_string)
        pp_match = re.search(r"PP=(.+?)(;|$)", mod_string)

        if ta_match:
            site = ta_match.group(1)
        elif pp_match:
            site = pp_match.group(1)
        else:
            raise ValueError(f"No target site (TA= or PP=) in: {mod_string}")

        # Convert to DIA-NN site notation
        if site == "Protein N-term":
            return "*n"
        elif site in ("N-term", "Any N-term"):
            return "n"
        elif site == "Protein C-term":
            return "*c"
        elif site in ("C-term", "Any C-term"):
            return "c"
        return site

    def _get_delta_mass(self, name: str, mod_string: str) -> float:
        """Get monoisotopic delta mass from Unimod."""
        accession_match = re.search(r"AC=(.+?)(;|$)", mod_string)
        ptm = None
        if accession_match:
            ptm = self._unimod_db.get_by_accession(accession_match.group(1))
        if ptm is None:
            ptm = self._unimod_db.get_by_name(name)
        return ptm._delta_mono_mass

    def find_unimod_by_name(self, name: str) -> str | None:
        """Look up a modification by name and return its UniMod accession.

        Args:
            name: Modification name (e.g., 'Phospho', 'Acetyl', 'GlyGly')

        Returns:
            UniMod accession string (e.g., 'UNIMOD:21') or None if not found
        """
        ptm = self._unimod_db.get_by_name(name)
        if ptm is not None:
            return ptm.get_accession()
        return None
