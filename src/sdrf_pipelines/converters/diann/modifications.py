"""Modification handling for DIA-NN conversion."""

import re

from sdrf_pipelines.converters.openms.unimod import UnimodDatabase

# Modification names that indicate isobaric/isotopic labels
_LABEL_MOD_PREFIXES = ("TMT", "iTRAQ", "Label")

# SDRF terminal target notations -> DIA-NN site tokens
_TERMINAL_SITE_MAP = {
    "Protein N-term": "*n",
    "N-term": "n",
    "Any N-term": "n",
    "Protein C-term": "*c",
    "C-term": "c",
    "Any C-term": "c",
}


class DiannModificationConverter:
    """Converts SDRF modification strings to DIA-NN notation.

    DIA-NN format: ``Name,DeltaMass,Sites[,label]``. ``Sites`` is a single
    *concatenated* string of target residues/termini **without separators**
    (e.g. ``STY``, ``MP``, ``nK``); the comma only ever delimits the optional
    fourth ``label`` field.

    DIA-NN keeps only the first residue of a comma-separated site list and
    de-duplicates ``--var-mod``/``--fixed-mod`` entries by modification name, so
    a modification targeting several residues must be emitted as a single entry
    with the sites concatenated. Two SDRF representations would otherwise be
    silently truncated by DIA-NN, dropping every residue except the first:

    * a single SDRF cell with comma-separated targets (``TA=S,T,Y``); and
    * the same modification (same name + mass) declared across several SDRF
      cells with different targets (``Oxidation`` on ``M`` and on ``P``).

    This converter handles both: it concatenates multi-residue sites within a
    cell (``TA=S,T,Y`` -> ``STY``) and merges same-(name, mass) modifications
    across cells (``Oxidation`` ``M`` + ``P`` -> ``Oxidation,15.994915,MP``).
    """

    def __init__(self):
        self._unimod_db = UnimodDatabase()

    def convert_modification(self, mod_string: str, is_fixed: bool) -> str:
        """Convert a single SDRF modification string to DIA-NN format.

        Args:
            mod_string: SDRF mod string (e.g., "NT=Carbamidomethyl;TA=C;MT=fixed;AC=UNIMOD:4")
            is_fixed: Whether this is a fixed modification (kept for API compatibility)

        Returns:
            DIA-NN format string (e.g., "Carbamidomethyl,57.021464,C"). Multi-residue
            sites declared in one cell are concatenated (e.g. "Phospho,79.966331,STY").

        Raises:
            ValueError: If modification not found in Unimod or has no target site
        """
        name, delta_mass, sites, is_label = self._parse_modification(mod_string)
        return self._format(name, delta_mass, sites, is_label)

    def convert_all_modifications(self, fixed_mods: list[str], var_mods: list[str]) -> tuple[list[str], list[str]]:
        """Convert lists of SDRF modifications to DIA-NN format.

        Modifications sharing the same name and delta mass are merged into a
        single DIA-NN entry with their target sites combined, so DIA-NN does not
        silently drop residues (see the class docstring).

        Args:
            fixed_mods: List of SDRF fixed modification strings
            var_mods: List of SDRF variable modification strings

        Returns:
            Tuple of (fixed_diann_mods, var_diann_mods)
        """
        return self._convert_and_merge(fixed_mods), self._convert_and_merge(var_mods)

    def _convert_and_merge(self, mods: list[str]) -> list[str]:
        """Convert and merge SDRF modification strings.

        Entries sharing ``(name, delta_mass, is_label)`` are combined into one,
        accumulating their target sites. The first-seen order of distinct
        modifications is preserved.
        """
        order: list[tuple] = []
        merged: dict[tuple, dict] = {}
        for mod in mods:
            if not mod or not mod.strip():
                continue
            name, delta_mass, sites, is_label = self._parse_modification(mod)
            key = (name, delta_mass, is_label)
            if key not in merged:
                merged[key] = {"name": name, "mass": delta_mass, "sites": [], "label": is_label}
                order.append(key)
            for token in sites:
                if token not in merged[key]["sites"]:
                    merged[key]["sites"].append(token)
        return [self._format(m["name"], m["mass"], m["sites"], m["label"]) for m in (merged[key] for key in order)]

    def _format(self, name: str, delta_mass: float, sites: list[str], is_label: bool) -> str:
        """Assemble a DIA-NN modification string from its components.

        Site tokens are concatenated (no separators), sorted for deterministic
        output; the comma-delimited ``label`` flag is appended for labels only.
        """
        parts = [name, str(delta_mass), "".join(sorted(set(sites)))]
        if is_label:
            parts.append("label")
        return ",".join(parts)

    def _parse_modification(self, mod_string: str) -> tuple[str, float, list[str], bool]:
        """Parse an SDRF modification string into (name, delta_mass, site tokens, is_label)."""
        name = self._extract_name(mod_string)
        sites = self._extract_sites(mod_string)
        delta_mass = self._get_delta_mass(name, mod_string)
        is_label = any(name.startswith(prefix) for prefix in _LABEL_MOD_PREFIXES)
        return name, delta_mass, sites, is_label

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

    def _extract_sites(self, mod_string: str) -> list[str]:
        """Extract target site(s) and convert to DIA-NN site tokens.

        Handles a single residue (``TA=M``), a comma-separated residue list in a
        single cell (``TA=S,T,Y``), and terminal notations (``PP=Protein
        N-term``). Returns a de-duplicated list of DIA-NN site tokens.
        """
        ta_match = re.search(r"TA=(.+?)(;|$)", mod_string)
        pp_match = re.search(r"PP=(.+?)(;|$)", mod_string)

        if ta_match:
            raw = ta_match.group(1)
        elif pp_match:
            raw = pp_match.group(1)
        else:
            raise ValueError(f"No target site (TA= or PP=) in: {mod_string}")

        tokens: list[str] = []
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            # Convert to DIA-NN site notation; residues map to themselves
            token = _TERMINAL_SITE_MAP.get(part, part)
            if token not in tokens:
                tokens.append(token)

        if not tokens:
            raise ValueError(f"No target site (TA= or PP=) in: {mod_string}")
        return tokens

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
