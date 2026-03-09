"""Constants for the MHCquant SDRF converter."""

from pathlib import Path

import pandas as pd

# Path to the bundled default search presets TSV (from nf-core/mhcquant assets)
DEFAULT_PRESETS_FILE = Path(__file__).parent / "default_search_presets.tsv"

# Column alias pairs: (new_name, old_name)
# Check new names first, fall back to old names
COLUMN_ALIASES = {
    "comment[ms min mz]": "comment[precursor min mz]",
    "comment[ms max mz]": "comment[precursor max mz]",
    "comment[ms min charge]": "comment[precursor min charge]",
    "comment[ms max charge]": "comment[precursor max charge]",
    "comment[ms min rt]": "comment[min retention time]",
    "comment[ms max rt]": "comment[max retention time]",
    "comment[ms min im]": "comment[min ion mobility]",
    "comment[ms max im]": "comment[max ion mobility]",
}

# Fields required to build a complete custom preset (all must be present)
REQUIRED_PRESET_FIELDS = [
    "precursor_mass_tolerance",
    "precursor_error_unit",
    "fragment_mass_tolerance",
    "precursor_mass_range",
    "precursor_charge",
    "activation_method",
    "instrument_resolution",
    "mhc_class",
]

# MHC class → (min_peptide_length, max_peptide_length)
MHC_CLASS_PEPTIDE_LENGTHS = {
    "class1": (8, 14),
    "class2": (8, 30),
}

# Instrument name patterns → preset prefix
# Keys are lowercased patterns to match against the instrument name
INSTRUMENT_PRESET_MAP = [
    # Order matters: more specific patterns first
    (["lumos", "fusion", "exploris", "eclipse"], "lumos"),
    (["q exactive", "exactive"], "qe"),
    (["timstof", "tims tof"], "timstof"),
    (["astral"], "astral"),
    (["ltq orbitrap xl", "orbitrap xl"], "xl"),
]

# Preset column order for output TSV
PRESET_COLUMNS = [
    "PresetName",
    "PeptideMinLength",
    "PeptideMaxLength",
    "PrecursorMassRange",
    "PrecursorCharge",
    "PrecursorMassTolerance",
    "PrecursorErrorUnit",
    "FragmentMassTolerance",
    "FragmentBinOffset",
    "MS2PIPModel",
    "ActivationMethod",
    "Instrument",
    "NumberMods",
    "FixedMods",
    "VariableMods",
]


def load_default_presets(presets_file: str | Path | None = None) -> dict[str, dict]:
    """Load default presets from a TSV file.

    Args:
        presets_file: Path to a presets TSV. If None, uses the bundled
            default_search_presets.tsv shipped with this package.

    Returns:
        Dict mapping preset name to a dict of preset column values.
    """
    path = Path(presets_file) if presets_file else DEFAULT_PRESETS_FILE
    df = pd.read_csv(path, sep="\t", keep_default_na=False)
    presets = {}
    for _, row in df.iterrows():
        name = str(row["PresetName"])
        presets[name] = {col: row[col] for col in PRESET_COLUMNS if col in row.index}
    return presets
