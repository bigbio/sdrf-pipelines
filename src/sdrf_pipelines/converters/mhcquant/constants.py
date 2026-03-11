"""Constants for the MHCquant SDRF converter."""

from pathlib import Path

import pandas as pd

DEFAULT_PRESETS_FILE = Path(__file__).parent / "default_search_presets.tsv"

MHC_CLASS_PEPTIDE_LENGTHS = {
    "class1": (8, 14),
    "class2": (8, 30),
}

# Instrument name patterns → preset prefix
# Order matters: first match wins
INSTRUMENT_PRESET_MAP = [
    (["lumos", "fusion", "exploris", "eclipse"], "lumos"),
    (["q exactive", "exactive"], "qe"),
    (["timstof", "tims tof"], "timstof"),
    (["astral"], "astral"),
    (["ltq orbitrap xl", "orbitrap xl"], "xl"),
]

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
    """Load default presets from a TSV file into a dict keyed by preset name."""
    path = Path(presets_file) if presets_file else DEFAULT_PRESETS_FILE
    df = pd.read_csv(path, sep="\t", keep_default_na=False)
    return {
        row["PresetName"]: {col: row[col] for col in PRESET_COLUMNS if col in row.index}
        for _, row in df.iterrows()
    }
