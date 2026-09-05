"""Constants for the MHCquant SDRF converter."""

from pathlib import Path

import pandas as pd

__all__ = [
    "DEFAULT_PRESETS_FILE",
    "EMPTY_VALUES",
    "INSTRUMENT_PRESET_MAP",
    "MHC_CLASS_PEPTIDE_LENGTHS",
    "PRESET_COLUMNS",
    "load_default_presets",
]

DEFAULT_PRESETS_FILE = Path(__file__).parent / "default_search_presets.tsv"

EMPTY_VALUES = {"nan", "", "not available"}

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
    # Elite/Velos deliberately map to "qe", NOT "xl". The xl preset differs from qe only on the
    # fragment side (0.50025 Da / bin offset 0.4 / low_res), i.e. it encodes ion-trap MS2, not an
    # instrument model. Elite/Velos deposits commonly acquire MS2 in the Orbitrap (e.g. R=15,000),
    # so "qe" is the correct fallback; resolve_fragment_tolerance() still downgrades a genuine
    # ion-trap run to low_res when comment[ms2 mass analyzer] is populated. Mapping to "xl" would
    # silently apply 0.50025 Da to any Elite/Velos file that omits that column.
    (["elite", "velos"], "qe"),
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


def load_default_presets(presets_file: str | Path | None = None) -> dict[str, dict[str, object]]:
    """Load default presets from a TSV file into a dict keyed by preset name."""
    path = Path(presets_file) if presets_file else DEFAULT_PRESETS_FILE
    df = pd.read_csv(path, sep="\t", keep_default_na=False)
    return {
        row["PresetName"]: {col: row[col] for col in PRESET_COLUMNS if col in row.index} for _, row in df.iterrows()
    }
