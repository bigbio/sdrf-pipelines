"""Constants for the MHCquant SDRF converter."""

from pathlib import Path

import pandas as pd

__all__ = [
    "ACTIVATION_METHOD_BY_ACCESSION",
    "ACTIVATION_METHOD_BY_NAME",
    "COMET_ACTIVATION_METHODS",
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
    # Velos/Elite behave like Q Exactive instruments, so they share the qe presets
    (["elite", "velos"], "qe"),
    (["timstof", "tims tof"], "timstof"),
    (["astral"], "astral"),
    (["ltq orbitrap xl", "orbitrap xl"], "xl"),
]

# PSI-MS dissociation method accession / lowercase name or synonym -> mhcquant ActivationMethod
ACTIVATION_METHOD_BY_ACCESSION = {
    "MS:1000133": "CID",
    "MS:1000422": "HCD",
    "MS:1002481": "HCD",
    "MS:1000598": "ETD",
    "MS:1000250": "ECD",
    "MS:1002631": "EThcD",
    "MS:1003182": "ETciD",
}

ACTIVATION_METHOD_BY_NAME = {
    "cid": "CID",
    "collision-induced dissociation": "CID",
    "hcd": "HCD",
    "beam-type collision-induced dissociation": "HCD",
    "higher energy beam-type collision-induced dissociation": "HCD",
    "etd": "ETD",
    "electron transfer dissociation": "ETD",
    "ecd": "ECD",
    "electron capture dissociation": "ECD",
    "ethcd": "EThcD",
    "electron-transfer/higher-energy collision dissociation": "EThcD",
    "etcid": "ETciD",
    "electron-transfer/collision-induced dissociation": "ETciD",
}

# ActivationMethod values accepted by both the mhcquant presets schema and OpenMS CometAdapter
COMET_ACTIVATION_METHODS = {"ALL", "CID", "HCD", "ETD", "ECD"}

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
