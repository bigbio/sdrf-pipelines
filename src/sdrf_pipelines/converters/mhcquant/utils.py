"""Utility functions for MHCquant conversion."""

import re
from typing import TypedDict

import pandas as pd

from sdrf_pipelines.converters.mhcquant.constants import (
    EMPTY_VALUES,
    MHC_CLASS_PEPTIDE_LENGTHS,
    PRESET_COLUMNS,
)

__all__ = [
    "PresetDict",
    "SearchParams",
    "build_custom_preset",
    "extract_nt_value",
    "find_matching_preset",
    "get_column_value",
    "get_sample_name",
    "normalize_preset_value",
    "parse_mhc_class",
    "ppm_to_da",
    "presets_match",
    "resolve_fragment_tolerance",
    "strip_unit",
    "write_presets",
]


class SearchParams(TypedDict, total=False):
    """Search parameters extracted from an SDRF row."""

    mhc_class: str
    precursor_mass_tolerance: float
    precursor_error_unit: str
    fragment_mass_tolerance: float
    fragment_bin_offset: float
    precursor_mass_range: str
    precursor_charge: str
    activation_method: str
    instrument_name: str
    ms2_analyzer: str
    instrument_resolution: str
    ms2pip_model: str
    fixed_mods: str
    variable_mods: str
    number_mods: int


class PresetDict(TypedDict, total=False):
    """A single search preset row."""

    PresetName: str
    PeptideMinLength: int
    PeptideMaxLength: int
    PrecursorMassRange: str
    PrecursorCharge: str
    PrecursorMassTolerance: float
    PrecursorErrorUnit: str
    FragmentMassTolerance: float
    FragmentBinOffset: float
    MS2PIPModel: str
    ActivationMethod: str
    Instrument: str
    NumberMods: int
    FixedMods: str
    VariableMods: str


def ppm_to_da(ppm: float, reference_mass: float = 1000.0) -> float:
    """Convert a ppm tolerance to Da using a reference mass.

    MHCquant only supports Da for fragment mass tolerance, so ppm values
    must be converted. Uses reference_mass=1000 Da as a representative
    fragment ion mass for MHC peptides.

    See: https://gist.github.com/RalfG/a74448fad9a0766a8e5afbbe3a6541d7
    """
    return ppm * reference_mass / 1_000_000


def get_column_value(row: pd.Series, column_name: str) -> str | None:
    """Get a column value, returning None if missing or empty."""
    if column_name not in row.index:
        return None
    val = str(row[column_name])
    if val.lower() in EMPTY_VALUES:
        return None
    return val


def get_sample_name(row: pd.Series, factor_cols: list[str], source_name: str) -> str:
    """Extract sample name from factor value columns."""
    if factor_cols:
        val = str(row[factor_cols[0]])
        if val.lower() not in EMPTY_VALUES:
            return val.strip().replace(" ", "_")
    return source_name.strip().replace(" ", "_")


def parse_mhc_class(value: str) -> str:
    """Parse MHC class from a string value."""
    lower = value.lower().replace(" ", "")
    if "classii" in lower or "class2" in lower:
        return "class2"
    if "classi" in lower or "class1" in lower:
        return "class1"
    raise ValueError(f"Unrecognized MHC class '{value}'. Expected 'class I' or 'class II'.")


def strip_unit(value: str) -> str:
    """Strip unit suffix from a value like '300m/z' -> '300'."""
    match = re.match(r"([\d.]+)", value.strip())
    return match.group(1) if match else value.strip()


def extract_nt_value(value: str) -> str:
    """Extract the NT= value from an SDRF field like 'NT=HCD;AC=MS:1000422'."""
    match = re.search(r"NT=([^;]+)", value)
    return match.group(1).strip() if match else value.strip()


def resolve_fragment_tolerance(
    instrument_name: str,
    ms2_analyzer: str,
    fragment_mass_tolerance: float,
) -> tuple[str, float, float]:
    """Determine resolution and adjust fragment tolerance for Comet.

    Returns (resolution, adjusted_tolerance, bin_offset).

    Resolution is determined as low_res when any of:
    - MS2 analyzer is ion trap / linear trap
    - Instrument contains "xl" or "ltq" without orbitrap MS2 analyzer
    - Fragment mass tolerance >= 0.1 Da

    Fragment tolerance adjustment:
    - Low-res: Comet requires fixed 0.50025 Da / 0.4 offset
    - High-res: Comet bins at 2x tolerance, so halve the value / 0.0 offset
    """
    lower_analyzer = ms2_analyzer.lower()
    lower_instrument = instrument_name.lower()

    if (
        "ion trap" in lower_analyzer
        or "linear trap" in lower_analyzer
        or (("xl" in lower_instrument or "ltq" in lower_instrument) and "orbitrap" not in lower_analyzer)
        or fragment_mass_tolerance >= 0.1
    ):
        return "low_res", 0.50025, 0.4

    return "high_res", fragment_mass_tolerance / 2, 0.0


def normalize_preset_value(value: str) -> str:
    """Normalize a preset value for comparison."""
    value = value.strip()
    try:
        return str(float(value))
    except ValueError:
        return value


def presets_match(preset_a: dict, preset_b: dict) -> bool:
    """Check if two presets have the same parameter values (ignoring name)."""
    keys = [k for k in PRESET_COLUMNS if k != "PresetName"]
    return all(
        normalize_preset_value(str(preset_a.get(k, ""))) == normalize_preset_value(str(preset_b.get(k, "")))
        for k in keys
    )


def find_matching_preset(
    preset_dict: dict,
    existing_presets: dict[str, dict],
    defaults: dict[str, dict],
) -> tuple[str, dict] | None:
    """Find a matching preset in existing presets or defaults."""
    for name, existing in existing_presets.items():
        if presets_match(preset_dict, existing):
            return name, existing
    for name, default in defaults.items():
        if presets_match(preset_dict, default):
            return name, default
    return None


def build_custom_preset(params: SearchParams) -> dict:
    """Build a custom preset dict from extracted search params."""
    min_len, max_len = MHC_CLASS_PEPTIDE_LENGTHS[params["mhc_class"]]
    return {
        "PresetName": "",
        "PeptideMinLength": min_len,
        "PeptideMaxLength": max_len,
        "PrecursorMassRange": params["precursor_mass_range"],
        "PrecursorCharge": params["precursor_charge"],
        "PrecursorMassTolerance": params["precursor_mass_tolerance"],
        "PrecursorErrorUnit": params["precursor_error_unit"],
        "FragmentMassTolerance": params["fragment_mass_tolerance"],
        "FragmentBinOffset": params["fragment_bin_offset"],
        "MS2PIPModel": params["ms2pip_model"],
        "ActivationMethod": params["activation_method"],
        "Instrument": params["instrument_resolution"],
        "NumberMods": params["number_mods"],
        "FixedMods": params["fixed_mods"],
        "VariableMods": params["variable_mods"],
    }


def write_presets(presets: dict[str, dict], output_path: str) -> None:
    """Write the search presets TSV."""
    rows = []
    for preset_dict in presets.values():
        row = {}
        for col in PRESET_COLUMNS:
            val = str(preset_dict.get(col, ""))
            if col in ("FixedMods", "VariableMods") and val.strip() == "":
                val = " "
            row[col] = val
        rows.append(row)
    df = pd.DataFrame(rows, columns=PRESET_COLUMNS)
    df.to_csv(output_path, sep="\t", index=False)
