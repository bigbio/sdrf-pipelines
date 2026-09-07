"""Utility functions for OpenMS conversion."""

import logging
import re
from collections.abc import Collection

from sdrf_pipelines.converters.openms.constants import CHANNEL_MAP

logger = logging.getLogger(__name__)


def parse_openms_label(value: str) -> str:
    """Read a plain or NT-encoded channel name without discarding other rows."""
    match = re.search(r"(?:^|;)\s*NT=([^;]+)", value, flags=re.IGNORECASE)
    label = (match.group(1) if match else value).strip()
    for channels in CHANNEL_MAP.values():
        for canonical in channels:
            if label.casefold() == canonical.casefold():
                return "label free sample" if canonical == "LFQ" else canonical
    return label


def _infer_plex(label_set: Collection[str], plex_prefix: str) -> str:
    """Return the smallest plex whose canonical labels contain ``label_set``."""
    labels = {label.casefold() for label in label_set}
    plexes: list[tuple[str, set[str]]] = [
        (plex, {label.casefold() for label in channels})
        for plex, channels in CHANNEL_MAP.items()
        if plex.startswith(plex_prefix)
    ]
    for plex, channels in sorted(plexes, key=lambda item: len(item[1])):
        if labels.issubset(channels):
            return plex
    raise ValueError(f"Cannot infer {plex_prefix.upper()} plex from labels: {sorted(labels)}")


def infer_tmtplex(label_set: Collection[str]) -> str:
    """Infer the TMT plex type from a set of labels.

    Args:
        label_set: Collection of canonical label strings (e.g., {"TMT126", "TMT127N"})

    Returns:
        Smallest compatible TMT plex type string (e.g., "tmt18plex", "tmt10plex")
    """
    return _infer_plex(label_set, "tmt")


def infer_itraqplex(label_set: Collection[str]) -> str:
    """Infer the smallest compatible iTRAQ plex from canonical label strings."""
    return _infer_plex(label_set, "itraq")


def get_openms_file_name(raw: str, extension_convert: str | None = None) -> str:
    """Convert file name for OpenMS.

    Supports extension conversion via patterns like "raw:mzML" or "d:mzML".

    Args:
        raw: Raw file name
        extension_convert: Comma-separated extension conversion patterns
            Examples: "raw:mzML", "mzML:mzml", "d:mzML"

    Returns:
        Converted file name

    Raises:
        RuntimeError: If the converted file has an unsupported extension
    """
    if extension_convert is None:
        return raw

    supported_extensions = ["raw", "mzML", "mzml", "d"]
    extension_convert_list = extension_convert.split(",")
    extension_convert_dict = {}

    for ext_convert in extension_convert_list:
        current_extension, new_extension = ext_convert.split(":")
        extension_convert_dict[current_extension] = new_extension

    raw_bkp = raw
    for current_extension, target_extension in extension_convert_dict.items():
        if raw.lower().endswith(current_extension.lower()):
            # Handle case-insensitive suffix removal
            suffix_len = len(current_extension)
            raw = raw[:-suffix_len] if raw[-suffix_len:].lower() == current_extension.lower() else raw
            raw += target_extension
            if not any(raw.endswith(x) for x in supported_extensions):
                raise RuntimeError(
                    f"Error converting extension, {raw_bkp} -> {raw},"
                    " the ending file does not have any of the supported"
                    f" extensions {supported_extensions}"
                )
            return raw

    return raw


def parse_tolerance(pc_tol_str: str, units: tuple[str, ...] = ("ppm", "da", "mmu")) -> tuple[str | None, str | None]:
    """Parse tolerance value and unit from a string.

    Args:
        pc_tol_str: Tolerance string (e.g., "10 ppm", "0.5 Da")
        units: Tuple of unit strings to look for

    Returns:
        Tuple of (tolerance value, unit) or (None, None) if not found
    """
    pc_tol_str = pc_tol_str.lower()
    for unit in units:
        if unit in pc_tol_str:
            tol = pc_tol_str.split(unit)[0].strip()
            if f" {unit}" not in pc_tol_str:
                msg = f"Missing whitespace in precursor mass tolerance: {pc_tol_str} Adding it: {tol} {unit}"
                logger.warning(msg)
            _ = float(tol)  # Validate it's a number
            if unit == "da":
                unit = "Da"
            if unit == "mmu":
                # Convert mmu to Da
                tol, unit = str(float(tol) * 0.001), "Da"
            return tol, unit
    return None, None


class FileToColumnEntries:
    """Container for mapping file names to column entries during conversion."""

    def __init__(self):
        self.file2mods: dict[str, tuple] = {}
        self.file2pctol: dict[str, str] = {}
        self.file2pctolunit: dict[str, str] = {}
        self.file2fragtol: dict[str, str] = {}
        self.file2fragtolunit: dict[str, str] = {}
        self.file2diss: dict[str, str] = {}
        self.file2enzyme: dict[str, str] = {}
        self.file2source: dict[str, str] = {}
        self.file2label: dict[str, list[str]] = {}
        self.file2fraction: dict[str, str] = {}
        self.file2combined_factors: dict[str, str] = {}
        self.file2technical_rep: dict[str, str] = {}
        self.file2fraction_group: dict[str, int] = {}
