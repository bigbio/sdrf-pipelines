"""Utility functions for OpenMS conversion."""

import logging

logger = logging.getLogger(__name__)


def infer_tmtplex(label_set: set) -> str:
    """Infer the TMT plex type from a set of labels.

    Args:
        label_set: Set of label strings (e.g., {"TMT126", "TMT127N"})

    Returns:
        TMT plex type string (e.g., "tmt18plex", "tmt10plex")
    """
    if len(label_set) > 16 or "TMT134C" in label_set or "TMT135N" in label_set:
        return "tmt18plex"
    elif (
        len(label_set) > 11
        or "TMT134N" in label_set
        or "TMT133C" in label_set
        or "TMT133N" in label_set
        or "TMT132C" in label_set
        or "TMT132N" in label_set
    ):
        return "tmt16plex"
    elif len(label_set) == 11 or "TMT131C" in label_set:
        return "tmt11plex"
    elif len(label_set) > 6:
        return "tmt10plex"
    else:
        return "tmt6plex"


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
     for current_extension, target_extension in extension_convert_dict.items():
         if raw.lower().endswith(current_extension.lower()):
             # Handle case-insensitive suffix removal
             suffix_len = len(current_extension)
             raw = raw[:-suffix_len] if raw[-suffix_len:].lower() == current_extension.lower() else raw
             raw += target_extension
             if not any(raw.endswith(x) for x in supported_extensions):
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
