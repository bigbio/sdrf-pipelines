"""OpenMS SDRF conversion module."""

from sdrf_pipelines.converters.openms.constants import (
    ENZYME_MAPPINGS,
    ITRAQ_4PLEX,
    ITRAQ_8PLEX,
    ITRAQ_DEFAULT_MODS,
    SILAC_2PLEX,
    SILAC_3PLEX,
    TMT_DEFAULT_MODS,
    TMT_PLEXES,
)
from sdrf_pipelines.converters.openms.experimental_design import ExperimentalDesignWriter
from sdrf_pipelines.converters.openms.modifications import ModificationConverter
from sdrf_pipelines.converters.openms.openms import OpenMS
from sdrf_pipelines.converters.openms.utils import (
    FileToColumnEntries,
    get_openms_file_name,
    infer_tmtplex,
    parse_tolerance,
)

__all__ = [
    # Main class
    "OpenMS",
    # Helper classes
    "ExperimentalDesignWriter",
    "ModificationConverter",
    "FileToColumnEntries",
    # Constants
    "ENZYME_MAPPINGS",
    "ITRAQ_4PLEX",
    "ITRAQ_8PLEX",
    "ITRAQ_DEFAULT_MODS",
    "SILAC_2PLEX",
    "SILAC_3PLEX",
    "TMT_DEFAULT_MODS",
    "TMT_PLEXES",
    # Utility functions
    "get_openms_file_name",
    "infer_tmtplex",
    "parse_tolerance",
]
