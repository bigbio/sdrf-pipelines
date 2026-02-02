"""Central configuration for sdrf-pipelines.

This module provides default configuration values used throughout the package.
Users can override these by modifying the values after import.
"""

from dataclasses import dataclass, field


@dataclass
class ValidationConfig:
    """Configuration for SDRF validation."""

    # Minimum number of columns required for a valid MS-proteomics SDRF
    minimum_columns: int = 12

    # Default templates to use for validation
    default_templates: list[str] = field(default_factory=lambda: ["ms-proteomics"])


@dataclass
class OpenMSConfig:
    """Configuration for OpenMS converter."""

    # Default precursor mass tolerance
    default_precursor_tolerance: str = "10"
    default_precursor_tolerance_unit: str = "ppm"

    # Default fragment mass tolerance
    default_fragment_tolerance: str = "20"
    default_fragment_tolerance_unit: str = "ppm"

    # Default dissociation method
    default_dissociation_method: str = "HCD"


@dataclass
class OntologyConfig:
    """Configuration for ontology validation."""

    # Cache directory name (under system cache)
    cache_dir_name: str = "sdrf-pipelines/ontologies"

    # GitHub branch for downloading ontology files
    ontology_branch: str = "main"

    # API retry settings
    api_retry_count: int = 10
    api_rows_per_page: int = 10


@dataclass
class Config:
    """Main configuration class combining all settings."""

    validation: ValidationConfig = field(default_factory=ValidationConfig)
    openms: OpenMSConfig = field(default_factory=OpenMSConfig)
    ontology: OntologyConfig = field(default_factory=OntologyConfig)


# Global configuration instance
# Users can modify this to customize behavior:
#   from sdrf_pipelines.config import config
#   config.validation.minimum_columns = 15
config = Config()


# For backwards compatibility, export individual values
# NOTE: These are snapshots at import time. For dynamic access, use config.validation.* directly.
MINIMUM_COLUMNS = config.validation.minimum_columns
DEFAULT_TEMPLATES = config.validation.default_templates
