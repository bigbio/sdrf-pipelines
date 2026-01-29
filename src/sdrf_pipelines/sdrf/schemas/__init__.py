"""SDRF schema definitions and validation.

This package provides schema-based validation for SDRF files.
"""

from sdrf_pipelines.sdrf.schemas.models import (
    ColumnDefinition,
    MergeStrategy,
    RequirementLevel,
    SchemaDefinition,
    ValidatorConfig,
)
from sdrf_pipelines.sdrf.schemas.registry import SchemaRegistry
from sdrf_pipelines.sdrf.schemas.utils import (
    load_and_validate_sdrf,
    merge_column_defs,
    schema_to_tsv,
)
from sdrf_pipelines.sdrf.schemas.validator import SchemaValidator

__all__ = [
    # Models
    "ColumnDefinition",
    "MergeStrategy",
    "RequirementLevel",
    "SchemaDefinition",
    "ValidatorConfig",
    # Registry
    "SchemaRegistry",
    # Validator
    "SchemaValidator",
    # Utils
    "load_and_validate_sdrf",
    "merge_column_defs",
    "schema_to_tsv",
]
