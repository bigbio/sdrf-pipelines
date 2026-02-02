"""Utility functions for SDRF schema operations."""

import pandas as pd

from sdrf_pipelines.sdrf.schemas.models import (
    ColumnDefinition,
    MergeStrategy,
    RequirementLevel,
    SchemaDefinition,
)

# Requirement level ordering for "stricter" comparison
_REQUIREMENT_ORDER = {
    RequirementLevel.OPTIONAL: 0,
    RequirementLevel.RECOMMENDED: 1,
    RequirementLevel.REQUIRED: 2,
}


def _merge_fields_first_strategy(merged: ColumnDefinition, col_def: ColumnDefinition) -> None:
    """Merge fields using FIRST strategy - use first non-null value."""
    for field in ["description", "requirement", "allow_not_applicable", "allow_not_available"]:
        if getattr(merged, field) is None and getattr(col_def, field) is not None:
            setattr(merged, field, getattr(col_def, field))


def _merge_fields_last_strategy(merged: ColumnDefinition, col_def: ColumnDefinition) -> None:
    """Merge fields using LAST strategy - use last non-null value."""
    for field in ["description", "requirement", "allow_not_applicable", "allow_not_available"]:
        if getattr(col_def, field) is not None:
            setattr(merged, field, getattr(col_def, field))


def _merge_fields_combine_strategy(merged: ColumnDefinition, col_def: ColumnDefinition) -> None:
    """Merge fields using COMBINE strategy - combine descriptions and use stricter requirement level."""
    # Merge description field
    if col_def.description and col_def.description not in (None, ""):
        if merged.description:
            combined = f"{merged.description}; {col_def.description}"
            merged.description = combined
        else:
            merged.description = col_def.description

    # Merge requirement field - use stricter level
    if col_def.requirement:
        if _REQUIREMENT_ORDER.get(col_def.requirement, 0) > _REQUIREMENT_ORDER.get(merged.requirement, 0):
            merged.requirement = col_def.requirement

    # Merge allow_not_applicable and allow_not_available fields
    for field in ["allow_not_applicable", "allow_not_available"]:
        if getattr(col_def, field):
            setattr(merged, field, True)


def _merge_validators(merged: ColumnDefinition, col_def: ColumnDefinition) -> None:
    """Merge validators without duplicates."""
    existing_validators = {v.validator_name: v for v in merged.validators}
    for v in col_def.validators:
        if v.validator_name not in existing_validators:
            merged.validators.append(v)


def merge_column_defs(
    col_defs: list[ColumnDefinition], strategy: MergeStrategy = MergeStrategy.LAST
) -> ColumnDefinition:
    """Merge multiple ColumnDefinition instances into one, combining validators.

    Args:
        col_defs: List of ColumnDefinition instances to merge
        strategy: Strategy for merging basic properties ('first', 'last', 'combine')

    Returns:
        Merged ColumnDefinition
    """
    if not col_defs:
        raise ValueError("No column definitions to merge")

    merged = col_defs[0].model_copy(deep=True)

    for col_def in col_defs[1:]:
        if strategy == MergeStrategy.FIRST:
            _merge_fields_first_strategy(merged, col_def)
        elif strategy == MergeStrategy.LAST:
            _merge_fields_last_strategy(merged, col_def)
        elif strategy == MergeStrategy.COMBINE:
            _merge_fields_combine_strategy(merged, col_def)

        _merge_validators(merged, col_def)

    return merged


def schema_to_tsv(schema: SchemaDefinition) -> str:
    """Convert SDRF schema definition to TSV format.

    Args:
        schema: SchemaDefinition instance

    Returns:
        TSV string with header line
    """
    header_list = [col.name for col in schema.columns]
    tsv_content = "\t".join(header_list) + "\n"
    return tsv_content


def load_and_validate_sdrf(
    sdrf_path: str,
    schema_dir: str | None = None,
    schema_name: str | None = None,
    use_versioned: bool = True,
    template_versions: dict[str, str] | None = None,
):
    """Load an SDRF file and validate it against a schema.

    Args:
        sdrf_path: Path to the SDRF file
        schema_dir: Path to directory containing schema files
        schema_name: Name of the schema to use for validation, if None will use best matching schema
        use_versioned: If True, load templates from versioned sdrf-templates structure
        template_versions: Dict mapping template names to specific versions to use

    Returns:
        Tuple of (DataFrame, validation results)
    """
    # Import here to avoid circular imports
    from sdrf_pipelines.sdrf.schemas.registry import SchemaRegistry
    from sdrf_pipelines.sdrf.schemas.validator import SchemaValidator

    df = pd.read_csv(sdrf_path, sep="\t")

    registry = SchemaRegistry(schema_dir, use_versioned=use_versioned, template_versions=template_versions)
    validator = SchemaValidator(registry)

    if schema_name:
        errors = validator.validate(df, schema_name)
        return df, {"schema": schema_name, "errors": errors}
    else:
        results = validator.validate_with_best_schema(df)
        return df, results
