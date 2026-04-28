"""Regression tests for schema loading."""

from sdrf_pipelines.sdrf.schemas import SchemaRegistry


def test_schema_registry_normalizes_null_validator_params():
    """Templates with validators that omit params should still load cleanly."""
    registry = SchemaRegistry()

    assert "ms-metabolomics" in registry.schemas

    has_empty_params = any(
        validator.params == {}
        for schema in registry.schemas.values()
        for column in schema.columns
        for validator in column.validators
    )
    assert has_empty_params
