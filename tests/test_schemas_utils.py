"""Tests for the schemas utilities module."""

import pytest

from sdrf_pipelines.sdrf.schemas.models import (
    ColumnDefinition,
    MergeStrategy,
    RequirementLevel,
    ValidatorConfig,
)
from sdrf_pipelines.sdrf.schemas.utils import merge_column_defs, schema_to_tsv
from sdrf_pipelines.sdrf.schemas.models import SchemaDefinition


class TestMergeColumnDefs:
    """Tests for the merge_column_defs function."""

    def test_merge_single_column(self):
        """Test merging a single column definition."""
        col = ColumnDefinition(
            name="source name",
            description="The source name",
            requirement=RequirementLevel.REQUIRED,
        )
        result = merge_column_defs([col])
        assert result.name == "source name"
        assert result.requirement == RequirementLevel.REQUIRED
        assert result.description == "The source name"

    def test_merge_combine_strategy_validators(self):
        """Test combining validators with COMBINE strategy."""
        col1 = ColumnDefinition(
            name="test",
            description="Test column",
            requirement=RequirementLevel.REQUIRED,
            validators=[
                ValidatorConfig(validator_name="ontology", params={"ontologies": ["efo"]})
            ],
        )
        col2 = ColumnDefinition(
            name="test",
            description="Another description",
            requirement=RequirementLevel.OPTIONAL,
            validators=[
                ValidatorConfig(validator_name="pattern", params={"pattern": ".*"})
            ],
        )
        result = merge_column_defs([col1, col2], strategy=MergeStrategy.COMBINE)

        # Should combine validators from both
        assert len(result.validators) == 2
        validator_names = [v.validator_name for v in result.validators]
        assert "ontology" in validator_names
        assert "pattern" in validator_names

    def test_merge_combine_strategy_requirement(self):
        """Test that COMBINE strategy keeps non-OPTIONAL requirement."""
        col1 = ColumnDefinition(
            name="test",
            description="Test column",
            requirement=RequirementLevel.OPTIONAL,
        )
        col2 = ColumnDefinition(
            name="test",
            description="Test column",
            requirement=RequirementLevel.REQUIRED,
        )
        result = merge_column_defs([col1, col2], strategy=MergeStrategy.COMBINE)
        assert result.requirement == RequirementLevel.REQUIRED

    def test_merge_first_strategy(self):
        """Test FIRST strategy uses first column's values."""
        col1 = ColumnDefinition(
            name="test",
            description="First description",
            requirement=RequirementLevel.REQUIRED,
        )
        col2 = ColumnDefinition(
            name="test",
            description="Second description",
            requirement=RequirementLevel.OPTIONAL,
        )
        result = merge_column_defs([col1, col2], strategy=MergeStrategy.FIRST)
        assert result.requirement == RequirementLevel.REQUIRED
        assert result.description == "First description"

    def test_merge_last_strategy(self):
        """Test LAST strategy uses last column's values."""
        col1 = ColumnDefinition(
            name="test",
            description="First description",
            requirement=RequirementLevel.REQUIRED,
        )
        col2 = ColumnDefinition(
            name="test",
            description="Second description",
            requirement=RequirementLevel.OPTIONAL,
        )
        result = merge_column_defs([col1, col2], strategy=MergeStrategy.LAST)
        assert result.requirement == RequirementLevel.OPTIONAL
        assert result.description == "Second description"

    def test_merge_empty_list_raises_error(self):
        """Test that merging empty list raises ValueError."""
        with pytest.raises(ValueError, match="No column definitions to merge"):
            merge_column_defs([])

    def test_merge_allow_not_applicable(self):
        """Test merging allow_not_applicable flag."""
        col1 = ColumnDefinition(
            name="test",
            description="Test",
            requirement=RequirementLevel.OPTIONAL,
            allow_not_applicable=False,
        )
        col2 = ColumnDefinition(
            name="test",
            description="Test",
            requirement=RequirementLevel.OPTIONAL,
            allow_not_applicable=True,
        )
        # COMBINE should be permissive (True if any is True)
        result = merge_column_defs([col1, col2], strategy=MergeStrategy.COMBINE)
        assert result.allow_not_applicable is True

    def test_merge_allow_not_available(self):
        """Test merging allow_not_available flag."""
        col1 = ColumnDefinition(
            name="test",
            description="Test",
            requirement=RequirementLevel.OPTIONAL,
            allow_not_available=True,
        )
        col2 = ColumnDefinition(
            name="test",
            description="Test",
            requirement=RequirementLevel.OPTIONAL,
            allow_not_available=False,
        )
        # COMBINE should be permissive (True if any is True)
        result = merge_column_defs([col1, col2], strategy=MergeStrategy.COMBINE)
        assert result.allow_not_available is True

    def test_merge_deduplicate_validators(self):
        """Test that duplicate validators are removed when merging."""
        validator = ValidatorConfig(validator_name="ontology", params={"ontologies": ["efo"]})
        col1 = ColumnDefinition(
            name="test",
            description="Test",
            requirement=RequirementLevel.OPTIONAL,
            validators=[validator],
        )
        col2 = ColumnDefinition(
            name="test",
            description="Test",
            requirement=RequirementLevel.OPTIONAL,
            validators=[validator],
        )
        result = merge_column_defs([col1, col2], strategy=MergeStrategy.COMBINE)
        # Should deduplicate validators with the same name
        assert len(result.validators) == 1


class TestSchemaToTsv:
    """Tests for the schema_to_tsv function."""

    def test_schema_to_tsv_basic(self):
        """Test converting schema to TSV format."""
        schema = SchemaDefinition(
            name="test",
            description="Test schema",
            columns=[
                ColumnDefinition(
                    name="source name",
                    description="Source name",
                    requirement=RequirementLevel.REQUIRED,
                ),
                ColumnDefinition(
                    name="characteristics[organism]",
                    description="Organism",
                    requirement=RequirementLevel.REQUIRED,
                ),
            ],
        )
        result = schema_to_tsv(schema)
        assert result == "source name\tcharacteristics[organism]\n"

    def test_schema_to_tsv_empty(self):
        """Test converting empty schema to TSV."""
        schema = SchemaDefinition(
            name="empty",
            description="Empty schema",
            columns=[],
        )
        result = schema_to_tsv(schema)
        assert result == "\n"
