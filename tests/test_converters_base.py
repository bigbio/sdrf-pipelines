"""Tests for the converters base module."""

import pandas as pd
import pytest

from sdrf_pipelines.converters.base import (
    SAMPLE_IDENTIFIER_PATTERN,
    BaseConverter,
    ConditionBuilder,
    SampleTracker,
)


class TestSampleIdentifierPattern:
    """Tests for the SAMPLE_IDENTIFIER_PATTERN regex."""

    def test_matches_sample_number(self):
        """Test that the pattern matches 'sample N' format."""
        match = SAMPLE_IDENTIFIER_PATTERN.search("Sample 1")
        assert match is not None
        assert match.group(1) == "1"

    def test_matches_case_insensitive(self):
        """Test that the pattern is case insensitive."""
        match = SAMPLE_IDENTIFIER_PATTERN.search("SAMPLE 123")
        assert match is not None
        assert match.group(1) == "123"

    def test_matches_at_end_of_string(self):
        """Test that the pattern matches at end of string."""
        match = SAMPLE_IDENTIFIER_PATTERN.search("My Sample 42")
        assert match is not None
        assert match.group(1) == "42"

    def test_no_match_without_number(self):
        """Test that the pattern doesn't match without a number."""
        match = SAMPLE_IDENTIFIER_PATTERN.search("Sample")
        assert match is None

    def test_no_match_middle_of_string(self):
        """Test that the pattern doesn't match in the middle of a string."""
        match = SAMPLE_IDENTIFIER_PATTERN.search("Sample 1 extra")
        assert match is None


class ConcreteConverter(BaseConverter):
    """Concrete implementation for testing BaseConverter."""

    def convert(self, sdrf_file: str, output_path: str, **kwargs) -> None:
        """Dummy convert method for testing."""
        pass


class TestBaseConverter:
    """Tests for the BaseConverter class."""

    def test_init(self):
        """Test that BaseConverter initializes correctly."""
        converter = ConcreteConverter()
        assert converter.warnings == {}
        assert converter._sdrf is None

    def test_add_warning(self):
        """Test adding warnings."""
        converter = ConcreteConverter()
        converter.add_warning("Test warning")
        assert converter.warnings["Test warning"] == 1
        converter.add_warning("Test warning")
        assert converter.warnings["Test warning"] == 2

    def test_parse_split_columns_none(self):
        """Test parsing None split_by_columns."""
        converter = ConcreteConverter()
        result = converter.parse_split_columns(None)
        assert result is None

    def test_parse_split_columns_empty(self):
        """Test parsing empty split_by_columns."""
        converter = ConcreteConverter()
        result = converter.parse_split_columns("")
        assert result is None

    def test_parse_split_columns_single(self):
        """Test parsing single column."""
        converter = ConcreteConverter()
        result = converter.parse_split_columns("[col1]")
        assert result == ["col1"]

    def test_parse_split_columns_multiple(self):
        """Test parsing multiple columns."""
        converter = ConcreteConverter()
        result = converter.parse_split_columns("[Col1, Col2, Col3]")
        assert result == ["col1", "col2", "col3"]

    def test_get_factor_columns(self):
        """Test extracting factor columns."""
        converter = ConcreteConverter()
        df = pd.DataFrame(
            {
                "source name": ["s1"],
                "factor value[treatment]": ["ctrl"],
                "factor value[time]": ["0h"],
                "comment[data file]": ["file1.raw"],
            }
        )
        factor_cols = converter.get_factor_columns(df)
        assert len(factor_cols) == 2
        assert "factor value[treatment]" in factor_cols
        assert "factor value[time]" in factor_cols

    def test_get_characteristics_columns(self):
        """Test extracting characteristics columns."""
        converter = ConcreteConverter()
        df = pd.DataFrame(
            {
                "source name": ["s1"],
                "characteristics[organism]": ["human"],
                "characteristics[tissue]": ["liver"],
                "comment[data file]": ["file1.raw"],
            }
        )
        char_cols = converter.get_characteristics_columns(df)
        assert len(char_cols) == 2
        assert "characteristics[organism]" in char_cols
        assert "characteristics[tissue]" in char_cols

    def test_combine_factors_to_conditions_with_factors(self):
        """Test combining factors with available factor columns."""
        converter = ConcreteConverter()
        factor_cols = ["factor value[treatment]", "factor value[time]"]
        row = pd.Series(
            {
                "source name": "sample 1",
                "factor value[treatment]": "drug",
                "factor value[time]": "24h",
            }
        )
        result = converter.combine_factors_to_conditions(factor_cols, row)
        assert result == "drug_24h"

    def test_combine_factors_to_conditions_empty(self):
        """Test combining factors with no factor columns."""
        converter = ConcreteConverter()
        row = pd.Series({"source name": "sample 1"})
        result = converter.combine_factors_to_conditions([], row)
        assert result == "sample 1"
        assert "No factors specified" in list(converter.warnings.keys())[0]

    def test_extract_sample_id_with_pattern(self):
        """Test extracting sample ID from source name with pattern."""
        converter = ConcreteConverter()
        sample_id, next_id = converter.extract_sample_id("Sample 42", {}, 1)
        assert sample_id == "42"
        assert next_id == 1

    def test_extract_sample_id_without_pattern(self):
        """Test extracting sample ID without pattern match."""
        converter = ConcreteConverter()
        sample_id_map: dict[str, int] = {}
        sample_id, next_id = converter.extract_sample_id("custom_source", sample_id_map, 1)
        assert sample_id == "1"
        assert next_id == 2
        assert "No sample number identifier" in list(converter.warnings.keys())[0]

    def test_get_technical_replicate_present(self):
        """Test getting technical replicate when present."""
        converter = ConcreteConverter()
        row = pd.Series({"comment[technical replicate]": "2"})
        result = converter.get_technical_replicate(row)
        assert result == "2"

    def test_get_technical_replicate_default(self):
        """Test getting technical replicate with default."""
        converter = ConcreteConverter()
        row = pd.Series({"source name": "s1"})
        result = converter.get_technical_replicate(row)
        assert result == "1"

    def test_get_fraction_identifier_present(self):
        """Test getting fraction identifier when present."""
        converter = ConcreteConverter()
        row = pd.Series({"comment[fraction identifier]": "3"})
        result = converter.get_fraction_identifier(row)
        assert result == "3"

    def test_get_fraction_identifier_default(self):
        """Test getting fraction identifier with default."""
        converter = ConcreteConverter()
        row = pd.Series({"source name": "s1"})
        result = converter.get_fraction_identifier(row)
        assert result == "1"


class TestConditionBuilder:
    """Tests for the ConditionBuilder class."""

    def test_init(self):
        """Test ConditionBuilder initialization."""
        builder = ConditionBuilder(["factor value[treatment]"])
        assert builder.factor_cols == ["factor value[treatment]"]
        assert builder.separator == "_"
        assert builder.conditions == []

    def test_add_from_row(self):
        """Test adding condition from row."""
        builder = ConditionBuilder(["factor value[treatment]", "factor value[time]"])
        row = pd.Series(
            {"factor value[treatment]": "drug", "factor value[time]": "24h"}
        )
        condition = builder.add_from_row(row)
        assert condition == "drug_24h"
        assert builder.conditions == ["drug_24h"]

    def test_add_from_row_with_fallback(self):
        """Test adding condition with fallback when no factors."""
        builder = ConditionBuilder([])
        row = pd.Series({"source name": "sample 1"})
        condition = builder.add_from_row(row, fallback="default")
        assert condition == "default"

    def test_get_unique_conditions(self):
        """Test getting unique conditions."""
        builder = ConditionBuilder(["factor value[treatment]"])
        builder.conditions = ["ctrl", "drug", "ctrl", "drug", "ctrl"]
        unique = builder.get_unique_conditions()
        assert unique == ["ctrl", "drug"]


class TestSampleTracker:
    """Tests for the SampleTracker class."""

    def test_init(self):
        """Test SampleTracker initialization."""
        tracker = SampleTracker()
        assert tracker.sample_id_map == {}
        assert tracker.bio_replicates == []
        assert tracker._next_id == 1

    def test_get_sample_id_with_pattern(self):
        """Test getting sample ID with pattern match."""
        tracker = SampleTracker()
        sample_id = tracker.get_sample_id("Sample 5")
        assert sample_id == "5"
        assert "5" in tracker.bio_replicates

    def test_get_sample_id_without_pattern(self):
        """Test getting sample ID without pattern match."""
        tracker = SampleTracker()
        sample_id = tracker.get_sample_id("custom_sample")
        assert sample_id == "1"
        assert tracker.sample_id_map["custom_sample"] == 1

    def test_get_sample_id_reuse_mapping(self):
        """Test reusing sample ID mapping."""
        tracker = SampleTracker()
        id1 = tracker.get_sample_id("custom_sample")
        id2 = tracker.get_sample_id("custom_sample")
        assert id1 == id2 == "1"

    def test_get_bio_replicate_index(self):
        """Test getting bio-replicate index."""
        tracker = SampleTracker()
        tracker.bio_replicates = ["1", "2", "3"]
        index = tracker.get_bio_replicate_index("2")
        assert index == 2

    def test_get_bio_replicate_index_new(self):
        """Test getting bio-replicate index for new sample."""
        tracker = SampleTracker()
        index = tracker.get_bio_replicate_index("1")
        assert index == 1
        assert "1" in tracker.bio_replicates
