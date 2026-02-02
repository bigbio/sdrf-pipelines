"""Tests for the machine-readable error codes and validation manifest."""

import json
import logging

import pytest

from sdrf_pipelines.utils.error_codes import (
    ErrorCategory,
    ErrorCode,
    format_error_message,
)
from sdrf_pipelines.utils.exceptions import LogicError
from sdrf_pipelines.utils.manifest import ValidationManifest


class TestErrorCode:
    """Test ErrorCode enum and helpers."""

    def test_error_code_values(self):
        """Test that error codes have expected string values."""
        assert ErrorCode.MISSING_REQUIRED_COLUMN.value == "MISSING_REQUIRED_COLUMN"
        assert ErrorCode.TRAILING_WHITESPACE.value == "TRAILING_WHITESPACE"
        assert ErrorCode.ONTOLOGY_TERM_NOT_FOUND.value == "ONTOLOGY_TERM_NOT_FOUND"

    def test_error_code_category(self):
        """Test that error codes map to correct categories."""
        assert ErrorCode.MISSING_REQUIRED_COLUMN.category == ErrorCategory.STRUCTURE
        assert ErrorCode.TRAILING_WHITESPACE.category == ErrorCategory.FORMAT
        assert ErrorCode.ONTOLOGY_TERM_NOT_FOUND.category == ErrorCategory.ONTOLOGY
        assert ErrorCode.EMPTY_CELL.category == ErrorCategory.CONTENT
        assert ErrorCode.DUPLICATE_VALUE.category == ErrorCategory.DUPLICATE

    def test_format_error_message_basic(self):
        """Test basic message formatting."""
        msg = format_error_message(ErrorCode.MISSING_REQUIRED_COLUMN, column="organism")
        assert "organism" in msg
        assert "missing" in msg.lower()

    def test_format_error_message_missing_vars(self):
        """Test formatting with missing variables returns template."""
        msg = format_error_message(ErrorCode.MISSING_REQUIRED_COLUMN)
        # Should return template with unfilled placeholder
        assert "{column}" in msg or "column" in msg.lower()


class TestLogicErrorWithCode:
    """Test LogicError with error code support."""

    def test_from_code_factory(self):
        """Test creating LogicError from error code."""
        error = LogicError.from_code(
            ErrorCode.MISSING_REQUIRED_COLUMN,
            column="organism",
            error_type=logging.ERROR,
        )
        assert error.error_code == ErrorCode.MISSING_REQUIRED_COLUMN
        assert error.column == "organism"
        assert "organism" in error.message
        assert error.error_type == logging.ERROR

    def test_from_code_with_row(self):
        """Test creating error with row information."""
        error = LogicError.from_code(
            ErrorCode.EMPTY_CELL,
            row=5,
            column="source name",
            error_type=logging.ERROR,
        )
        assert error.row == 5
        assert error.column == "source name"
        assert error.error_code == ErrorCode.EMPTY_CELL

    def test_from_code_with_extra_context(self):
        """Test creating error with additional context."""
        error = LogicError.from_code(
            ErrorCode.ONTOLOGY_TERM_NOT_FOUND,
            column="organism",
            value="invalid_term",
            ontologies=["NCBITaxon", "EFO"],
            error_type=logging.ERROR,
        )
        assert error.error_code == ErrorCode.ONTOLOGY_TERM_NOT_FOUND
        assert "ontologies" in error.context

    def test_to_dict(self):
        """Test JSON serialization of error."""
        error = LogicError.from_code(
            ErrorCode.MISSING_REQUIRED_COLUMN,
            column="organism",
            error_type=logging.ERROR,
        )
        d = error.to_dict()
        assert d["error_code"] == "MISSING_REQUIRED_COLUMN"
        assert d["category"] == "structure"
        assert d["severity"] == "error"
        assert d["column"] == "organism"

    def test_to_json(self):
        """Test JSON string serialization."""
        error = LogicError.from_code(
            ErrorCode.TRAILING_WHITESPACE,
            row=3,
            column="source name",
            value="value ",
            error_type=logging.WARNING,
        )
        json_str = error.to_json()
        data = json.loads(json_str)
        assert data["error_code"] == "TRAILING_WHITESPACE"
        assert data["row"] == 3
        assert data["severity"] == "warning"

    def test_severity_property(self):
        """Test severity property returns correct string."""
        error_err = LogicError.from_code(ErrorCode.EMPTY_CELL, error_type=logging.ERROR)
        error_warn = LogicError.from_code(ErrorCode.TRAILING_WHITESPACE, error_type=logging.WARNING)
        error_info = LogicError.from_code(ErrorCode.UNKNOWN)

        assert error_err.severity == "error"
        assert error_warn.severity == "warning"
        assert error_info.severity == "info"

    def test_backwards_compatible(self):
        """Test that old-style LogicError still works."""
        error = LogicError(
            message="Old style error",
            column="test",
            error_type=logging.ERROR,
        )
        assert error.message == "Old style error"
        assert error.error_code is None
        assert error.to_dict()["message"] == "Old style error"

    def test_str_includes_error_code(self):
        """Test that __str__ includes error code when present."""
        error = LogicError.from_code(
            ErrorCode.MISSING_REQUIRED_COLUMN,
            column="organism",
            error_type=logging.ERROR,
        )
        s = str(error)
        assert "MISSING_REQUIRED_COLUMN" in s


class TestValidationManifest:
    """Test ValidationManifest class."""

    @pytest.fixture
    def sample_errors(self):
        """Create sample errors for testing."""
        return [
            LogicError.from_code(
                ErrorCode.MISSING_REQUIRED_COLUMN,
                column="organism",
                error_type=logging.ERROR,
            ),
            LogicError.from_code(
                ErrorCode.TRAILING_WHITESPACE,
                row=1,
                column="source name",
                value="value ",
                error_type=logging.WARNING,
            ),
            LogicError.from_code(
                ErrorCode.TRAILING_WHITESPACE,
                row=2,
                column="source name",
                value="other ",
                error_type=logging.WARNING,
            ),
            LogicError.from_code(
                ErrorCode.EMPTY_CELL,
                row=3,
                column="organism",
                error_type=logging.ERROR,
            ),
        ]

    def test_from_errors(self, sample_errors):
        """Test creating manifest from error list."""
        manifest = ValidationManifest.from_errors(sample_errors)
        assert len(manifest) == 4

    def test_filter_by_code(self, sample_errors):
        """Test filtering errors by code."""
        manifest = ValidationManifest.from_errors(sample_errors)
        whitespace_errors = manifest.filter_by_code(ErrorCode.TRAILING_WHITESPACE)
        assert len(whitespace_errors) == 2

    def test_filter_by_multiple_codes(self, sample_errors):
        """Test filtering by multiple codes."""
        manifest = ValidationManifest.from_errors(sample_errors)
        errors = manifest.filter_by_code(ErrorCode.TRAILING_WHITESPACE, ErrorCode.EMPTY_CELL)
        assert len(errors) == 3

    def test_filter_by_row(self, sample_errors):
        """Test filtering errors by row."""
        manifest = ValidationManifest.from_errors(sample_errors)
        row_errors = manifest.filter_by_row(1)
        assert len(row_errors) == 1
        assert row_errors[0].row == 1

    def test_filter_by_column(self, sample_errors):
        """Test filtering errors by column."""
        manifest = ValidationManifest.from_errors(sample_errors)
        organism_errors = manifest.filter_by_column("organism")
        assert len(organism_errors) == 2

    def test_filter_by_cell(self, sample_errors):
        """Test filtering by specific cell."""
        manifest = ValidationManifest.from_errors(sample_errors)
        cell_errors = manifest.filter_by_cell(1, "source name")
        assert len(cell_errors) == 1

    def test_has_code(self, sample_errors):
        """Test checking if code exists."""
        manifest = ValidationManifest.from_errors(sample_errors)
        assert manifest.has_code(ErrorCode.MISSING_REQUIRED_COLUMN)
        assert manifest.has_code(ErrorCode.TRAILING_WHITESPACE)
        assert not manifest.has_code(ErrorCode.DUPLICATE_VALUE)

    def test_count_by_code(self, sample_errors):
        """Test counting errors by code."""
        manifest = ValidationManifest.from_errors(sample_errors)
        assert manifest.count_by_code(ErrorCode.TRAILING_WHITESPACE) == 2
        assert manifest.count_by_code(ErrorCode.MISSING_REQUIRED_COLUMN) == 1
        assert manifest.count_by_code(ErrorCode.DUPLICATE_VALUE) == 0

    def test_count_by_severity(self, sample_errors):
        """Test counting by severity."""
        manifest = ValidationManifest.from_errors(sample_errors)
        counts = manifest.count_by_severity()
        assert counts["error"] == 2
        assert counts["warning"] == 2

    def test_get_cell_errors(self, sample_errors):
        """Test grouping errors by cell location."""
        manifest = ValidationManifest.from_errors(sample_errors)
        cell_errors = manifest.get_cell_errors()
        assert (1, "source name") in cell_errors
        assert len(cell_errors[(1, "source name")]) == 1

    def test_error_count_property(self, sample_errors):
        """Test error_count property."""
        manifest = ValidationManifest.from_errors(sample_errors)
        assert manifest.error_count == 2
        assert manifest.warning_count == 2

    def test_is_valid(self, sample_errors):
        """Test is_valid property."""
        manifest = ValidationManifest.from_errors(sample_errors)
        assert not manifest.is_valid  # Has errors

        warnings_only = [e for e in sample_errors if e.severity == "warning"]
        manifest_warnings = ValidationManifest.from_errors(warnings_only)
        assert manifest_warnings.is_valid  # Only warnings

    def test_to_dict(self, sample_errors):
        """Test dict serialization."""
        manifest = ValidationManifest.from_errors(sample_errors)
        d = manifest.to_dict()
        assert "summary" in d
        assert "errors" in d
        assert d["summary"]["total"] == 4
        assert d["summary"]["errors"] == 2
        assert d["summary"]["warnings"] == 2

    def test_to_json(self, sample_errors):
        """Test JSON serialization."""
        manifest = ValidationManifest.from_errors(sample_errors)
        json_str = manifest.to_json()
        data = json.loads(json_str)
        assert data["summary"]["total"] == 4

    def test_empty_manifest(self):
        """Test empty manifest behavior."""
        manifest = ValidationManifest()
        assert len(manifest) == 0
        assert manifest.is_valid
        assert manifest.error_count == 0
        assert not manifest  # falsy when empty

    def test_iteration(self, sample_errors):
        """Test iterating over manifest."""
        manifest = ValidationManifest.from_errors(sample_errors)
        errors_list = list(manifest)
        assert len(errors_list) == 4

    def test_unique_error_codes(self, sample_errors):
        """Test getting unique error codes."""
        manifest = ValidationManifest.from_errors(sample_errors)
        codes = manifest.unique_error_codes
        assert ErrorCode.MISSING_REQUIRED_COLUMN in codes
        assert ErrorCode.TRAILING_WHITESPACE in codes
        assert ErrorCode.EMPTY_CELL in codes
        assert len(codes) == 3
