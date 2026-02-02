"""Validation manifest for machine-readable error reporting.

This module provides the ValidationManifest class which wraps a collection
of validation errors and provides filtering, aggregation, and serialization
capabilities for use in tests, web apps, and editor integrations.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sdrf_pipelines.utils.error_codes import ErrorCode
    from sdrf_pipelines.utils.exceptions import LogicError


@dataclass
class ValidationManifest:
    """A collection of validation errors with filtering and serialization support.

    This class wraps validation errors from SDRF validation and provides:
    - Filtering by error code, row, column, or cell location
    - Counting and aggregation methods for tests
    - JSON serialization for web apps and editors
    - Cell-based error mapping for table highlighting

    Example usage in tests:
        manifest = ValidationManifest.from_errors(errors)
        assert manifest.has_code(ErrorCode.MISSING_REQUIRED_COLUMN)
        assert manifest.count_by_code(ErrorCode.TRAILING_WHITESPACE) == 3

    Example usage in web apps:
        cell_errors = manifest.get_cell_errors()
        for (row, col), errors in cell_errors.items():
            highlight_cell(row, col, errors)
    """

    errors: list[LogicError] = field(default_factory=list)

    @classmethod
    def from_errors(cls, errors: Iterable[LogicError]) -> ValidationManifest:
        """Create a ValidationManifest from an iterable of LogicError objects."""
        return cls(errors=list(errors))

    def __len__(self) -> int:
        """Return the total number of errors."""
        return len(self.errors)

    def __iter__(self):
        """Iterate over errors."""
        return iter(self.errors)

    def __bool__(self) -> bool:
        """Return True if there are any errors."""
        return len(self.errors) > 0

    # Filtering methods

    def filter_by_code(self, *codes: ErrorCode) -> list[LogicError]:
        """Return errors matching any of the given error codes.

        Args:
            *codes: One or more ErrorCode values to filter by

        Returns:
            List of LogicError objects with matching error codes
        """
        code_set = set(codes)
        return [e for e in self.errors if e.error_code in code_set]

    def filter_by_row(self, row: int) -> list[LogicError]:
        """Return errors at the specified row index (0-based).

        Args:
            row: The row index to filter by

        Returns:
            List of LogicError objects at the given row
        """
        return [e for e in self.errors if e.row == row]

    def filter_by_column(self, column: str) -> list[LogicError]:
        """Return errors in the specified column.

        Args:
            column: The column name to filter by

        Returns:
            List of LogicError objects in the given column
        """
        return [e for e in self.errors if e.column == column]

    def filter_by_cell(self, row: int, column: str) -> list[LogicError]:
        """Return errors at a specific cell location.

        Args:
            row: The row index (0-based)
            column: The column name

        Returns:
            List of LogicError objects at the specific cell
        """
        return [e for e in self.errors if e.row == row and e.column == column]

    def filter_by_severity(self, severity: str) -> list[LogicError]:
        """Return errors with the specified severity.

        Args:
            severity: "error", "warning", or "info"

        Returns:
            List of LogicError objects with matching severity
        """
        return [e for e in self.errors if e.severity == severity]

    # Checking methods

    def has_code(self, code: ErrorCode) -> bool:
        """Check if any error has the given error code.

        Args:
            code: The ErrorCode to check for

        Returns:
            True if at least one error has this code
        """
        return any(e.error_code == code for e in self.errors)

    def has_error_at(self, row: int, column: str) -> bool:
        """Check if there's an error at the specified cell.

        Args:
            row: The row index (0-based)
            column: The column name

        Returns:
            True if there's at least one error at this cell
        """
        return any(e.row == row and e.column == column for e in self.errors)

    # Counting methods

    def count_by_code(self, code: ErrorCode) -> int:
        """Count errors with the given error code.

        Args:
            code: The ErrorCode to count

        Returns:
            Number of errors with this code
        """
        return sum(1 for e in self.errors if e.error_code == code)

    def count_by_severity(self) -> Counter[str]:
        """Count errors grouped by severity.

        Returns:
            Counter with keys "error", "warning", "info"
        """
        return Counter(e.severity for e in self.errors)

    def count_by_category(self) -> Counter[str]:
        """Count errors grouped by category.

        Returns:
            Counter with category names as keys
        """
        return Counter(e.error_code.category.value for e in self.errors if e.error_code)

    def code_counts(self) -> Counter[str]:
        """Count errors grouped by error code.

        Returns:
            Counter with error code values as keys
        """
        return Counter(e.error_code.value for e in self.errors if e.error_code)

    # Aggregation for web apps / editors

    def get_cell_errors(self) -> dict[tuple[int, str], list[LogicError]]:
        """Group errors by cell location for table highlighting.

        Returns:
            Dictionary mapping (row, column) tuples to lists of errors.
            Only includes errors with valid row (>= 0) and column values.
        """
        result: dict[tuple[int, str], list[LogicError]] = {}
        for error in self.errors:
            if error.row >= 0 and error.column is not None:
                key = (error.row, error.column)
                if key not in result:
                    result[key] = []
                result[key].append(error)
        return result

    def get_row_errors(self) -> dict[int, list[LogicError]]:
        """Group errors by row for row-level highlighting.

        Returns:
            Dictionary mapping row indices to lists of errors.
            Only includes errors with valid row (>= 0).
        """
        result: dict[int, list[LogicError]] = {}
        for error in self.errors:
            if error.row >= 0:
                if error.row not in result:
                    result[error.row] = []
                result[error.row].append(error)
        return result

    def get_column_errors(self) -> dict[str, list[LogicError]]:
        """Group errors by column for column-level highlighting.

        Returns:
            Dictionary mapping column names to lists of errors.
            Only includes errors with column values set.
        """
        result: dict[str, list[LogicError]] = {}
        for error in self.errors:
            if error.column is not None:
                if error.column not in result:
                    result[error.column] = []
                result[error.column].append(error)
        return result

    # Convenience properties

    @property
    def error_count(self) -> int:
        """Number of errors with severity 'error'."""
        return sum(1 for e in self.errors if e.severity == "error")

    @property
    def warning_count(self) -> int:
        """Number of errors with severity 'warning'."""
        return sum(1 for e in self.errors if e.severity == "warning")

    @property
    def is_valid(self) -> bool:
        """True if there are no errors (warnings are allowed)."""
        return self.error_count == 0

    @property
    def unique_error_codes(self) -> set[ErrorCode]:
        """Set of unique error codes present in the manifest."""
        return {e.error_code for e in self.errors if e.error_code is not None}

    # Serialization

    def to_dict(self) -> dict[str, Any]:
        """Convert the manifest to a dictionary for JSON serialization.

        Returns:
            Dictionary with summary and error list
        """
        return {
            "summary": {
                "total": len(self.errors),
                "errors": self.error_count,
                "warnings": self.warning_count,
                "is_valid": self.is_valid,
                "by_category": dict(self.count_by_category()),
                "by_code": dict(self.code_counts()),
            },
            "errors": [e.to_dict() for e in self.errors],
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialize the manifest to a JSON string.

        Args:
            indent: JSON indentation level (default 2)

        Returns:
            JSON string representation
        """
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_json(cls, json_str: str) -> ValidationManifest:
        """Deserialize a manifest from JSON string.

        Note: This creates LogicError objects without full error_code enum
        restoration. For full round-trip serialization, use pickle instead.

        Args:
            json_str: JSON string from to_json()

        Returns:
            ValidationManifest with reconstructed errors
        """
        from sdrf_pipelines.utils.exceptions import LogicError

        data = json.loads(json_str)
        errors = []
        for err_dict in data.get("errors", []):
            error = LogicError(
                message=err_dict.get("message", ""),
                value=err_dict.get("value"),
                row=err_dict.get("row", -1),
                column=err_dict.get("column"),
            )
            errors.append(error)
        return cls(errors=errors)

    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"ValidationManifest(total={len(self.errors)}, errors={self.error_count}, warnings={self.warning_count})"
