"""Machine-readable error codes for SDRF validation.

This module provides:
- ErrorCategory: High-level categorization of errors
- ErrorCode: Specific error codes for each validation failure type
- ERROR_MESSAGE_TEMPLATES: Templates for generating human-readable messages
"""

from enum import Enum


class ErrorCategory(str, Enum):
    """High-level categorization of errors for grouping and filtering."""

    STRUCTURE = "structure"  # Column-level issues (missing, order)
    FORMAT = "format"  # Value format issues (pattern, whitespace)
    ONTOLOGY = "ontology"  # Ontology term validation
    CONTENT = "content"  # Content validation (empty cells, invalid values)
    DUPLICATE = "duplicate"  # Uniqueness violations
    CONSTRAINT = "constraint"  # Business logic constraints


class ErrorCode(str, Enum):
    """Machine-readable error codes for SDRF validation errors.

    Naming convention: CATEGORY_SPECIFIC_ISSUE
    Each code maps to a human-readable message template.
    """

    # Structure errors
    MISSING_REQUIRED_COLUMN = "MISSING_REQUIRED_COLUMN"
    COLUMN_ORDER_INVALID = "COLUMN_ORDER_INVALID"
    INSUFFICIENT_COLUMNS = "INSUFFICIENT_COLUMNS"
    CHARACTERISTICS_AFTER_ASSAY = "CHARACTERISTICS_AFTER_ASSAY"
    COMMENT_BEFORE_ASSAY = "COMMENT_BEFORE_ASSAY"
    TECHNOLOGY_TYPE_MISPLACED = "TECHNOLOGY_TYPE_MISPLACED"
    FACTOR_COLUMN_NOT_LAST = "FACTOR_COLUMN_NOT_LAST"

    # Format errors
    TRAILING_WHITESPACE = "TRAILING_WHITESPACE"
    TRAILING_WHITESPACE_COLUMN_NAME = "TRAILING_WHITESPACE_COLUMN_NAME"
    INVALID_FORMAT = "INVALID_FORMAT"
    PATTERN_MISMATCH = "PATTERN_MISMATCH"

    # Ontology errors
    ONTOLOGY_TERM_NOT_FOUND = "ONTOLOGY_TERM_NOT_FOUND"
    INVALID_ONTOLOGY_TERM_FORMAT = "INVALID_ONTOLOGY_TERM_FORMAT"

    # Content errors
    EMPTY_CELL = "EMPTY_CELL"
    INVALID_VALUE = "INVALID_VALUE"
    NOT_APPLICABLE_NOT_ALLOWED = "NOT_APPLICABLE_NOT_ALLOWED"
    NOT_AVAILABLE_NOT_ALLOWED = "NOT_AVAILABLE_NOT_ALLOWED"

    # Duplicate errors
    DUPLICATE_VALUE = "DUPLICATE_VALUE"
    DUPLICATE_COMBINATION = "DUPLICATE_COMBINATION"

    # Constraint errors
    SINGLE_CARDINALITY_VIOLATED = "SINGLE_CARDINALITY_VIOLATED"
    COLUMNS_NOT_FOUND = "COLUMNS_NOT_FOUND"

    # Generic/Unknown (for backwards compatibility)
    UNKNOWN = "UNKNOWN"
    VALIDATION_ERROR = "VALIDATION_ERROR"

    @property
    def category(self) -> ErrorCategory:
        """Return the category for this error code."""
        return _ERROR_CATEGORY_MAP.get(self, ErrorCategory.CONTENT)


# Map error codes to categories
_ERROR_CATEGORY_MAP = {
    # Structure
    ErrorCode.MISSING_REQUIRED_COLUMN: ErrorCategory.STRUCTURE,
    ErrorCode.COLUMN_ORDER_INVALID: ErrorCategory.STRUCTURE,
    ErrorCode.INSUFFICIENT_COLUMNS: ErrorCategory.STRUCTURE,
    ErrorCode.CHARACTERISTICS_AFTER_ASSAY: ErrorCategory.STRUCTURE,
    ErrorCode.COMMENT_BEFORE_ASSAY: ErrorCategory.STRUCTURE,
    ErrorCode.TECHNOLOGY_TYPE_MISPLACED: ErrorCategory.STRUCTURE,
    ErrorCode.FACTOR_COLUMN_NOT_LAST: ErrorCategory.STRUCTURE,
    # Format
    ErrorCode.TRAILING_WHITESPACE: ErrorCategory.FORMAT,
    ErrorCode.TRAILING_WHITESPACE_COLUMN_NAME: ErrorCategory.FORMAT,
    ErrorCode.INVALID_FORMAT: ErrorCategory.FORMAT,
    ErrorCode.PATTERN_MISMATCH: ErrorCategory.FORMAT,
    # Ontology
    ErrorCode.ONTOLOGY_TERM_NOT_FOUND: ErrorCategory.ONTOLOGY,
    ErrorCode.INVALID_ONTOLOGY_TERM_FORMAT: ErrorCategory.ONTOLOGY,
    # Content
    ErrorCode.EMPTY_CELL: ErrorCategory.CONTENT,
    ErrorCode.INVALID_VALUE: ErrorCategory.CONTENT,
    ErrorCode.NOT_APPLICABLE_NOT_ALLOWED: ErrorCategory.CONTENT,
    ErrorCode.NOT_AVAILABLE_NOT_ALLOWED: ErrorCategory.CONTENT,
    # Duplicate
    ErrorCode.DUPLICATE_VALUE: ErrorCategory.DUPLICATE,
    ErrorCode.DUPLICATE_COMBINATION: ErrorCategory.DUPLICATE,
    # Constraint
    ErrorCode.SINGLE_CARDINALITY_VIOLATED: ErrorCategory.CONSTRAINT,
    ErrorCode.COLUMNS_NOT_FOUND: ErrorCategory.CONSTRAINT,
    # Unknown
    ErrorCode.UNKNOWN: ErrorCategory.CONTENT,
    ErrorCode.VALIDATION_ERROR: ErrorCategory.CONTENT,
}


# Message templates for generating human-readable messages from error codes
# Use {field} placeholders for context variables
ERROR_MESSAGE_TEMPLATES: dict[ErrorCode, str] = {
    # Structure
    ErrorCode.MISSING_REQUIRED_COLUMN: "Required column '{column}' is missing from the SDRF file",
    ErrorCode.COLUMN_ORDER_INVALID: "Column '{column}' is in the wrong position",
    ErrorCode.INSUFFICIENT_COLUMNS: "SDRF has {actual} columns but requires at least {required}",
    ErrorCode.CHARACTERISTICS_AFTER_ASSAY: (
        "All characteristics columns must be before 'assay name'. Found after: {columns}"
    ),
    ErrorCode.COMMENT_BEFORE_ASSAY: "The column '{column}' cannot be before the assay name",
    ErrorCode.TECHNOLOGY_TYPE_MISPLACED: "The column '{column}' must be immediately after the assay name",
    ErrorCode.FACTOR_COLUMN_NOT_LAST: "The following factor column should be last: {columns}",
    # Format
    ErrorCode.TRAILING_WHITESPACE: "Trailing whitespace detected",
    ErrorCode.TRAILING_WHITESPACE_COLUMN_NAME: "Trailing whitespace detected in column name",
    ErrorCode.INVALID_FORMAT: "Invalid format for value '{value}'",
    ErrorCode.PATTERN_MISMATCH: "Value '{value}' does not match required pattern",
    # Ontology
    ErrorCode.ONTOLOGY_TERM_NOT_FOUND: (
        "Term: {value} in column '{column}', is not found in the given ontology list {ontologies}"
    ),
    ErrorCode.INVALID_ONTOLOGY_TERM_FORMAT: "Term: {value} in column '{column}', is not a valid ontology term",
    # Content
    ErrorCode.EMPTY_CELL: "Empty value found Row: {row}, Column: {column}",
    ErrorCode.INVALID_VALUE: "Invalid value '{value}' - must be one of the allowed values",
    ErrorCode.NOT_APPLICABLE_NOT_ALLOWED: (
        "Column '{column}' contains 'not applicable' values but requires actual values"
    ),
    ErrorCode.NOT_AVAILABLE_NOT_ALLOWED: (
        "Column '{column}' contains 'not available' values but requires actual values"
    ),
    # Duplicate
    ErrorCode.DUPLICATE_VALUE: "Duplicate value '{value}' found at rows: {rows}",
    ErrorCode.DUPLICATE_COMBINATION: "Combination '{value}' in columns '{columns}' is duplicated at rows: {rows}",
    # Constraint
    ErrorCode.SINGLE_CARDINALITY_VIOLATED: "Column '{column}' has multiple unique values: {values}",
    ErrorCode.COLUMNS_NOT_FOUND: "Columns not found in DataFrame: {columns}",
    # Generic
    ErrorCode.UNKNOWN: "{message}",
    ErrorCode.VALIDATION_ERROR: "Validation error: {message}",
}


def format_error_message(error_code: ErrorCode, **context) -> str:
    """Format an error message from a code and context variables.

    Args:
        error_code: The ErrorCode enum value
        **context: Variables to substitute into the message template

    Returns:
        Formatted error message string
    """
    template = ERROR_MESSAGE_TEMPLATES.get(error_code, "{message}")
    try:
        # Filter out None values for cleaner formatting
        filtered_context = {k: v for k, v in context.items() if v is not None}
        return template.format(**filtered_context)
    except KeyError:
        # Return template as-is if formatting fails
        return template
