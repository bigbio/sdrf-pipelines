from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sdrf_pipelines.utils.error_codes import ErrorCode


class ValidationWarning:
    """
    Represents a difference between the schema and data frame, found during the validation of the data frame
    """

    def __init__(self, message: str, value: str | None = None, row: int = -1, column: str | None = None):
        """
        Validation Warning in the SDRF, The warning contains the following information.

        Parameters:
            message: Message of the error.
            value: the value that is failing the validation
            row: The index of the row in the Pandas dataframe
            column: The column name of the dataframe.
        """
        self.message = message
        self.value = value
        self.row = row
        self.column = column

    def __str__(self) -> str:
        """
        The entire warning message as a string
        """
        if self.row is not None and self.column is not None and self.value is not None:
            return f'{{row: {self.row}, column: "{self.column}"}}: "{self.value}" {self.message}'
        return self.message


class AppException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


__all__ = ["LogicError"]


class LogicError(ValidationWarning):
    def __init__(
        self,
        message: str,
        value: str | None = None,
        row: int = -1,
        column: str | None = None,
        error_type=None,
        suggestion: str | None = None,
        error_code: ErrorCode | None = None,
        context: dict[str, Any] | None = None,
    ):
        """
        Initialize an instance of LogicError with detailed error information.

        Parameters:
            message: Message describing the logic error.
            value: The value associated with the exception.
            row: The row index where the error occurred (0-based, -1 if not applicable).
            column: The column name where the error occurred.
            error_type: Logging level (e.g., logging.ERROR, logging.WARNING).
            suggestion: Optional suggestion for how to fix the error.
            error_code: Machine-readable error code from ErrorCode enum.
            context: Additional context variables for the error (used with error_code).
        """
        super().__init__(message, value, row, column)
        self._error_type = error_type
        self.suggestion = suggestion
        self.error_code = error_code
        self.context = context or {}

    @classmethod
    def from_code(
        cls,
        error_code: ErrorCode,
        row: int = -1,
        column: str | None = None,
        value: str | None = None,
        error_type=None,
        suggestion: str | None = None,
        **context,
    ) -> LogicError:
        """
        Create a LogicError from an error code with automatic message generation.

        Args:
            error_code: The ErrorCode enum value
            row: Row index (0-based, -1 if not applicable)
            column: Column name where error occurred
            value: The problematic value
            error_type: Logging level (ERROR, WARNING, etc.)
            suggestion: Optional fix suggestion
            **context: Additional variables for the message template

        Returns:
            A new LogicError instance with formatted message
        """
        from sdrf_pipelines.utils.error_codes import format_error_message

        # Build context from all available info
        full_context = {"column": column, "value": value, "row": row, **context}
        message = format_error_message(error_code, **full_context)

        return cls(
            message=message,
            value=value,
            row=row,
            column=column,
            error_type=error_type,
            suggestion=suggestion,
            error_code=error_code,
            context=full_context,
        )

    @property
    def error_type(self):
        return self._error_type

    @property
    def severity(self) -> str:
        """Return severity as a string (error, warning, info)."""
        if self._error_type == logging.ERROR:
            return "error"
        elif self._error_type == logging.WARNING:
            return "warning"
        return "info"

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the error to a dictionary for JSON serialization.

        Returns:
            Dictionary with all error information
        """
        result: dict[str, Any] = {
            "message": self.message,
            "severity": self.severity,
        }

        # Add optional fields only if they have meaningful values
        if self.error_code is not None:
            result["error_code"] = self.error_code.value
            result["category"] = self.error_code.category.value

        if self.row >= 0:
            result["row"] = self.row

        if self.column is not None:
            result["column"] = self.column

        if self.value is not None:
            result["value"] = self.value

        if self.suggestion:
            result["suggestion"] = self.suggestion

        if self.context:
            # Include context but filter out None values and duplicates
            filtered_context = {
                k: v
                for k, v in self.context.items()
                if v is not None and k not in ("column", "value", "row")
            }
            if filtered_context:
                result["context"] = filtered_context

        return result

    def to_json(self) -> str:
        """Serialize the error to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    def __str__(self) -> str:
        level_name = logging.getLevelName(self._error_type) if self._error_type else "INFO"
        parts = []

        # Location info
        if self.row is not None and self.row >= 0 and self.column is not None:
            parts.append(f"[Row {self.row + 1}, Column '{self.column}']")
        elif self.column is not None:
            parts.append(f"[Column '{self.column}']")
        elif self.row is not None and self.row >= 0:
            parts.append(f"[Row {self.row + 1}]")

        # Value if present
        if self.value is not None:
            parts.append(f'Value: "{self.value}"')

        # Main message
        parts.append(self.message)

        # Error code if present
        if self.error_code is not None:
            parts.append(f"({self.error_code.value})")

        # Level
        parts.append(f"[{level_name}]")

        result = " ".join(parts)

        # Add suggestion on new line if present
        if self.suggestion:
            result += f"\n  → Suggestion: {self.suggestion}"

        return result


class AppConfigException(AppException):
    pass


class ConfigManagerException(Exception):
    def __init__(self, value):
        Exception.__init__(self)
        self.value = value

    def __str__(self):
        return repr(self.value)
