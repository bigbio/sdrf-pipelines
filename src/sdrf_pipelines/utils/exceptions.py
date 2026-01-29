import logging


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
        """
        super().__init__(message, value, row, column)
        self._error_type = error_type
        self.suggestion = suggestion

    @property
    def error_type(self):
        return self._error_type

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
