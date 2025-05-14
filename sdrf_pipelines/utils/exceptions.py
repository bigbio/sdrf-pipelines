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
            return '{{row: {}, column: "{}"}}: "{}" {}'.format(self.row, self.column, self.value, self.message)
        return self.message


class AppException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


__all__ = ["LogicError"]


class LogicError(ValidationWarning):
    def __init__(
        self, message: str, value: str | None = None, row: int = -1, column: str | None = None, error_type=None
    ):
        """
        Initialize an instance of AppException with a specified value.

        Parameters:
            message"
            value: The value associated with the exception.
        """
        super().__init__(message, value, row, column)
        self._error_type = error_type

    @property
    def error_type(self):
        return self._error_type

    def __str__(self) -> str:
        if self.row is not None and self.column is not None and self.value is not None:
            return (
                f'{{row: {self.row}, column: "{self.column}"}}: '
                f'"{self.value}" {self.message} -- {logging.getLevelName(self._error_type)}'
            )
        return f"{self.message} -- {logging.getLevelName(self._error_type)}"


class AppConfigException(AppException):
    pass


class ConfigManagerException(Exception):
    def __init__(self, value):
        Exception.__init__(self)
        self.value = value

    def __str__(self):
        return repr(self.value)
