import logging

from pandas_schema import ValidationWarning


class AppException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


__all__ = ["LogicError"]


class LogicError(ValidationWarning):
    def __init__(self, message: str, value: str = None, row: int = -1, column: str = None, error_type: logging = None):
        super().__init__(message, value, row, column)
        self._error_type = error_type

    def __str__(self) -> str:
        if self.row is not None and self.column is not None and self.value is not None:
            return '{{row: {}, column: "{}"}}: "{}" {} -- {}'.format(
                self.row, self.column, self.value, self.message, logging.getLevelName(self._error_type)
            )
        else:
            return f"{self.message} -- {logging.getLevelName(self._error_type)}"


class AppConfigException(AppException):
    def __init__(self, value):
        super().__init__(value)


class ConfigManagerException(Exception):
    def __init__(self, value):
        Exception.__init__(self)
        self.value = value

    def __str__(self):
        return repr(self.value)
