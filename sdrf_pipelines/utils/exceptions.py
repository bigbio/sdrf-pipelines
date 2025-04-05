import logging


class AppException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


__all__ = ["LogicError"]


class LogicError():
    def __init__(self, message: str, error_type: logging = None):
        self._error_type = error_type
        self.message = message

    def __str__(self) -> str:
        return f"{self.message} -- {logging.getLevelName(self._error_type)}"

    def __eq__(self, other):
        if not isinstance(other, LogicError):
            return False
        return (self.message == other.message and
                self._error_type == other._error_type)

    def __hash__(self):
        return hash((self.message, self._error_type))


class AppConfigException(AppException):
    def __init__(self, value):
        super().__init__(value)


class ConfigManagerException(Exception):
    def __init__(self, value):
        Exception.__init__(self)
        self.value = value

    def __str__(self):
        return repr(self.value)