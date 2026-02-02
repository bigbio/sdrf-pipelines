"""Utility modules for SDRF validation.

This package provides:
- error_codes: Machine-readable error codes and message templates
- exceptions: LogicError and ValidationWarning classes
- manifest: ValidationManifest for error collection and filtering
"""

from sdrf_pipelines.utils.error_codes import ErrorCategory, ErrorCode, format_error_message
from sdrf_pipelines.utils.exceptions import LogicError, ValidationWarning
from sdrf_pipelines.utils.manifest import ValidationManifest

__all__ = [
    "ErrorCategory",
    "ErrorCode",
    "LogicError",
    "ValidationManifest",
    "ValidationWarning",
    "format_error_message",
]
