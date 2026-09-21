"""Regression tests for EmptyCellValidator on SDRFs with repeated column headers.

SDRF carries multi-valued annotations as repeated columns sharing one header, so a required
column can legitimately appear several times (comment[modification parameters] is the common
case). Addressing the validation grid by label rather than by position raised
``ValueError: The truth value of a Series is ambiguous`` and aborted validation entirely.
"""

import logging

import pandas as pd

from sdrf_pipelines.sdrf.validators import EmptyCellValidator
from sdrf_pipelines.utils.error_codes import ErrorCode


def _frame(mod_values: list[str]) -> pd.DataFrame:
    """One row, with comment[modification parameters] repeated once per value."""
    data = {"source name": ["sample 1"], "comment[data file]": ["run1.raw"]}
    frame = pd.DataFrame(data)
    for value in mod_values:
        frame = pd.concat([frame, pd.DataFrame({"comment[modification parameters]": [value]})], axis=1)
    return frame


def test_duplicate_required_column_does_not_crash():
    df = _frame(["NT=Oxidation", "NT=Acetyl", "NT=Carbamidomethyl"])
    validator = EmptyCellValidator({"required_columns": ["comment[modification parameters]"]})

    errors = validator.validate(df)

    assert errors == []


def test_duplicate_required_column_still_reports_the_empty_one():
    df = _frame(["NT=Oxidation", "   ", "NT=Carbamidomethyl"])
    validator = EmptyCellValidator({"required_columns": ["comment[modification parameters]"]})

    errors = validator.validate(df)

    assert len(errors) == 1
    assert errors[0].error_code == ErrorCode.EMPTY_CELL
    assert errors[0].error_type == logging.ERROR


def test_single_required_column_is_unaffected():
    df = pd.DataFrame({"source name": ["sample 1"], "characteristics[organism]": [""]})
    validator = EmptyCellValidator({"required_columns": ["characteristics[organism]"]})

    errors = validator.validate(df)

    assert len(errors) == 1
    assert errors[0].error_code == ErrorCode.EMPTY_CELL
