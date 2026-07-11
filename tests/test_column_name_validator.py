"""Tests for ColumnNameValidator: SDRF column names must have no whitespace around the brackets."""

import logging

import pandas as pd

from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame
from sdrf_pipelines.sdrf.validators import ColumnNameValidator, get_validator
from sdrf_pipelines.utils.error_codes import ErrorCode

GOOD = [
    "source name",
    "characteristics[organism]",
    "comment[modification parameters]",
    "comment[ms2 mass analyzer]",
    "assay name",
]
BAD = [
    "characteristics [organism]",  # space before '['
    "characteristics[ organism]",  # space after '['
    "comment[modification parameters ]",  # space before ']'
]


def test_registered_by_name():
    assert get_validator("column_name_validator") is ColumnNameValidator


def test_flags_only_bracket_whitespace():
    errors = ColumnNameValidator().validate(GOOD + BAD)
    assert {e.value for e in errors} == set(BAD)
    assert all(e.error_code == ErrorCode.MALFORMED_COLUMN_NAME for e in errors)
    assert all(e.error_type == logging.ERROR for e in errors)


def test_clean_names_pass():
    assert ColumnNameValidator().validate(GOOD) == []


def test_does_not_double_flag_trailing_whitespace():
    # Trailing whitespace *after* the closing bracket is TrailingWhitespaceValidator's job, not ours.
    assert ColumnNameValidator().validate(["characteristics[organism] "]) == []


def test_dataframe_and_sdrf_dataframe_paths():
    df = pd.DataFrame({c: ["x"] for c in ["source name", "characteristics[organism ]", "assay name"]})
    assert [e.value for e in ColumnNameValidator().validate(df)] == ["characteristics[organism ]"]
    assert [e.value for e in ColumnNameValidator().validate(SDRFDataFrame(df))] == ["characteristics[organism ]"]
