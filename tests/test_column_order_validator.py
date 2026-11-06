"""Tests for ColumnOrderValidator: a column's type is what its name starts with, not a word inside it.

``comment[characteristics source]`` is a comment column, and comment columns belong after ``assay name``. Looking for
the word anywhere in the name reported it as a misplaced characteristics column.
"""

import logging
from io import StringIO

import pandas as pd
import pytest

from sdrf_pipelines.sdrf.sdrf import read_sdrf
from sdrf_pipelines.sdrf.validators import ColumnOrderValidator, get_validator
from sdrf_pipelines.utils.error_codes import ErrorCode
from sdrf_pipelines.utils.exceptions import LogicError

SAMPLE = ["source name", "characteristics[organism]"]
ASSAY = ["assay name", "technology type"]
DATA = ["comment[data file]"]

SDRF_ROW = {
    "source name": "sample 1",
    "characteristics[organism]": "homo sapiens",
    "characteristics[organism part]": "liver",
    "characteristics[disease]": "normal",
    "characteristics[biological replicate]": "1",
    "assay name": "run 1",
    "technology type": "proteomic profiling by mass spectrometry",
    "comment[proteomics data acquisition method]": "NT=Data-dependent acquisition;AC=PRIDE:0000627",
    "comment[label]": "NT=label free sample;AC=MS:1002038",
    "comment[instrument]": "NT=Q Exactive;AC=MS:1001911",
    "comment[cleavage agent details]": "NT=Trypsin;AC=MS:1001251",
    "comment[technical replicate]": "1",
    "comment[fraction identifier]": "1",
    "comment[data file]": "run1.raw",
}


def _validate(columns: list[str]) -> list[LogicError]:
    return ColumnOrderValidator().validate(pd.DataFrame(columns=columns))


def _validate_template(extra_column: str) -> list[LogicError]:
    row = {**SDRF_ROW, extra_column: "value"}
    text = "\t".join(row) + "\n" + "\t".join(row.values()) + "\n"
    return read_sdrf(StringIO(text)).validate_sdrf(template="ms-proteomics", skip_ontology=True)


def test_registered_by_name():
    assert get_validator("column_order") is ColumnOrderValidator


def test_well_ordered_columns_pass():
    assert _validate(SAMPLE + ASSAY + DATA + ["factor value[disease]"]) == []


def test_without_assay_name_the_order_is_not_checked():
    assert _validate(["comment[data file]", "source name", "characteristics[organism]"]) == []


@pytest.mark.parametrize(
    "columns",
    [
        pytest.param(SAMPLE + ASSAY + DATA + ["comment[characteristics source]"], id="comment-naming-characteristics"),
        pytest.param(SAMPLE + ASSAY + DATA + ["comment[material type source]"], id="comment-naming-material-type"),
        pytest.param(SAMPLE + ASSAY + DATA + ["comment[technology type source]"], id="comment-naming-technology-type"),
        pytest.param(
            SAMPLE + ASSAY + DATA + ["factor value[disease]", "factor value[characteristics score]"],
            id="factor-naming-characteristics",
        ),
        pytest.param(SAMPLE + ASSAY + DATA + ["factor value[material type]"], id="factor-naming-material-type"),
        pytest.param(
            SAMPLE + ASSAY + DATA + ["factor value[disease]", "factor value[comment]"], id="factor-naming-comment"
        ),
        pytest.param(
            SAMPLE + ASSAY + DATA + ["comment[factor value disease source]", "factor value[disease]"],
            id="comment-naming-factor-value",
        ),
        pytest.param(SAMPLE + ["characteristics[comment]"] + ASSAY + DATA, id="characteristics-naming-comment"),
        pytest.param(
            SAMPLE + ["characteristics[technology type]"] + ASSAY + DATA, id="characteristics-naming-technology-type"
        ),
    ],
)
def test_a_word_inside_another_column_type_does_not_misplace_the_column(columns):
    assert _validate(columns) == []


AFTER_ASSAY_CHARACTERISTICS = {
    (ErrorCode.CHARACTERISTICS_AFTER_ASSAY, logging.ERROR),
    (ErrorCode.COLUMN_ORDER_INVALID, logging.ERROR),
}


@pytest.mark.parametrize(
    "columns, expected",
    [
        pytest.param(
            SAMPLE + ASSAY + DATA + ["characteristics[disease]"],
            AFTER_ASSAY_CHARACTERISTICS,
            id="characteristics-after-assay",
        ),
        pytest.param(
            SAMPLE + ASSAY + DATA + ["characteristics[material type]"],
            AFTER_ASSAY_CHARACTERISTICS,
            id="characteristics-material-type-after-assay",
        ),
        pytest.param(
            SAMPLE + ASSAY + ["material type"] + DATA,
            {(ErrorCode.COLUMN_ORDER_INVALID, logging.ERROR)},
            id="material-type-after-assay",
        ),
        pytest.param(
            SAMPLE + ["comment[label]"] + ASSAY + DATA,
            {(ErrorCode.COMMENT_BEFORE_ASSAY, logging.ERROR)},
            id="comment-before-assay",
        ),
        pytest.param(
            SAMPLE + ["technology type", "assay name"] + DATA,
            {(ErrorCode.TECHNOLOGY_TYPE_MISPLACED, logging.WARNING)},
            id="technology-type-just-before-assay",
        ),
        pytest.param(
            ["technology type"] + SAMPLE + ["assay name"] + DATA,
            {(ErrorCode.TECHNOLOGY_TYPE_MISPLACED, logging.ERROR)},
            id="technology-type-far-before-assay",
        ),
        pytest.param(
            SAMPLE + ["assay name"] + DATA + ["technology type"],
            {(ErrorCode.TECHNOLOGY_TYPE_MISPLACED, logging.ERROR)},
            id="technology-type-far-after-assay",
        ),
        pytest.param(
            SAMPLE + ASSAY + ["factor value[disease]"] + DATA,
            {(ErrorCode.FACTOR_COLUMN_NOT_LAST, logging.ERROR)},
            id="factor-before-comment",
        ),
        pytest.param(
            SAMPLE + ASSAY + ["factor value[disease]", "characteristics[age]"],
            AFTER_ASSAY_CHARACTERISTICS | {(ErrorCode.FACTOR_COLUMN_NOT_LAST, logging.ERROR)},
            id="factor-before-characteristics",
        ),
        pytest.param(
            SAMPLE + ASSAY + ["factor value[comment]"] + DATA,
            {(ErrorCode.FACTOR_COLUMN_NOT_LAST, logging.ERROR)},
            id="factor-naming-comment-before-comment",
        ),
        pytest.param(
            SAMPLE + ASSAY + DATA + ["characteristics [disease]"],
            AFTER_ASSAY_CHARACTERISTICS,
            id="malformed-characteristics-after-assay",
        ),
        pytest.param(
            SAMPLE + ["comment [label]"] + ASSAY + DATA,
            {(ErrorCode.COMMENT_BEFORE_ASSAY, logging.ERROR)},
            id="malformed-comment-before-assay",
        ),
    ],
)
def test_misplaced_columns_are_reported(columns, expected):
    assert {(e.error_code, e.error_type) for e in _validate(columns)} == expected


def test_only_the_misplaced_column_is_named():
    errors = _validate(SAMPLE + ASSAY + DATA + ["comment[characteristics source]", "characteristics[disease]"])

    named = {e.error_code: e.context.get("columns") or e.column for e in errors}
    assert named == {
        ErrorCode.CHARACTERISTICS_AFTER_ASSAY: "characteristics[disease]",
        ErrorCode.COLUMN_ORDER_INVALID: "characteristics[disease]",
    }


@pytest.mark.parametrize(
    "column",
    ["comment[characteristics source]", "comment[material type source]", "comment[technology type source]"],
)
def test_template_validation_accepts_a_comment_naming_another_column_type(column):
    errors = _validate_template(column)

    assert [e for e in errors if e.error_type == logging.ERROR] == []


def test_template_validation_still_rejects_misplaced_characteristics():
    errors = _validate_template("characteristics[age]")

    assert ErrorCode.CHARACTERISTICS_AFTER_ASSAY in {e.error_code for e in errors}
