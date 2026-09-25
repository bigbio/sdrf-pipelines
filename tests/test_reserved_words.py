"""'pooled' and 'anonymized' are accepted only where the template allows them (allow_pooled/allow_anonymized)."""

import pandas as pd
import pytest

from sdrf_pipelines.sdrf.schemas.models import ColumnDefinition, MergeStrategy, RequirementLevel
from sdrf_pipelines.sdrf.schemas.utils import merge_column_defs
from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame
from sdrf_pipelines.sdrf.validators import PatternValidator, ValuesValidator

AGE_PATTERN = r"^\d+[Yy]$"


@pytest.mark.parametrize("word, flag", [("pooled", "allow_pooled"), ("anonymized", "allow_anonymized")])
def test_pattern_skips_reserved_word_only_when_allowed(word, flag):
    series = pd.Series(["45Y", word])
    rejected = PatternValidator(params={"pattern": AGE_PATTERN}).validate(series, column_name="characteristics[age]")
    assert len(rejected) == 1
    allowed = PatternValidator(params={"pattern": AGE_PATTERN, flag: True}).validate(
        series, column_name="characteristics[age]"
    )
    assert allowed == []


@pytest.mark.parametrize("word, flag", [("pooled", "allow_pooled"), ("anonymized", "allow_anonymized")])
def test_values_skips_reserved_word_only_when_allowed(word, flag):
    series = pd.Series(["male", word])
    params = {"values": ["male", "female"], "error_level": "error"}
    assert len(ValuesValidator(params=params).validate(series, column_name="characteristics[sex]")) == 1
    assert ValuesValidator(params={**params, flag: True}).validate(series, column_name="characteristics[sex]") == []


@pytest.mark.parametrize("strategy", [MergeStrategy.LAST, MergeStrategy.COMBINE])
def test_merge_keeps_pooled_and_anonymized_flags(strategy):
    base = ColumnDefinition(name="characteristics[age]", description="age", requirement=RequirementLevel.REQUIRED)
    human = base.model_copy(update={"allow_pooled": True, "allow_anonymized": True})
    merged = merge_column_defs([base, human], strategy=strategy)
    assert merged.allow_pooled is True
    assert merged.allow_anonymized is True


def _human_sdrf(age, sex):
    return SDRFDataFrame(
        pd.DataFrame(
            {
                "source name": ["s1", "s2"],
                "characteristics[organism]": ["Homo sapiens", "Homo sapiens"],
                "characteristics[organism part]": ["blood plasma", "blood plasma"],
                "characteristics[disease]": ["normal", "normal"],
                "characteristics[age]": ["45Y", age],
                "characteristics[sex]": ["male", sex],
                "characteristics[biological replicate]": ["1", "2"],
                "assay name": ["run1", "run2"],
                "comment[data file]": ["run1.raw", "run2.raw"],
            }
        )
    )


def _errors_for(errors, column):
    return [e for e in errors if e.column == column and e.error_type >= 40]


@pytest.mark.parametrize("word", ["pooled", "anonymized"])
def test_human_template_accepts_reserved_age_and_sex(word):
    errors = _human_sdrf(word, word).validate_sdrf(template="human", skip_ontology=True)
    assert _errors_for(errors, "characteristics[age]") == []
    assert _errors_for(errors, "characteristics[sex]") == []


def test_human_template_still_rejects_other_free_text_age():
    errors = _human_sdrf("unknown", "male").validate_sdrf(template="human", skip_ontology=True)
    assert _errors_for(errors, "characteristics[age]")
