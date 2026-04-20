"""End-to-end validation tests for metabolomics SDRF templates.

Loads each example SDRF (3 re-annotated MTBLS deposits + 1 MetaboBank deposit)
and asserts that validation against the corresponding template produces no errors
(warnings are allowed). Ontology lookups are skipped because they require the
parquet ontology cache, which is not always available in CI; the syntactic and
required-column checks still cover the bulk of the validation surface.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd
import pytest

from sdrf_pipelines.sdrf.schemas.registry import SchemaRegistry
from sdrf_pipelines.sdrf.schemas.validator import SchemaValidator

DATA_DIR = Path(__file__).parent / "data" / "metabolomics"

CASES = [
    ("MTBLS1129.sdrf.tsv", "lc-ms-metabolomics"),
    ("MTBLS1903.sdrf.tsv", "lc-ms-metabolomics"),
    ("MTBLS547.sdrf.tsv", "lc-ms-metabolomics"),
    ("MTBKS5.sdrf.tsv", "gc-ms-metabolomics"),
]


def _load_sdrf(path: Path) -> pd.DataFrame:
    """Load an SDRF TSV and strip pandas '.N' duplicate-column suffixes."""
    df = pd.read_csv(path, sep="\t", dtype=str)
    df.columns = [re.sub(r"\]\.\d+$", "]", c) for c in df.columns]
    return df


@pytest.fixture(scope="module")
def validator() -> SchemaValidator:
    return SchemaValidator(SchemaRegistry())


@pytest.mark.parametrize(("filename", "schema_name"), CASES)
def test_metabolomics_example_validates(filename: str, schema_name: str, validator: SchemaValidator) -> None:
    path = DATA_DIR / filename
    df = _load_sdrf(path)

    errors = validator.validate(df, schema_name, skip_ontology=True)
    blocking = [e for e in errors if getattr(e, "error_type", logging.ERROR) == logging.ERROR]

    assert not blocking, f"{filename} produced {len(blocking)} blocking errors against {schema_name}: " + "; ".join(
        str(e)[:150] for e in blocking[:5]
    )
