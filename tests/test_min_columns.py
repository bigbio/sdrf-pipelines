import logging
from collections import Counter
from pathlib import Path

import pandas as pd
import pytest

from sdrf_pipelines.sdrf.schemas import SchemaRegistry, SchemaValidator
from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame, read_sdrf

TESTS_DIR = Path(__file__).parent


@pytest.mark.ontology
def test_min_columns_ms_proteomics_schema():
    """Test validation with ontology checking on a file with valid ontology terms.

    The error.sdrf.tsv file has valid ontology terms, so ontology validation
    doesn't add errors. The 6 errors are structural (column order, missing columns).
    """
    registry = SchemaRegistry()  # Default registry, but users can create their own
    validator = SchemaValidator(registry)
    test_file = TESTS_DIR / "data/generic/error.sdrf.tsv"
    sdrf_df = SDRFDataFrame(read_sdrf(test_file))
    errors = validator.validate(sdrf_df, "ms-proteomics", skip_ontology=False)
    # Same as skip_ontology=True because the file has valid ontology terms
    assert len(errors) == 6


def test_min_columns_ms_proteomics_schema_skip_ontology():
    """Test validation without ontology validation (works without OLS dependencies)."""
    registry = SchemaRegistry()
    validator = SchemaValidator(registry)
    test_file = TESTS_DIR / "data/generic/error.sdrf.tsv"
    sdrf_df = SDRFDataFrame(read_sdrf(test_file))
    errors = validator.validate(sdrf_df, "ms-proteomics", skip_ontology=True)
    # Without ontology validation, we expect fewer errors
    assert len(errors) == 6


def test_min_columns_with_reduced_columns():
    """Test that validation fails when there are fewer than the required schema columns."""
    test_df = pd.DataFrame(
        {
            "source name": ["sample 1"],
            "characteristics[organism]": ["homo sapiens23"],
            "characteristics[organism part]": [" liver "],
            "characteristics[disease] ": ["normal"],
            "characteristics[cell type]": ["hepatocyte"],
            "characteristics[age]": ["1"],
            "assay name": ["run 1"],
        }
    )
    registry = SchemaRegistry()
    validator = SchemaValidator(registry)
    sdrf_df = SDRFDataFrame(test_df)
    errors = validator.validate(sdrf_df, "human", skip_ontology=True)
    error_name_counts = Counter((error.message for error in errors if error.error_type == logging.ERROR))

    # Expected errors based on human template schema requirements (without ontology)
    expected_error_name_counts = Counter(
        {
            "Trailing whitespace detected in column name": 1,
            "Trailing whitespace detected": 1,
            "Required column 'characteristics[biological replicate]' is missing from the SDRF file": 1,
            "Required column 'technology type' is missing from the SDRF file": 1,
            "Required column 'comment[data file]' is missing from the SDRF file": 1,
            "Required column 'comment[instrument]' is missing from the SDRF file": 1,
            "Required column 'characteristics[disease]' is missing from the SDRF file": 1,
            # Age pattern validation - use the new improved error message format
            "Invalid format for value '1'": 1,
        }
    )
    assert error_name_counts == expected_error_name_counts


@pytest.mark.ontology
def test_min_columns_with_reduced_columns_with_ontology():
    """Test validation with ontology checking (requires OLS dependencies)."""
    test_df = pd.DataFrame(
        {
            "source name": ["sample 1"],
            "characteristics[organism]": ["homo sapiens23"],
            "characteristics[organism part]": [" liver "],
            "characteristics[disease] ": ["normal"],
            "characteristics[cell type]": ["hepatocyte"],
            "characteristics[age]": ["1"],
            "assay name": ["run 1"],
        }
    )
    registry = SchemaRegistry()
    validator = SchemaValidator(registry)
    sdrf_df = SDRFDataFrame(test_df)
    errors = validator.validate(sdrf_df, "human", skip_ontology=False)
    error_name_counts = Counter((error.message for error in errors if error.error_type == logging.ERROR))

    # Expected errors including ontology validation
    expected_error_name_counts = Counter(
        {
            "Trailing whitespace detected in column name": 1,
            "Trailing whitespace detected": 1,
            "Required column 'characteristics[biological replicate]' is missing from the SDRF file": 1,
            "Required column 'technology type' is missing from the SDRF file": 1,
            "Required column 'comment[data file]' is missing from the SDRF file": 1,
            "Required column 'comment[instrument]' is missing from the SDRF file": 1,
            "Required column 'characteristics[disease]' is missing from the SDRF file": 1,
            (
                "Term: homo sapiens23 in column 'characteristics[organism]', "
                "is not found in the given ontology list ncbitaxon"
            ): 1,
            # Age pattern validation - use the new improved error message format
            "Invalid format for value '1'": 1,
        }
    )
    assert error_name_counts == expected_error_name_counts
