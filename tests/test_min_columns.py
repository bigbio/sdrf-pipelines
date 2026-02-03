import logging
from pathlib import Path

import pandas as pd
import pytest

from sdrf_pipelines.sdrf.schemas import SchemaRegistry, SchemaValidator
from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame, read_sdrf
from sdrf_pipelines.utils.error_codes import ErrorCode
from sdrf_pipelines.utils.manifest import ValidationManifest

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
    """Test that validation fails when there are fewer than the required schema columns.

    Uses ValidationManifest for robust error checking by error code rather than
    brittle message string matching.
    """
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

    # Use ValidationManifest for cleaner, more maintainable error checking
    manifest = ValidationManifest.from_errors(errors)

    # Check error counts by code (more robust than checking exact messages)
    assert manifest.count_by_code(ErrorCode.TRAILING_WHITESPACE_COLUMN_NAME) == 1
    assert manifest.count_by_code(ErrorCode.TRAILING_WHITESPACE) == 1
    assert manifest.count_by_code(ErrorCode.MISSING_REQUIRED_COLUMN) == 5
    assert manifest.count_by_code(ErrorCode.PATTERN_MISMATCH) == 1

    # Check that specific columns are flagged as missing
    missing_col_errors = manifest.filter_by_code(ErrorCode.MISSING_REQUIRED_COLUMN)
    missing_columns = {e.column for e in missing_col_errors}
    assert "characteristics[biological replicate]" in missing_columns
    assert "technology type" in missing_columns
    assert "comment[data file]" in missing_columns
    assert "comment[instrument]" in missing_columns
    assert "characteristics[disease]" in missing_columns

    # Check that age pattern validation failed
    pattern_errors = manifest.filter_by_column("characteristics[age]")
    assert len(pattern_errors) == 1
    assert pattern_errors[0].error_code == ErrorCode.PATTERN_MISMATCH

    # Total errors (only count ERROR level, not warnings)
    error_only = [e for e in errors if e.error_type == logging.ERROR]
    assert len(error_only) == 8


@pytest.mark.ontology
def test_min_columns_with_reduced_columns_with_ontology():
    """Test validation with ontology checking (requires OLS dependencies).

    Uses ValidationManifest for robust error checking by error code.
    """
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

    # Use ValidationManifest for robust assertions
    manifest = ValidationManifest.from_errors(errors)

    # Structure errors
    assert manifest.count_by_code(ErrorCode.TRAILING_WHITESPACE_COLUMN_NAME) == 1
    assert manifest.count_by_code(ErrorCode.TRAILING_WHITESPACE) == 1
    assert manifest.count_by_code(ErrorCode.MISSING_REQUIRED_COLUMN) == 5

    # Pattern errors
    assert manifest.count_by_code(ErrorCode.PATTERN_MISMATCH) == 1

    # Ontology errors - this is the additional check when skip_ontology=False
    # Two ontology errors: invalid organism and organism part with leading whitespace
    assert manifest.count_by_code(ErrorCode.ONTOLOGY_TERM_NOT_FOUND) == 2
    ontology_errors = manifest.filter_by_code(ErrorCode.ONTOLOGY_TERM_NOT_FOUND)
    ontology_columns = {e.column for e in ontology_errors}
    assert "characteristics[organism]" in ontology_columns
    assert "characteristics[organism part]" in ontology_columns

    # Total errors (9 errors + 1 warning for organism part)
    error_only = [e for e in errors if e.error_type == logging.ERROR]
    assert len(error_only) == 9
    assert manifest.warning_count == 1
