"""
Test to ensure the minimum number of columns is 7 using the default schema.
Also tests that the validator checks that all the columns and rules in the schema are valid.
"""

from pathlib import Path

import pandas as pd

from sdrf_pipelines.sdrf.schema_loader import schema_loader
from sdrf_pipelines.sdrf.sdrf import SdrfDataFrame
from sdrf_pipelines.sdrf.validators.base import SDRFValidator

TESTS_DIR = Path(__file__).parent


def test_min_columns_default_schema():
    """Test that the minimum number of columns is 7 using the default schema."""
    default_schema = schema_loader.get_schema("default")
    assert default_schema.min_columns == 17, f"Expected min_columns to be 17, got {default_schema.min_columns}"

    test_file = TESTS_DIR / "data/generic/sdrf.tsv"
    print(f"Using test file: {test_file}")
    sdrf = SdrfDataFrame.parse(str(test_file))
    num_columns = len(sdrf.get_sdrf_columns())
    assert num_columns >= 7, f"Expected at least 7 columns, got {num_columns}"

    errors = sdrf.validate("default", use_ols_cache_only=True)
    min_column_errors = [error for error in errors if "number of columns" in error.message.lower()]
    assert not min_column_errors, f"Found errors related to minimum columns: {min_column_errors}"
    print(f"The SDRF file has {num_columns} columns, which is >= the minimum of 7")


def test_min_columns_with_reduced_columns():
    """Test that validation fails when there are fewer than 7 columns."""
    test_df = pd.DataFrame(
        {
            "source name": ["sample 1"],
            "characteristics[organism]": ["homo sapiens"],
            "characteristics[organism part]": ["liver"],
            "characteristics[disease]": ["normal"],
            "characteristics[cell type]": ["hepatocyte"],
            "assay name": ["run 1"],
        }
    )

    sdrf = SdrfDataFrame(test_df)
    num_columns = len(sdrf.get_sdrf_columns())
    assert num_columns < 7, f"Expected fewer than 7 columns, got {num_columns}"

    errors = sdrf.validate("default", use_ols_cache_only=True)
    min_column_errors = [error for error in errors if "number of columns" in error.message.lower()]
    assert min_column_errors, "Expected errors related to minimum columns, but found none"
    for error in min_column_errors:
        print(f"Error: {error.message}")


def test_validate_columns_and_rules():
    """Test that the validator checks that all the columns and rules in the schema are valid."""
    default_schema = schema_loader.get_schema("default")
    test_file = TESTS_DIR / "data/generic/sdrf.tsv"
    sdrf = SdrfDataFrame.parse(test_file)
    errors = sdrf.validate("default", use_ols_cache_only=True)

    for error in errors:
        print(f"Error: {error.message}")

    assert len(errors) == 2
    missing_fields = ["technical replicate", "technology type"]
    for field in missing_fields:
        field_errors = [error for error in errors if field in error.message.lower()]
        assert field_errors, f"Expected errors related to {field}, but found none"

    validator = SDRFValidator()
    try:
        validator.validate_whitespace("sample 1")
        print("Whitespace validation passed for 'sample 1'")
    except ValueError as e:
        assert False, f"Whitespace validation failed for 'sample 1': {str(e)}"

    try:
        validator.validate_whitespace(" sample 1 ")
        assert False, "Whitespace validation should have failed for ' sample 1 '"
    except ValueError as e:
        print(f"Whitespace validation correctly failed for ' sample 1 ': {str(e)}")

    try:
        validator.validate_pattern(
            "30y", r"(?:^(?:\d+y)?(?:\d+m)?(?:\d+d)?$)|(?:not available)|(?:not applicable)", case_sensitive=False
        )
        print("Pattern validation passed for '30y'")
    except ValueError as e:
        assert False, f"Pattern validation failed for '30y': {str(e)}"

    try:
        validator.validate_pattern(
            "invalid", r"(?:^(?:\d+y)?(?:\d+m)?(?:\d+d)?$)|(?:not available)|(?:not applicable)", case_sensitive=False
        )
        assert False, "Pattern validation should have failed for 'invalid'"
    except ValueError as e:
        print(f"Pattern validation correctly failed for 'invalid': {str(e)}")
