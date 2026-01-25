import logging
from collections import Counter
from pathlib import Path

import pandas as pd

from sdrf_pipelines.sdrf.schemas import SchemaRegistry, SchemaValidator
from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame, read_sdrf

TESTS_DIR = Path(__file__).parent


def test_min_columns_default_schema():
    registry = SchemaRegistry()  # Default registry, but users can create their own
    validator = SchemaValidator(registry)
    test_file = TESTS_DIR / "data/generic/error.sdrf.tsv"
    sdrf_df = SDRFDataFrame(read_sdrf(test_file))
    errors = validator.validate(sdrf_df, "default", skip_ontology=False)
    assert len(errors) == 139


def test_min_columns_with_reduced_columns():
    """Test that validation fails when there are fewer than the default schema with 12 columns.
    Test result should be 15 errors."""
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
    expected_error_name_counts = Counter(
        {
            "The number of columns is lower than the mandatory number 12": 1,
            "Trailing whitespace detected in column name": 1,
            "Trailing whitespace detected": 1,
            "Columns not found in DataFrame: comment[label]": 1,
            "Required column 'characteristics[biological replicate]' is missing": 1,
            "Required column 'technology type' is missing": 1,
            "Required column 'comment[technical replicate]' is missing": 1,
            "Required column 'comment[fraction identifier]' is missing": 1,
            "Required column 'comment[label]' is missing": 1,
            "Required column 'comment[data file]' is missing": 1,
            "Required column 'comment[instrument]' is missing": 1,
            "Required column 'comment[proteomics data acquisition method]' is missing": 1,
            "Required column 'characteristics[disease]' is missing": 1,
            (
                "Term: homo sapiens23 in column 'characteristics[organism]', "
                "is not found in the given ontology list ncbitaxon"
            ): 1,
            (
                "Value '1' in column 'characteristics[age]' does not match the required pattern: "
                "^(?:(?:\\d+[yY])(?:\\d+[mM])?(?:\\d+[wW])?(?:\\d+[dD])?|(?:\\d+[yY])?(?:\\d+[mM])(?:\\d+[wW])?(?:\\d+[dD])?|"
                "(?:\\d+[yY])?(?:\\d+[mM])?(?:\\d+[wW])(?:\\d+[dD])?|(?:\\d+[yY])?(?:\\d+[mM])?(?:\\d+[wW])?(?:\\d+[dD])|"
                "(?:(?:\\d+[yY])(?:\\d+[mM])?(?:\\d+[wW])?(?:\\d+[dD])?-(?:\\d+[yY])(?:\\d+[mM])?(?:\\d+[wW])?(?:\\d+[dD])?)|"
                "(?:(?:\\d+[yY])?(?:\\d+[mM])(?:\\d+[wW])?(?:\\d+[dD])?-(?:\\d+[yY])?(?:\\d+[mM])(?:\\d+[wW])?(?:\\d+[dD])?)|"
                "(?:(?:\\d+[yY])?(?:\\d+[mM])?(?:\\d+[wW])(?:\\d+[dD])?-(?:\\d+[yY])?(?:\\d+[mM])?(?:\\d+[wW])(?:\\d+[dD])?)|"
                "(?:(?:\\d+[yY])?(?:\\d+[mM])?(?:\\d+[wW])?(?:\\d+[dD])-(?:\\d+[yY])?(?:\\d+[mM])?(?:\\d+[wW])?(?:\\d+[dD]))|"
                "(?:not available)|(?:not applicable))$"
            ): 1,
        }
    )
    assert error_name_counts == expected_error_name_counts
