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
    errors = validator.validate(sdrf_df, "default")
    num_colums = sdrf_df.get_dataframe_columns()
    assert len(errors) == 138


def test_min_columns_with_reduced_columns():
    """Test that validation fails when there are fewer than 7 columns."""
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
    errors = validator.validate(sdrf_df, "default")
    error_name_counts = Counter((error.message for error in errors))
    expected_error_name_counts = Counter(
        (
            "The number of columns is lower than the mandatory number 12",
            "Trailing whitespace detected in column name",
            "Trailing whitespace detected",
            "Required column 'characteristics[biological replicate]' is missing",
            "Required column 'technology name' is missing",
            "Required column 'comment[technical replicate]' is missing",
            "Required column 'comment[fraction identifier]' is missing",
            "Required column 'comment[label]' is missing",
            "Required column 'comment[data file]' is missing",
            "Required column 'comment[instrument]' is missing",
            "Required column 'characteristics[disease]' is missing",
            "Term: homo sapiens23 in column 'characteristics[organism]', is not found in the given ontology list ncbitaxon",
            r"Value '1' in column 'characteristics[age]' does not match the required pattern: (?:^(?:\d+y)?(?:\d+m)?(?:\d+d)?$)|(?:not available)|(?:not applicable)",
        )
    )

    assert error_name_counts == expected_error_name_counts
