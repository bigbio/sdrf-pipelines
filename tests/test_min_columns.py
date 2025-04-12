from pathlib import Path
import pandas as pd
from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame
from sdrf_pipelines.sdrf.schemas import SchemaRegistry, SchemaValidator

TESTS_DIR = Path(__file__).parent


def test_min_columns_default_schema():
    """Test that the minimum number of columns is 7 using the default schema."""
    registry = SchemaRegistry()  # Default registry, but users can create their own
    validator = SchemaValidator(registry)
    test_file = TESTS_DIR / "data/generic/sdrf.tsv"
    sdrf_df = SDRFDataFrame(test_file)
    errors = validator.validate(sdrf_df, "default")
    num_colums = sdrf_df.get_dataframe_columns()
    print(f"Using test file: {test_file} with {len(num_colums)} columns")

    assert len(errors) == 3


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
    numb_column = sdrf_df.get_dataframe_columns()
    print(f"Using dataframe with {len(numb_column)} columns")
    min_column_errors = [
        error for error in errors if "number of columns" in error.message.lower()
    ]
    assert (
        min_column_errors
    ), "Expected errors related to minimum columns, but found none"

    assert len(errors) == 14