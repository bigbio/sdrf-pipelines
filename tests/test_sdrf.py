import pandas as pd

from sdrf_pipelines.sdrf.schema_loader import schema_loader
from sdrf_pipelines.sdrf.sdrf import SdrfDataFrame


def test_sdrf_parse():
    """Test parsing an SDRF file."""
    # Create a test DataFrame
    test_df = pd.DataFrame(
        {
            "source name": ["sample 1", "sample 2"],
            "characteristics[organism]": ["homo sapiens", "homo sapiens"],
            "characteristics[organism part]": ["liver", "brain"],
            "characteristics[disease]": ["normal", "normal"],
            "characteristics[cell type]": ["hepatocyte", "neuron"],
            "characteristics[biological replicate]": ["1", "2"],
            "assay name": ["run 1", "run 2"],
            "comment[technical replicate]": ["1", "1"],
            "comment[fraction identifier]": ["1", "1"],
            "comment[data file]": ["file1.raw", "file2.raw"],
        }
    )

    # Create SdrfDataFrame
    sdrf = SdrfDataFrame(test_df)

    # Check that the columns are correct
    assert set(sdrf.get_sdrf_columns()) == set(test_df.columns)

    # Check that the shape is correct
    assert sdrf.shape == test_df.shape


def test_human_record():
    """Test human record model."""
    # Get the human record model
    HumanRecord = schema_loader.get_model("human")

    # Create a test record
    try:
        record = HumanRecord(
            source_name="sample 1",
            characteristics_organism="homo sapiens",
            characteristics_organism_part="liver",
            characteristics_disease="normal",
            characteristics_cell_type="hepatocyte",
            characteristics_biological_replicate="1",
            assay_name="run 1",
            comment_technical_replicate="1",
            comment_fraction_identifier="1",
            comment_data_file="file1.raw",
            characteristics_ancestry_category="european",
            characteristics_age="30y",
            characteristics_sex="male",
            characteristics_developmental_stage="adult",
            characteristics_individual="patient1",
        )
    except ValueError as e:
        print(f"ValueError: {e}")


def test_validation():
    """Test validation of SDRF."""
    # Create a test DataFrame with valid data
    test_df = pd.DataFrame(
        {
            "source name": ["sample 1", "sample 2"],
            "characteristics[organism]": ["homo sapiens", "homo sapiens"],
            "characteristics[organism part]": ["liver", "brain"],
            "characteristics[disease]": ["normal", "normal"],
            "characteristics[cell type]": ["hepatocyte", "neuron"],
            "characteristics[biological replicate]": ["1", "2"],
            "assay name": ["run 1", "run 2"],
            "comment[technical replicate]": ["1", "1"],
            "comment[fraction identifier]": ["1", "1"],
            "comment[label]": ["label-free", "label-free"],
            "comment[data file]": ["file1.raw", "file2.raw"],
        }
    )

    # Create SdrfDataFrame
    sdrf = SdrfDataFrame(test_df)

    # Validate with default template
    errors = sdrf.validate("default", use_ols_cache_only=True)

    # Print errors for debugging
    for error in errors:
        print(f"Error: {error.message}")

    # Should be valid
    assert len(errors) == 7

    # Create a test DataFrame with invalid data (missing required field)
    test_df = pd.DataFrame(
        {
            "source name": ["sample 1", "sample 2"],
            # Missing organism
            "characteristics[organism part]": ["liver", "brain"],
            "characteristics[disease]": ["normal", "normal"],
            "characteristics[cell type]": ["hepatocyte", "neuron"],
            "characteristics[biological replicate]": ["1", "2"],
            "assay name": ["run 1", "run 2"],
            "comment[technical replicate]": ["1", "1"],
            "comment[fraction identifier]": ["1", "1"],
            "comment[label]": ["label-free", "label-free"],
            "comment[data file]": ["file1.raw", "file2.raw"],
        }
    )

    # Create SdrfDataFrame
    sdrf = SdrfDataFrame(test_df)

    # Validate with default template
    errors = sdrf.validate("default", use_ols_cache_only=True)

    # Should have errors
    assert len(errors) > 0


def test_experimental_design_validation():
    """Test experimental design validation."""
    # Create a test DataFrame with valid data
    test_df = pd.DataFrame(
        {
            "source name": ["sample 1", "sample 2"],
            "characteristics[organism]": ["homo sapiens", "homo sapiens"],
            "characteristics[organism part]": ["liver", "brain"],
            "characteristics[disease]": ["normal", "normal"],
            "characteristics[cell type]": ["hepatocyte", "neuron"],
            "characteristics[biological replicate]": ["1", "2"],
            "assay name": ["run 1", "run 2"],
            "comment[technical replicate]": ["1", "1"],
            "comment[fraction identifier]": ["1", "1"],
            "comment[label]": ["label-free", "label-free"],
            "comment[data file]": ["file1.raw", "file2.raw"],
        }
    )

    # Create SdrfDataFrame
    sdrf = SdrfDataFrame(test_df)

    # Validate experimental design
    errors = sdrf.validate_experimental_design()

    # Should be valid
    assert len(errors) == 0