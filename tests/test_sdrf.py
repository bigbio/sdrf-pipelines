import pandas as pd

from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame


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

    # Create SDRFDataFrame
    sdrf = SDRFDataFrame(test_df)

    # Check that the columns are correct
    assert set(sdrf.get_dataframe_columns()) == set(test_df.columns)

    # Check that the shape is correct
    assert sdrf.shape == test_df.shape
