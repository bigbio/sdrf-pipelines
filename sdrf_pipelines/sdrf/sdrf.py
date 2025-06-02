from pathlib import Path

import pandas as pd
from pydantic import BaseModel, Field


class SDRFDataFrame(BaseModel):
    df: pd.DataFrame = Field(default_factory=pd.DataFrame)
    sdrf_columns: list[str] = Field(default_factory=list)
    model_config = {"arbitrary_types_allowed": True}

    def __init__(self, df: pd.DataFrame, /, **data):
        """
        Initialize the SDRFDataFrame.

        Args:
            df: Pandas DataFrame containing the SDRF data
        """
        super().__init__(**data)
        self.df = df
        self.sdrf_columns = self.df.columns.tolist()

    def __getitem__(self, key):
        """Enable subscriptable behavior by delegating to the df attribute."""
        if self.df is None:
            raise ValueError("DataFrame is not initialized")
        return self.df[key]

    def __iter__(self):
        """Make the object iterable as if iterating over the dataframe."""
        if self.df is not None:
            return iter(self.df)
        return iter([])  # Return empty iterator if df is None

    def map(self, func, *args, **kwargs):
        """Delegate map operation to the underlying DataFrame."""
        if self.df is not None:
            return self.df.map(func, *args, **kwargs)
        raise ValueError("Cannot map on empty DataFrame")

    def get_dataframe_columns(self) -> list[str]:
        """
        Get the column names of the SDRF DataFrame.

        Returns:
            List of column names
        """
        return self.df.columns.tolist()

    def get_original_columns(self) -> list[str]:
        """
        Get the original column names of the SDRF DataFrame.

        Returns:
            List of column names
        """
        return self.sdrf_columns

    @property
    def columns(self):
        """
        Get the column names of the SDRF DataFrame.

        Returns:
            List of column names
        """
        return self.df.columns.tolist()

    @property
    def shape(self):
        """
        Get the shape of the SDRF DataFrame.

        Returns:
            Tuple of (rows, columns)
        """
        return self.df.shape


def read_sdrf(sdrf_file: str | Path):
    """
    Create an SDRFDataFrame from an SDRF file.

    Args:
        sdrf_file: Path to the SDRF file.

    Returns:
        pandas.DataFrame
    """
    return pd.read_csv(sdrf_file, sep="\t", dtype=str).fillna("")
