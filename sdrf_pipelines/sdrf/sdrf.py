from pathlib import Path
from typing import Union

import pandas as pd
from pydantic import BaseModel, Field


class SDRFDataFrame(BaseModel):
    sdrf_columns: list[str] = Field(default_factory=list)
    df: pd.DataFrame = Field(default=None)
    model_config = {"arbitrary_types_allowed": True}

    def __init__(self, sdrf_file: Union[pd.DataFrame, str, Path], /, **data):
        """
        Initialize the SDRF DataFrame.

        Args:
            sdrf_file: Pandas DataFrame containing the SDRF data
        """
        super().__init__(**data)
        if isinstance(sdrf_file, pd.DataFrame):
            self.df = sdrf_file
            self.sdrf_columns = sdrf_file.columns.tolist()
        if isinstance(sdrf_file, str) or isinstance(sdrf_file, Path):
            self.df = self.parse(sdrf_file)

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

    def parse(self, sdrf_file: str | Path) -> pd.DataFrame:
        """
        Parse an SDRF file.

        Args:
            sdrf_file: Path to the SDRF file

        Returns:
            SDRFDataFrame instance
        """

        with open(sdrf_file, "r", encoding="utf-8") as file:
            first_line = file.readline().strip()
            self.sdrf_columns = first_line.split("\t")

        df = pd.read_csv(sdrf_file, sep="\t", dtype=str)
        df.fillna("")
        return df

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
