import io
from pathlib import Path
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field


class SDRFMetadata:
    """
    Class to hold metadata about the SDRF DataFrame.
    """

    def __init__(self, str_content: Optional[str] = None, property_indicator: str = "#"):
        self.str_content = str_content
        self.property_indicator = property_indicator
        self.properties: list[dict[str, str]] = []
        if str_content:
            self.parse(str_content)

    def parse(self, str_content: str):
        lines = str_content.split("\n")
        for line in lines:
            if line.startswith(self.property_indicator):
                line = line[len(self.property_indicator) :].strip()
                data = {}
                for kv in line.split(";"):
                    key, value = kv.split("=", 1)
                    data[key] = value
                self.properties.append(data)

    def get_templates(self) -> list[dict]:
        return [p for p in self.properties if "template" in p]

    def get_fileformat(self):
        return [p for p in self.properties if "fileformat" in p]

    def get_guidelines(self):
        return [p for p in self.properties if "guideline" in p]


class SDRFDataFrame(BaseModel):
    df: pd.DataFrame = Field(default_factory=pd.DataFrame)
    sdrf_columns: list[str] = Field(default_factory=list)
    model_config = {"arbitrary_types_allowed": True}
    metadata: Optional[SDRFMetadata] = Field(default_factory=SDRFMetadata)

    def __init__(self, df: pd.DataFrame, /, **data):
        """
        Initialize the SDRFDataFrame.

        Args:
            df: Pandas DataFrame containing the SDRF data
        """
        super().__init__(**data)
        if isinstance(df, pd.DataFrame):
            self.df = df
            self.sdrf_columns = self.df.columns.tolist()
            self.metadata = None
        elif isinstance(df, SDRFDataFrame):
            self.df = df.df
            self.sdrf_columns = df.sdrf_columns
            self.metadata = df.metadata

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

    def validate_sdrf(self, template: str | None = None, **kwargs):
        """
        Validate the SDRF DataFrame against a schema template.

        Args:
            template: Name of the schema template to validate against (e.g., 'default', 'human')
            **kwargs: Additional validation parameters (use_ols_cache_only, skip_ontology, etc.)

        Returns:
            List of validation errors

        Raises:
            ImportError: If schemas module cannot be imported
        """
        # Lazy import to avoid circular dependency
        from sdrf_pipelines.sdrf.schemas import SchemaRegistry, SchemaValidator

        registry = SchemaRegistry()
        validator = SchemaValidator(registry)
        schema_name = template or "default"
        return validator.validate(self, schema_name, **kwargs)


def read_sdrf(sdrf_file: str | Path | io.StringIO) -> SDRFDataFrame:
    """
    Create an SDRFDataFrame from an SDRF file.

    Args:
        sdrf_file: Path to the SDRF file or string content of the SDRF file or StringIO object.

    Returns:
        SDRFDataFrame object.
    """
    df = pd.DataFrame()
    metadata = ""

    def _read_sdrf_file(file) -> tuple[pd.DataFrame, str]:
        metadata = ""
        for line in file:
            if line.strip():
                if line.startswith("#"):
                    metadata += f"{line}\n"
                    continue
                else:
                    break
        if hasattr(file, "seek"):
            file.seek(0)
        return pd.read_csv(file, sep="\t", dtype=str, comment="#").fillna(""), metadata

    if isinstance(sdrf_file, Path):
        with open(sdrf_file, "rt") as file:
            df, metadata = _read_sdrf_file(file)
    elif isinstance(sdrf_file, str):
        try:
            with open(sdrf_file, "rt") as file:
                df, metadata = _read_sdrf_file(file)
        except OSError:
            df, metadata = _read_sdrf_file(io.StringIO(sdrf_file))
    elif isinstance(sdrf_file, io.StringIO):
        sdrf_file.seek(0)
        df, metadata = _read_sdrf_file(sdrf_file)
    if not df.empty:
        sdrf_df = SDRFDataFrame(df)
        sdrf_df.metadata = SDRFMetadata(metadata)
        return sdrf_df
    raise ValueError("No valid data found in the file")
