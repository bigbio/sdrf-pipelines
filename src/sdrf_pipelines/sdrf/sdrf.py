import io
from pathlib import Path
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field


class SDRFMetadata:
    """
    Class to hold metadata about the SDRF DataFrame.

    Supports both:
    - Header-based metadata (legacy format with #key=value lines)
    - Column-based metadata (v1.1.0+ format with comment[sdrf *] columns)
    """

    def __init__(
        self,
        str_content: Optional[str] = None,
        df: Optional[pd.DataFrame] = None,
        property_indicator: str = "#",
    ):
        self.str_content = str_content
        self.property_indicator = property_indicator
        self.properties: list[dict[str, str]] = []

        # Column-based metadata
        self.version: Optional[str] = None
        self.templates: list[str] = []
        self.annotation_tool: Optional[str] = None
        self.validation_hash: Optional[str] = None

        # Parse header-based metadata (legacy)
        if str_content:
            self._parse_headers(str_content)

        # Parse column-based metadata (v1.1.0+)
        if df is not None:
            self._parse_columns(df)

    def _parse_headers(self, str_content: str):
        """Parse metadata from header comment lines (legacy format)."""
        lines = str_content.split("\n")
        for line in lines:
            if line.startswith(self.property_indicator):
                line = line[len(self.property_indicator) :].strip()
                data = {}
                # Handle template format: template=name,version=vX.Y.Z
                if "=" in line:
                    for kv in line.split(";"):
                        if "=" in kv:
                            key, value = kv.split("=", 1)
                            data[key.strip()] = value.strip()
                    self.properties.append(data)

                    # Also populate structured fields for backward compatibility
                    if "version" in data and "template" not in data:
                        self.version = data["version"]
                    if "template" in data:
                        template_str = data["template"]
                        if "version" in data:
                            template_str = f"{template_str},version={data['version']}"
                        self.templates.append(template_str)
                    if "source" in data:
                        self.annotation_tool = data["source"]

    def _parse_columns(self, df: pd.DataFrame):
        """Parse metadata from column values (v1.1.0+ format)."""
        if df.empty:
            return

        # Get first row values for metadata columns
        first_row = df.iloc[0] if len(df) > 0 else None
        if first_row is None:
            return

        # Parse comment[sdrf version]
        version_cols = [c for c in df.columns if "comment[sdrf version]" in c.lower()]
        if version_cols and pd.notna(first_row.get(version_cols[0])):
            self.version = str(first_row[version_cols[0]])

        # Parse comment[sdrf template] - can have multiple columns
        template_cols = [c for c in df.columns if "comment[sdrf template]" in c.lower()]
        for col in template_cols:
            if pd.notna(first_row.get(col)):
                template_val = str(first_row[col])
                if template_val and template_val not in self.templates:
                    self.templates.append(template_val)

        # Parse comment[sdrf annotation tool]
        tool_cols = [c for c in df.columns if "comment[sdrf annotation tool]" in c.lower()]
        if tool_cols and pd.notna(first_row.get(tool_cols[0])):
            self.annotation_tool = str(first_row[tool_cols[0]])

        # Parse comment[sdrf validation hash]
        hash_cols = [c for c in df.columns if "comment[sdrf validation hash]" in c.lower()]
        if hash_cols and pd.notna(first_row.get(hash_cols[0])):
            self.validation_hash = str(first_row[hash_cols[0]])

    def get_templates(self) -> list[dict]:
        """Get templates as list of dicts (legacy format compatibility)."""
        # First check column-based templates
        if self.templates:
            result = []
            for t in self.templates:
                data = {"template": t}
                # Parse new format: NT=name;version=vX.Y.Z
                if t.startswith("NT=") and ";version=" in t:
                    # Key=value format: NT=template_name;version=vX.Y.Z
                    parts = t[3:].split(";version=")  # Remove "NT=" prefix
                    data["template"] = parts[0]
                    data["version"] = parts[1] if len(parts) > 1 else None
                elif " v" in t:
                    # Simple format: name vX.Y.Z
                    parts = t.rsplit(" v", 1)
                    data["template"] = parts[0]
                    data["version"] = "v" + parts[1] if len(parts) > 1 else None
                result.append(data)
            return result
        # Fall back to header-based
        return [p for p in self.properties if "template" in p]

    def get_version(self) -> Optional[str]:
        """Get SDRF specification version."""
        if self.version:
            return self.version
        # Fall back to header-based
        for p in self.properties:
            if "version" in p and "template" not in p:
                return p["version"]
        return None

    def get_annotation_tool(self) -> Optional[str]:
        """Get annotation tool/source."""
        if self.annotation_tool:
            return self.annotation_tool
        # Fall back to header-based
        for p in self.properties:
            if "source" in p:
                return p["source"]
        return None

    def get_fileformat(self):
        """Get file format (legacy method)."""
        return [p for p in self.properties if "fileformat" in p or "file_format" in p]

    def get_guidelines(self):
        """Get guidelines (legacy method)."""
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
        data_lines = []
        for line in file:
            if line.strip():
                if line.startswith("#"):
                    metadata += f"{line}\n"
                else:
                    data_lines.append(line)
        # Create StringIO from non-comment lines and read with pandas
        # Don't use comment="#" as SDRF values can contain # (e.g., siKMT9#1)
        if data_lines:
            return pd.read_csv(io.StringIO("".join(data_lines)), sep="\t", dtype=str).fillna(""), metadata
        return pd.DataFrame(), metadata

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
        # Parse metadata from both headers (legacy) and columns (v1.1.0+)
        sdrf_df.metadata = SDRFMetadata(str_content=metadata, df=df)
        return sdrf_df
    raise ValueError("No valid data found in the file")
