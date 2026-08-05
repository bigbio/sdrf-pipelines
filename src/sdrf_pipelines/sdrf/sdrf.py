import io
import re
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import pandas as pd
from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from sdrf_pipelines.utils.exceptions import LogicError


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

        # Parse comment[sdrf template] - can have multiple columns (including duplicates)
        # Use column indices to handle duplicate column names
        template_col_indices = [i for i, c in enumerate(df.columns) if "comment[sdrf template]" in c.lower()]
        for idx in template_col_indices:
            value = df.iloc[0, idx]
            if pd.notna(value):
                template_val = str(value)
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
                data: dict[str, str | None] = {"template": t}
                parsed = self._parse_name_version_format(t)
                if parsed:
                    data["template"] = parsed["name"]
                    data["version"] = parsed["version"]
                result.append(data)
            return result
        # Fall back to header-based
        return [p for p in self.properties if "template" in p]

    @staticmethod
    def _normalize_version(version: Optional[str]) -> Optional[str]:
        """Strip a version and prepend 'v' to a bare numeric one ('1.1.0' -> 'v1.1.0')."""
        # '2.0.0-dev' -> 'v2.0.0-dev'; already-'v'/non-numeric unchanged; empty/None -> None.
        if not version:
            return None
        version = version.strip()
        if not version:
            return None
        if not version.lower().startswith("v") and re.match(r"^\d+(?:\.\d+)*(?:-\w+)?$", version):
            version = f"v{version}"
        return version

    def _parse_name_version_format(self, value: str) -> Optional[dict[str, str | None]]:
        """Parse a comment[sdrf template] value into {name, version}, or None if empty/non-str."""
        # Formats (permissive): 'name vX.Y.Z'; key/value pairs order- and case-insensitive,
        # ';' or ',' separated ('NT=human;VV=v1.1.0', 'nt=human;version=1.1.0'); 'NT=human'
        # (no version); and a bare 'human' -> name only.
        if not isinstance(value, str):
            return None

        v = value.strip()

        # key=value pairs, order- and case-insensitive, separated by ';' or ','
        kv_map: dict[str, str] = {}
        for pair in re.split(r"[;,]", v):
            if "=" in pair:
                k, val = pair.split("=", 1)
                kv_map[k.strip().lower()] = val.strip()

        if "nt" in kv_map:
            return {"name": kv_map["nt"], "version": self._normalize_version(kv_map.get("vv") or kv_map.get("version"))}

        # Simple 'name vX.Y.Z' (the last ' v' separates the version)
        if " v" in v:
            name, _, ver = v.rpartition(" v")
            ver = ver.strip()
            return {"name": name.strip(), "version": f"v{ver}" if ver else None}

        # Bare value -> treat as a template name
        if v:
            return {"name": v, "version": None}

        return None

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

    def get_fileformat(self) -> list[dict[str, str]]:
        """Get file format (legacy method)."""
        return [p for p in self.properties if "fileformat" in p or "file_format" in p]

    def get_guidelines(self) -> list[dict[str, str]]:
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

    def __getitem__(self, key: str | list[str]) -> pd.Series | pd.DataFrame:
        """Enable subscriptable behavior by delegating to the df attribute."""
        if self.df is None:
            raise ValueError("DataFrame is not initialized")
        return self.df[key]

    def __iter__(self) -> Iterator[str]:  # type: ignore[override]
        """Make the object iterable as if iterating over the dataframe columns."""
        if self.df is not None:
            return iter(str(c) for c in self.df.columns)
        return iter([])  # Return empty iterator if df is None

    def map(self, func: Any, *args: Any, **kwargs: Any) -> pd.DataFrame:
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
    def columns(self) -> list[str]:
        """
        Get the column names of the SDRF DataFrame.

        Returns:
            List of column names
        """
        return self.df.columns.tolist()

    @property
    def shape(self) -> tuple[int, int]:
        """
        Get the shape of the SDRF DataFrame.

        Returns:
            Tuple of (rows, columns)
        """
        return self.df.shape

    def validate_sdrf(self, template: str | None = None, **kwargs: Any) -> "list[LogicError]":
        """
        Validate the SDRF DataFrame against a schema template.

        Args:
            template: Name of the schema template to validate against (e.g., 'ms-proteomics', 'human')
            **kwargs: Additional validation parameters (use_ols_cache_only, skip_ontology, etc.)

        Returns:
            List of LogicError objects representing validation errors

        Raises:
            ImportError: If schemas module cannot be imported
        """
        # Lazy import to avoid circular dependency
        from sdrf_pipelines.sdrf.schemas import SchemaRegistry, SchemaValidator

        registry = SchemaRegistry()
        validator = SchemaValidator(registry)
        schema_name = template or "ms-proteomics"
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
