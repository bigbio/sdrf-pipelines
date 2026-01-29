"""Base class for SDRF converters with common functionality."""

import re
from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


# Common regex pattern for sample identifiers
SAMPLE_IDENTIFIER_PATTERN = re.compile(r"sample (\d+)$", re.IGNORECASE)


class BaseConverter(ABC):
    """Base class for SDRF file converters.

    Provides common functionality for loading SDRF files, extracting
    factor columns, combining factors to conditions, and handling
    sample identifiers.
    """

    def __init__(self):
        self.warnings: dict[str, int] = {}
        self._sdrf: pd.DataFrame | None = None

    def add_warning(self, message: str) -> None:
        """Add or increment a warning message."""
        self.warnings[message] = self.warnings.get(message, 0) + 1

    def load_sdrf(self, sdrf_file: str) -> pd.DataFrame:
        """Load and preprocess an SDRF file.

        Args:
            sdrf_file: Path to the SDRF file

        Returns:
            Preprocessed DataFrame with lowercase column names
        """
        sdrf = pd.read_csv(sdrf_file, sep="\t")
        sdrf = sdrf.astype(str)
        sdrf.columns = sdrf.columns.str.lower()
        self._sdrf = sdrf
        return sdrf

    def parse_split_columns(self, split_by_columns: str | None) -> list[str] | None:
        """Parse the split_by_columns string into a list.

        Args:
            split_by_columns: String like '[col1,col2]' or None

        Returns:
            List of lowercase column names or None
        """
        if not split_by_columns:
            return None

        # Remove brackets and split
        columns = split_by_columns[1:-1].split(",")
        # Lowercase all column names
        return [col.strip().lower() for col in columns]

    def get_factor_columns(self, sdrf: pd.DataFrame) -> list[str]:
        """Get factor value columns from the SDRF.

        Args:
            sdrf: The SDRF DataFrame

        Returns:
            List of column names starting with 'factor value['
        """
        return [c for c in sdrf.columns if c.startswith("factor value[")]

    def get_characteristics_columns(self, sdrf: pd.DataFrame) -> list[str]:
        """Get characteristics columns from the SDRF.

        Args:
            sdrf: The SDRF DataFrame

        Returns:
            List of column names starting with 'characteristics['
        """
        return [c for c in sdrf.columns if c.startswith("characteristics[")]

    def combine_factors_to_conditions(
        self,
        factor_cols: list[str],
        row: pd.Series,
        separator: str = "_",
        fallback_column: str = "source name",
    ) -> str:
        """Combine factor columns into a single condition string.

        Args:
            factor_cols: List of factor column names
            row: A row from the SDRF DataFrame
            separator: String to join factors with (default: "_")
            fallback_column: Column to use if no factors found (default: "source name")

        Returns:
            Combined condition string
        """
        if not factor_cols:
            self.add_warning(f"No factors specified. Using {fallback_column} as condition.")
            return str(row.get(fallback_column, "unknown"))

        all_factors = [str(row[col]) for col in factor_cols if col in row.index]
        combined = separator.join(all_factors)

        if not combined:
            self.add_warning(f"No factors specified. Using {fallback_column} as condition.")
            return str(row.get(fallback_column, "unknown"))

        return combined

    def extract_sample_id(
        self,
        source_name: str,
        sample_id_map: dict[str, int],
        next_sample_id: int,
    ) -> tuple[str, int]:
        """Extract sample ID from source name.

        Args:
            source_name: The source name string
            sample_id_map: Mapping of source names to sample IDs
            next_sample_id: Next available sample ID

        Returns:
            Tuple of (sample_id as string, updated next_sample_id)
        """
        match = SAMPLE_IDENTIFIER_PATTERN.search(source_name)
        if match:
            return match.group(1), next_sample_id

        # No match - use or create mapping
        self.add_warning("No sample number identifier found in source name")

        if source_name in sample_id_map:
            return str(sample_id_map[source_name]), next_sample_id

        sample_id_map[source_name] = next_sample_id
        return str(next_sample_id), next_sample_id + 1

    def get_technical_replicate(self, row: pd.Series, default: str = "1") -> str:
        """Get technical replicate value from a row.

        Args:
            row: A row from the SDRF DataFrame
            default: Default value if column not present

        Returns:
            Technical replicate value as string
        """
        if "comment[technical replicate]" in row.index:
            value = str(row["comment[technical replicate]"])
            if value.lower() not in ("", "nan", "not available"):
                return value
        return default

    def get_fraction_identifier(self, row: pd.Series, default: str = "1") -> str:
        """Get fraction identifier from a row.

        Args:
            row: A row from the SDRF DataFrame
            default: Default value if column not present

        Returns:
            Fraction identifier as string
        """
        if "comment[fraction identifier]" in row.index:
            value = str(row["comment[fraction identifier]"])
            if value.lower() not in ("", "nan", "not available"):
                return value
        return default

    def report_warnings(self) -> None:
        """Print all accumulated warnings."""
        for message, count in self.warnings.items():
            print(f'WARNING: "{message}" occurred {count} times.')

    @abstractmethod
    def convert(self, sdrf_file: str, output_path: str, **kwargs) -> None:
        """Convert an SDRF file to the target format.

        Args:
            sdrf_file: Path to the input SDRF file
            output_path: Path for the output file
            **kwargs: Additional converter-specific arguments
        """
        pass


class ConditionBuilder:
    """Helper class for building conditions from SDRF rows."""

    def __init__(self, factor_cols: list[str], separator: str = "_"):
        self.factor_cols = factor_cols
        self.separator = separator
        self.conditions: list[str] = []

    def add_from_row(self, row: pd.Series, fallback: str | None = None) -> str:
        """Add a condition from an SDRF row.

        Args:
            row: A row from the SDRF DataFrame
            fallback: Fallback value if no factors found

        Returns:
            The condition string
        """
        factors = [str(row[col]) for col in self.factor_cols if col in row.index]
        condition = self.separator.join(factors) if factors else (fallback or "")
        self.conditions.append(condition)
        return condition

    def get_unique_conditions(self) -> list[str]:
        """Get unique conditions in order of first appearance."""
        seen = set()
        unique = []
        for c in self.conditions:
            if c not in seen:
                seen.add(c)
                unique.append(c)
        return unique


class SampleTracker:
    """Helper class for tracking sample IDs and bio-replicates."""

    def __init__(self):
        self.sample_id_map: dict[str, int] = {}
        self.bio_replicates: list[str] = []
        self._next_id = 1

    def get_sample_id(self, source_name: str) -> str:
        """Get or create a sample ID for a source name.

        Args:
            source_name: The source name string

        Returns:
            Sample ID as string
        """
        match = SAMPLE_IDENTIFIER_PATTERN.search(source_name)
        if match:
            sample_id = match.group(1)
        elif source_name in self.sample_id_map:
            sample_id = str(self.sample_id_map[source_name])
        else:
            self.sample_id_map[source_name] = self._next_id
            sample_id = str(self._next_id)
            self._next_id += 1

        if sample_id not in self.bio_replicates:
            self.bio_replicates.append(sample_id)

        return sample_id

    def get_bio_replicate_index(self, sample_id: str) -> int:
        """Get the bio-replicate index (1-based) for a sample ID.

        Args:
            sample_id: The sample ID

        Returns:
            1-based index in the bio-replicates list
        """
        if sample_id not in self.bio_replicates:
            self.bio_replicates.append(sample_id)
        return self.bio_replicates.index(sample_id) + 1
