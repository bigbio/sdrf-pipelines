"""
SDRF module for parsing and validating SDRF files.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pydantic
from pydantic import BaseModel, Field
import pandas as pd

from sdrf_pipelines.sdrf.schema_loader import schema_loader
from sdrf_pipelines.sdrf.validators.base import create_field_with_validators
from sdrf_pipelines.sdrf.validators.ontology import OntologyTermValidator
from sdrf_pipelines.utils.exceptions import LogicError


class SDRFRecord(BaseModel):
    """Base model for SDRF records."""

    source_name: str = create_field_with_validators(
        description="Source name. This field is required and cannot be empty.",
        validate_whitespace=True,
    )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SDRFRecord":
        """
        Create a record from a dictionary.

        Args:
            data: Dictionary with record data

        Returns:
            SDRFRecord instance
        """
        # Convert SDRF column names to model field names
        field_data = {}
        for key, value in data.items():
            key = key.lower()
            if key == "source name":
                field_name = "source_name"
            elif key.startswith("characteristics["):
                # Convert 'characteristics[organism]' to 'characteristics_organism'
                attribute = key.replace("characteristics[", "").replace("]", "")
                # Replace spaces with underscores in the attribute name
                attribute = attribute.replace(" ", "_")
                field_name = f"characteristics_{attribute}"
            elif key.startswith("comment["):
                # Convert 'comment[data file]' to 'comment_data_file'
                attribute = key.replace("comment[", "").replace("]", "").replace(" ", "_")
                field_name = f"comment_{attribute}"
            elif key.startswith("factor value["):
                # Convert 'factor value[treatment]' to 'factor_value_treatment'
                attribute = key.replace("factor value[", "").replace("]", "").replace(" ", "_")
                field_name = f"factor_value_{attribute}"
            else:
                # Convert 'assay name' to 'assay_name'
                field_name = key.replace(" ", "_")
            field_data[field_name] = value

        return cls(**field_data)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the record to a dictionary with SDRF column names.

        Returns:
            Dictionary with SDRF column names
        """
        data = {}
        for field_name, field_value in self.model_dump().items():
            # Skip None values
            if field_value is None:
                continue

            # Convert 'source_name' to 'source name'
            if field_name == "source_name":
                sdrf_name = "source name"
            elif field_name.startswith("characteristics_"):
                # Convert 'characteristics_organism' to 'characteristics[organism]'
                attribute = field_name.replace("characteristics_", "")
                # Replace underscores with spaces in the attribute name
                attribute = attribute.replace("_", " ")
                sdrf_name = f"characteristics[{attribute}]"
            elif field_name.startswith("comment_"):
                # Convert 'comment_data_file' to 'comment[data file]'
                attribute = field_name.replace("comment_", "").replace("_", " ")
                sdrf_name = f"comment[{attribute}]"
            elif field_name.startswith("factor_value_"):
                # Convert 'factor_value_treatment' to 'factor value[treatment]'
                attribute = field_name.replace("factor_value_", "").replace("_", " ")
                sdrf_name = f"factor value[{attribute}]"
            else:
                # Convert 'assay_name' to 'assay name'
                sdrf_name = field_name.replace("_", " ")

            data[sdrf_name] = field_value

        return data

    def validate_record(self, use_ols_cache_only: bool = False) -> List[LogicError]:
        """
        Validate the record.

        Args:
            use_ols_cache_only: Whether to use only the cache for ontology validation

        Returns:
            List of validation errors
        """
        errors = []

        # This method can be overridden by subclasses to add custom validation

        return errors


# Define constants
DEFAULT_TEMPLATE = "default"
ALL_TEMPLATES = ["vertebrates", "nonvertebrates", "plants", "cell_lines", "human"]


class SdrfDataFrame(BaseModel):
    """
    SDRF DataFrame class for parsing and validating SDRF files.
    """

    df: pd.DataFrame = Field(default=None)

    model_config = {"arbitrary_types_allowed": True}

    def __init__(self, df: pd.DataFrame, /, **data: Any):
        """
        Initialize the SDRF DataFrame.

        Args:
            df: Pandas DataFrame containing the SDRF data
        """
        super().__init__(**data)
        self.df = df

    @classmethod
    def parse(cls, sdrf_file: Union[str, Path]) -> "SdrfDataFrame":
        """
        Parse an SDRF file.

        Args:
            sdrf_file: Path to the SDRF file

        Returns:
            SdrfDataFrame instance
        """
        df = pd.read_csv(sdrf_file, sep="\t", dtype=str)
        # Replace NaN with empty string
        df = df.fillna("")
        return cls(df)

    def get_sdrf_columns(self) -> List[str]:
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

    def validate(self, template: str, use_ols_cache_only: bool = False) -> List[LogicError]:
        """
        Validate the SDRF DataFrame.

        Args:
            template: Template name to determine the validation rules
            use_ols_cache_only: Whether to use only the cache for ontology validation

        Returns:
            List of validation errors
        """
        errors = []

        # Check the minimum number of columns
        try:
            schema = schema_loader.get_schema(template)
            min_columns = schema.min_columns
            if len(self.get_sdrf_columns()) < min_columns:
                error_message = (
                    f"The number of columns in the SDRF ({len(self.get_sdrf_columns())}) "
                    f"is smaller than the number of mandatory fields ({min_columns})"
                )
                errors.append(LogicError(error_message, error_type=logging.WARN))
        except ValueError:
            # If the schema is not found, skip the min_columns check
            pass

        # Validate each row
        for i, row in self.df.iterrows():
            try:
                # Convert row to record
                record = self.row_to_record(row, template)

                # Validate record
                record_errors = record.validate_record(use_ols_cache_only=use_ols_cache_only)
                errors.extend(record_errors)
            except pydantic.ValidationError as e:
                for error in e.errors():
                    if error["type"] == "missing" and schema is not None:
                        field_name = error["loc"][0]
                        sdrf_name_field = next((field for field in schema.fields if field.name == field_name), None)
                        if sdrf_name_field is not None:
                            errors.append(
                                LogicError(
                                    f"The following field, {sdrf_name_field.description} is required, use the column {sdrf_name_field.sdrf_name}",
                                    error_type=logging.ERROR,
                                )
                            )
                    else:
                        errors.append(LogicError(f"Error validating row {i}: {str(e)}", error_type=logging.ERROR))
            except Exception as e:
                errors.append(LogicError(f"Error validating row {i}: {str(e)}", error_type=logging.ERROR))

        return list(set(errors))

    def validate_factor_values(self) -> List[LogicError]:
        """
        Validate factor values in the SDRF DataFrame.

        Returns:
            List of validation errors
        """
        # For now, just return an empty list
        # This can be implemented later
        return []

    def validate_experimental_design(self) -> List[LogicError]:
        """
        Validate experimental design in the SDRF DataFrame.

        Returns:
            List of validation errors
        """
        # For now, just return an empty list
        # This can be implemented later
        return []

    def row_to_record(self, row: pd.Series, template: str):
        """
        Convert a row to a record.

        Args:
            row: Pandas Series containing the row data
            template: Template name to determine the record type

        Returns:
            Record instance
        """
        # Get the model for the template
        model = schema_loader.get_model(template)

        # Convert row to dict
        row_dict = row.to_dict()

        # Create record from dict
        record = model.from_dict(row_dict)

        return record

    def to_records(self, template: str) -> List[Dict[str, Any]]:
        """Convert the SDRF DataFrame to a list of records.

        Args:
            template: Template name to determine the record type

        Returns:
            List of records
        """
        records = []
        for _, row in self.df.iterrows():
            print(f"Processing row: {row}")
            record = self.row_to_record(row, template)
            print(f"Created record: {record}")
            records.append(record.to_dict())
        return records
