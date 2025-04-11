from pathlib import Path
from typing import Union, Any, List

import pandas as pd
from pydantic import BaseModel, Field


class SDRFDataFrame(BaseModel):
    sdrf_columns: List[str] = Field(default_factory=list)
    df: pd.DataFrame = Field(default=None)
    model_config = {"arbitrary_types_allowed": True}

    def __init__(self, sdrf_file: Union[pd.DataFrame, str, Path], /, **data: Any):
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

    def parse(self, sdrf_file: Union[str, Path]) -> pd.DataFrame:
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

    def get_dataframe_columns(self) -> List[str]:
        """
        Get the column names of the SDRF DataFrame.

        Returns:
            List of column names
        """
        return self.df.columns.tolist()

    def get_original_columns(self) -> List[str]:
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

    # def validate(self, template: str, use_ols_cache_only: bool = False) -> List[LogicError]:
    #     """
    #     Validate the SDRF DataFrame.
    #
    #     Args:
    #         template: Template name to determine the validation rules
    #         use_ols_cache_only: Whether to use only the cache for ontology validation
    #
    #     Returns:
    #         List of validation errors
    #     """
    #     errors = []
    #
    #     # Check the minimum number of columns
    #     try:
    #         schema = schema_loader.get_schema(template)
    #         min_columns = schema.min_columns
    #         if len(self.get_sdrf_columns()) < min_columns:
    #             error_message = (
    #                 f"The number of columns in the SDRF ({len(self.get_sdrf_columns())}) "
    #                 f"is smaller than the number of mandatory fields ({min_columns})"
    #             )
    #             errors.append(LogicError(error_message, error_type=logging.WARN))
    #     except ValueError:
    #         # If the schema is not found, skip the min_columns check
    #         pass
    #
    #     # Validate each row
    #     for i, row in self.sdrf_file.iterrows():
    #         try:
    #             # Convert row to record
    #             record = self.row_to_record(row, template)
    #
    #             # Validate record
    #             record_errors = record.validate_record(use_ols_cache_only=use_ols_cache_only)
    #             errors.extend(record_errors)
    #         except pydantic.ValidationError as e:
    #             for error in e.errors():
    #                 if error["name"] == "missing" and schema is not None:
    #                     field_name = error["loc"][0]
    #                     sdrf_name_field = next((field for field in schema.fields if field.name == field_name), None)
    #                     if sdrf_name_field is not None:
    #                         errors.append(
    #                             LogicError(
    #                                 f"The following field, {sdrf_name_field.description} is required, use the column {sdrf_name_field.sdrf_name}",
    #                                 error_type=logging.ERROR,
    #                             )
    #                         )
    #                 else:
    #                     errors.append(LogicError(f"Error validating row {i}: {str(e)}", error_type=logging.ERROR))
    #         except Exception as e:
    #             errors.append(LogicError(f"Error validating row {i}: {str(e)}", error_type=logging.ERROR))
    #
    #     return list(set(errors))