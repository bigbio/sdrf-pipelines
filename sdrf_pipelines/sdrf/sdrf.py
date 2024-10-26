from __future__ import annotations

import logging
from typing import List

import pandas as pd

from sdrf_pipelines.sdrf.sdrf_schema import CELL_LINES_TEMPLATE
from sdrf_pipelines.sdrf.sdrf_schema import HUMAN_TEMPLATE
from sdrf_pipelines.sdrf.sdrf_schema import MASS_SPECTROMETRY
from sdrf_pipelines.sdrf.sdrf_schema import NON_VERTEBRATES_TEMPLATE
from sdrf_pipelines.sdrf.sdrf_schema import PLANTS_TEMPLATE
from sdrf_pipelines.sdrf.sdrf_schema import VERTEBRATES_TEMPLATE
from sdrf_pipelines.sdrf.sdrf_schema import cell_lines_schema
from sdrf_pipelines.sdrf.sdrf_schema import default_schema
from sdrf_pipelines.sdrf.sdrf_schema import human_schema
from sdrf_pipelines.sdrf.sdrf_schema import mass_spectrometry_schema
from sdrf_pipelines.sdrf.sdrf_schema import nonvertebrates_chema
from sdrf_pipelines.sdrf.sdrf_schema import plants_chema
from sdrf_pipelines.sdrf.sdrf_schema import vertebrates_chema
from sdrf_pipelines.utils.exceptions import LogicError


def check_if_integer(x):
    """
    Check if value x from panda cell can be converted to an integer.
    :param x: value to check
    :return: True if x can be converted to an integer, False otherwise
    """
    try:
        int(x)
        return True
    except ValueError:
        return False


class SdrfDataFrame(pd.DataFrame):
    @property
    def _constructor(self):
        """
        This method is making it so our methods return an instance
        :return:
        """
        return SdrfDataFrame

    def get_sdrf_columns(self):
        """
        This method returns the name of the columns of the SDRF.
        :return:
        """
        return self.columns

    @staticmethod
    def parse(sdrf_file: str):
        """
        Read an SDRF into a dataframe
        :param sdrf_file:
        :return:
        """

        df = pd.read_csv(sdrf_file, sep="\t", skip_blank_lines=False)
        nrows = df.shape[0]
        df = df.dropna(axis="index", how="all")
        if df.shape[0] < nrows:
            logging.warning("There were empty lines.")
        # Convert all columns and values in the dataframe to lowercase
        df = df.astype(str).apply(lambda x: x.str.lower())
        df.columns = map(str.lower, df.columns)

        return SdrfDataFrame(df)

    def validate(self, template: str, use_ols_cache_only: bool = False) -> List[LogicError]:
        """
        Validate a corresponding SDRF
        :return:
        """
        errors = []
        if template != MASS_SPECTROMETRY:
            errors = default_schema.validate(self, use_ols_cache_only=use_ols_cache_only)

        if template == HUMAN_TEMPLATE:
            errors = errors + human_schema.validate(self, use_ols_cache_only=use_ols_cache_only)
        elif template == VERTEBRATES_TEMPLATE:
            errors = errors + vertebrates_chema.validate(self, use_ols_cache_only=use_ols_cache_only)
        elif template == NON_VERTEBRATES_TEMPLATE:
            errors = errors + nonvertebrates_chema.validate(self, use_ols_cache_only=use_ols_cache_only)
        elif template == PLANTS_TEMPLATE:
            errors = errors + plants_chema.validate(self, use_ols_cache_only=use_ols_cache_only)
        elif template == CELL_LINES_TEMPLATE:
            errors = errors + cell_lines_schema.validate(self, use_ols_cache_only=use_ols_cache_only)
        elif template == MASS_SPECTROMETRY:
            errors = mass_spectrometry_schema.validate(self, use_ols_cache_only=use_ols_cache_only)

        return errors

    def validate_factor_values(self) -> List[LogicError]:
        """
        Validate that factor values are present in the SDRF columns.

        :return: A list of LogicError objects if any factor value columns are missing, otherwise an empty list.
        """
        errors = []
        # Check if any column starts with 'factor value' (case-insensitive)
        fv_values = [col for col in self.columns if col.lower().startswith("factor value")]

        if len(fv_values) == 0:
            error_message = f"No factor values present in the following SDRF columns: {self.columns}"
            errors.append(LogicError(error_message, error_type=logging.ERROR))

        # find the corresponding columns for the factor values
        fv_dc = {}
        for fv in fv_values:
            factor = fv.lower().replace("factor value[", "").replace("]", "")
            cols = [col for col in self.columns if (factor in col.lower() and "factor value" not in col.lower())]
            if len(cols) == 0:
                error_message = f"Make sure your SDRF have a sample characteristics or data comment '{factor}' for your factor value column '{fv}'"
                errors.append(LogicError(error_message, error_type=logging.ERROR))
            elif len(cols) > 1:
                error_message = f"Multiple columns found for factor '{factor}': {cols}"
                errors.append(LogicError(error_message, error_type=logging.ERROR))
            else:
                fv_dc[fv] = cols[0]

        for factor, col in fv_dc.items():
            equals_cols = self[factor].equals(self[col])
            if not equals_cols:
                # if factor value contains different values from corresponding columns, print the values
                different_values = self[factor][self[factor] != self[col]]
                different_values = different_values.index.tolist()
                error_message = f"Factor '{factor}' and column '{col}' do not have the same values for the following rows: {different_values}"
                errors.append(LogicError(error_message, error_type=logging.ERROR))

        return errors

    def validate_experimental_design(self) -> List[LogicError]:
        """
        Validate that the experimental design is correct. This method checks that the experimental design is correct,
        including the following:
        - A raw file can only have one associated assay name. If a raw file has more than one assay name, an error is
          raised.
        :return: A list of LogicError objects if the experimental design is incorrect, otherwise an empty list.
        """

        errors = []

        # Check that combination of value assay name and characteristics[data file] is unique in self
        errors = self.check_inconsistencies_assay_file(errors)

        errors = self.check_unique_sample_file_combinations(errors)

        errors = self.check_accessions_conventions(errors)

        return errors

    def check_inconsistencies_assay_file(self, errors: List[LogicError]) -> List[LogicError]:
        """
        Check that combination of values assay name and comment[data file] is unique in self.
        :return: A list of LogicError objects if the combination of values assay name and characteristics[data file] is
        not unique, otherwise an empty list.
        """

        # Group by col1 and check if each group has only one unique col2 value
        col1_inconsistencies = self.groupby("assay name")["comment[data file]"].nunique()
        col1_inconsistent_groups = col1_inconsistencies[col1_inconsistencies > 1]
        if len(col1_inconsistent_groups) > 0:
            cell_index = col1_inconsistent_groups.index.tolist()
            error_message = f"Multiple assays with the same raw files: {cell_index}, the combination assay name and comment[data file] should be unique"
            errors.append(LogicError(error_message, error_type=logging.ERROR))

        # Group by col2 and check if each group has only one unique col1 value
        col2_inconsistencies = self.groupby("comment[data file]")["assay name"].nunique()
        col2_inconsistent_groups = col2_inconsistencies[col2_inconsistencies > 1]
        if len(col2_inconsistent_groups) > 0:
            cell_index = col2_inconsistent_groups.index.tolist()
            error_message = f"Multiple raw files with the same assay: {cell_index}, the combination assay name and comment[data file] should be unique"
            errors.append(LogicError(error_message, error_type=logging.ERROR))

        return errors

    def check_unique_sample_file_combinations(self, errors: List[LogicError]) -> List[LogicError]:
        """
        The combination of the following columns should be unique:
        - source name
        - comment[technical replicate]
        - comment[biological replicate]
        - comment[label]
        - comment[fraction identifier]
        :return: A list of LogicError objects if the source names are not unique, otherwise an empty list.
        """
        cols = [
            "source name",
            "comment[technical replicate]",
            "characteristics[biological replicate]",
            "comment[label]",
            "comment[fraction identifier]",
        ]

        for col in cols:
            if col not in self.columns:
                error_message = (
                    f"In order to perform experimental design validation, column '{col}' must be present in the SDRF"
                )
                errors.append(LogicError(error_message, error_type=logging.ERROR))

        colum_present = all(col in self.columns for col in cols)
        if not colum_present:
            return errors

        duplicates = self.duplicated(subset=cols, keep=False)
        if duplicates.any():
            error_message = f"Duplicate samples found in the SDRF for the combinations of the following columns: {cols}"
            errors.append(LogicError(error_message, error_type=logging.ERROR))

        return errors

    def check_accessions_conventions(self, errors):
        """
        Check that the accessions in the SDRF follow the conventions for the different templates.
        :return: A list of LogicError objects if the accessions do not follow the conventions, otherwise an empty list.
        """
        errors = []

        def check_integer_columns(df, columns):
            """
            This method checks that all the values in the given columns are integers. Retrieve a dictionary with the
            columns as keys and the list of row indexes that do not contain integer as values.
            :param df: The dataframe to check
            :param columns: The columns to check
            :return: A dataframe containing the rows that do not contain only integers in the specified columns
            """

            non_integer_rows = {}
            for column in columns:
                # Check if the column contains only integers
                non_integers = df[~df[column].apply(check_if_integer)].index.tolist()
                if non_integers:
                    non_integer_rows[column] = non_integers
            return non_integer_rows

        # Specify the columns to check
        columns_to_check = [
            "comment[technical replicate]",
            "characteristics[biological replicate]",
            "comment[fraction identifier]",
        ]

        ## Remove columns that are not present in the dataframe
        columns_to_check = [col for col in columns_to_check if col in self.columns]

        # Find rows that do not contain only integers in the specified columns
        non_integer_rows = check_integer_columns(self, columns_to_check)

        if len(non_integer_rows) > 0:
            errors.append(
                LogicError(
                    f"Non-integer values found in the following columns and rows: {non_integer_rows}",
                    error_type=logging.WARNING,
                )
            )

        def check_all_integers_higher_than_one(df, columns):
            """
            This method check that all the values in the columns (if they are numbers) are higher than 0.
            :param df: The dataframe to check
            :param columns: The columns to check
            :return: A dataframe containing the rows that do not contain only integers in the specified columns
            """
            non_integer_rows = {}
            for column in columns:
                # Check if the column contains only integers
                non_integers = df[~df[column].apply(lambda x: check_if_integer(x) and int(x) > 0)].index.tolist()
                if non_integers:
                    non_integer_rows[column] = non_integers
            return non_integer_rows

        lower_than_one = check_all_integers_higher_than_one(self, columns_to_check)
        if len(lower_than_one) > 0:
            errors.append(
                LogicError(
                    f"Values lower than 1 found in the following columns and rows: {lower_than_one}",
                    error_type=logging.WARNING,
                )
            )

        return errors
