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
from typing import List


class SdrfDataFrame(pd.DataFrame):
    @property
    def _constructor(self):
        """
        This method is makes it so our methods return an instance
        :return:
        """
        return SdrfDataFrame

    def get_sdrf_columns(self):
        """
        This method return the name of the columns of the SDRF.
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

    def validate(self, template: str):
        """
        Validate a corresponding SDRF
        :return:
        """
        errors = []
        if template != MASS_SPECTROMETRY:
            errors = default_schema.validate(self)

        if template == HUMAN_TEMPLATE:
            errors = errors + human_schema.validate(self)
        elif template == VERTEBRATES_TEMPLATE:
            errors = errors + vertebrates_chema.validate(self)
        elif template == NON_VERTEBRATES_TEMPLATE:
            errors = errors + nonvertebrates_chema.validate(self)
        elif template == PLANTS_TEMPLATE:
            errors = errors + plants_chema.validate(self)
        elif template == CELL_LINES_TEMPLATE:
            errors = errors + cell_lines_schema.validate(self)
        elif template == MASS_SPECTROMETRY:
            errors = mass_spectrometry_schema.validate(self)

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
