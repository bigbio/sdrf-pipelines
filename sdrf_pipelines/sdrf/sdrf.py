import logging

import pandas as pd
from sdrf_pipelines.sdrf.sdrf_schema import (CELL_LINES_TEMPLATE,
                                             HUMAN_TEMPLATE, MASS_SPECTROMETRY,
                                             NON_VERTEBRATES_TEMPLATE,
                                             PLANTS_TEMPLATE,
                                             VERTEBRATES_TEMPLATE,
                                             cell_lines_schema, default_schema,
                                             human_schema,
                                             mass_spectrometry_schema,
                                             nonvertebrates_chema,
                                             plants_chema, vertebrates_chema)


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
