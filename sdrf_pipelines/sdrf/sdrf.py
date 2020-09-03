import pandas as pd
import logging

from sdrf_pipelines.sdrf.sdrf_schema import human_schema, HUMAN_TEMPLATE, VERTEBRATES_TEMPLATE, \
    vertebrates_chema, NON_VERTEBRATES_TEMPLATE, nonvertebrates_chema, PLANTS_TEMPLATE, plants_chema, \
    CELL_LINES_TEMPLATE, cell_lines_schema, default_schema, MASS_SPECTROMETRY, mass_spectrometry_schema


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

        df = pd.read_csv(sdrf_file, sep='\t', skip_blank_lines=False)
        nrows = df.shape[0]
        df = df.dropna(axis='index', how='all')
        if df.shape[0] < nrows:
            logging.warning('There were empty lines.')
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
