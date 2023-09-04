import logging
import re
import typing
from typing import Any

import pandas as pd
from pandas_schema import Column
from pandas_schema import Schema
from pandas_schema.validation import LeadingWhitespaceValidation
from pandas_schema.validation import MatchesPatternValidation
from pandas_schema.validation import TrailingWhitespaceValidation
from pandas_schema.validation import _BaseValidation
from pandas_schema.validation import _SeriesValidation

from sdrf_pipelines.sdrf import sdrf
from sdrf_pipelines.utils.exceptions import LogicError
from sdrf_pipelines.zooma.ols import OlsClient

client = OlsClient()

HUMAN_TEMPLATE = "human"
DEFAULT_TEMPLATE = "default"
VERTEBRATES_TEMPLATE = "vertebrates"
NON_VERTEBRATES_TEMPLATE = "nonvertebrates"
PLANTS_TEMPLATE = "plants"
CELL_LINES_TEMPLATE = "cell_lines"
MASS_SPECTROMETRY = "mass_spectrometry"
ALL_TEMPLATES = [
    DEFAULT_TEMPLATE,
    HUMAN_TEMPLATE,
    VERTEBRATES_TEMPLATE,
    NON_VERTEBRATES_TEMPLATE,
    PLANTS_TEMPLATE,
    CELL_LINES_TEMPLATE,
]

TERM_NAME = "NT"
NOT_AVAILABLE = "not available"
NOT_APPLICABLE = "not applicable"


def check_minimum_columns(panda_sdrf=None, minimun_columns: int = 0):
    return len(panda_sdrf.get_sdrf_columns()) < minimun_columns


def ontology_term_parser(cell_value: str = None):
    """
    Parse a line string and convert it into a dictionary {key -> value}
    :param cell_value: String line
    :return:
    """
    term = {}
    values = cell_value.split(";")
    if len(values) == 1 and "=" not in values[0]:
        term[TERM_NAME] = values[0].lower()
    else:
        for name in values:
            value_terms = name.split("=")
            term[value_terms[0].strip().upper()] = value_terms[1].strip().lower()
    return term


class SDRFColumn(Column):
    def __init__(
        self,
        name: str,
        validations: typing.Iterable["_BaseValidation"] = None,
        optional_validations: typing.Iterable["_BaseValidation"] = None,
        allow_empty=False,
        optional_type=True,
    ):
        if validations is None:
            validations = []
        if optional_validations is None:
            optional_validations = []

        super().__init__(name, validations, allow_empty)
        self.optional_validations = optional_validations
        self._optional = optional_type

    def validate_optional(self, series):
        warnings = []
        for validation in self.optional_validations:
            for error in validation.get_errors(series, self):
                w = LogicError(error.message, error.value, error.row, error.column, error_type=logging.WARN)
                warnings.append(w)
        return warnings


class OntologyTerm(_SeriesValidation):
    """
    Checks that there is no leading whitespace in this column
    """

    def __init__(self, ontology_name: str = None, not_available: bool = False, not_applicable: bool = False, **kwargs):
        super().__init__(**kwargs)
        self._ontology_name = ontology_name
        self._not_available = not_available
        self._not_applicable = not_applicable

    @property
    def default_message(self):
        """
        Return a message for the validation of the Ontology Term
        :return:
        """
        return f"the term name or title can't be found in the ontology -- {self._ontology_name}"

    @staticmethod
    def validate_ontology_terms(cell_value, labels):
        """
        Check if a cell value is in a list of labels or list of strings
        :param cell_value: string line in cell
        :param labels: list of labels
        :return:
        """
        cell_value = cell_value.lower()
        term = ontology_term_parser(cell_value)
        if term.get(TERM_NAME) in labels:
            return True
        return False

    def validate(self, series: pd.Series) -> pd.Series:
        """
        Validate if the term is present in the provided ontology. This method looks in the provided
        ontology _ontology_name
        :param series: return the series that do not match the criteria
        :return:
        """
        terms = [ontology_term_parser(x) for x in series.unique()]
        labels = []
        for term in terms:
            if TERM_NAME not in term:
                ontology_terms = None
            else:
                if self._ontology_name is not None:
                    ontology_terms = client.search(term[TERM_NAME], ontology=self._ontology_name, exact="true")
                else:
                    ontology_terms = client.search(term[TERM_NAME], exact="true")

            if ontology_terms is not None:
                query_labels = [o["label"].lower() for o in ontology_terms]
                for label in query_labels:
                    labels.append(label)
        if self._not_available:
            labels.append(NOT_AVAILABLE)
        if self._not_applicable:
            labels.append(NOT_APPLICABLE)
        return series.apply(lambda cell_value: self.validate_ontology_terms(cell_value, labels))


class SDRFSchema(Schema):
    _special_columns = {"sourcename", "assayname", "materialtype", "technologytype"}
    _column_template = r"^(characteristics|comment|factor value)\s*\[([^\]]+)\](?:\.\d+)?$"

    def __init__(self, columns: typing.Iterable[SDRFColumn], ordered: bool = False, min_columns: int = 0):
        super().__init__(columns, ordered)
        self._min_columns = min_columns

    def __new__(cls, ordered: bool = False, min_columns: int = 0) -> Any:
        obj = super().__new__(cls)
        obj._min_columns = min_columns
        return obj

    def validate(self, panda_sdrf: sdrf = None) -> typing.List[LogicError]:
        errors = []

        # Check minimum number of columns
        if check_minimum_columns(panda_sdrf, self._min_columns):
            error_message = (
                "The number of columns in the SDRF ({}) is smaller than the number of mandatory fields ({})".format(
                    len(panda_sdrf.get_sdrf_columns()), self._min_columns
                )
            )
            errors.append(LogicError(error_message, error_type=logging.WARN))

        # Check the mandatory fields
        error_mandatory = self.validate_mandatory_columns(panda_sdrf)
        if error_mandatory is not None:
            errors.append(error_mandatory)

        # Check the columns order
        error_columns_order = self.validate_columns_order(panda_sdrf)
        if error_columns_order is not None:
            errors.extend(error_columns_order)

        # Check that the term is present in ontology
        error_ontology_terms = self.validate_columns(panda_sdrf)
        if error_ontology_terms is not None:
            for error in error_ontology_terms:
                errors.append(error)

        error_names = self.validate_column_names(panda_sdrf)
        if error_names:
            errors.extend(error_names)

        errors.extend(self.check_recommendations(panda_sdrf))

        return errors

    def validate_column_names(self, panda_sdrf):
        errors = []
        spaces = []
        logerror = []
        for cname in panda_sdrf.columns:
            if cname != cname.strip():
                spaces.append(cname)
                continue
            if cname.replace(" ", "") in self._special_columns:
                continue
            m = re.match(self._column_template, cname)
            if not m:
                errors.append(cname)
            elif m.group().startswith("factor value"):
                if (
                    m.group().replace("factor value", "comment") not in panda_sdrf.columns
                    and m.group().replace("factor value", "characteristics") not in panda_sdrf.columns
                    and m.group() not in panda_sdrf.columns
                ):
                    error_message = "The " + cname + " column should also be in the characteristics or comment"
                    logerror.append(LogicError(error_message, error_type=logging.ERROR))

        if errors + spaces:
            logerror.append(
                LogicError(
                    "Invalid columns present: "
                    + ", ".join(errors)
                    + ", ".join(e + " (leading or trailing whitespace)" for e in spaces),
                    error_type=logging.ERROR,
                )
            )
        return logerror

    def validate_mandatory_columns(self, panda_sdrf):
        error_mandatory = []
        for column in self.columns:
            if column._optional is False and column.name not in panda_sdrf.get_sdrf_columns():
                error_mandatory.append(column.name)
        if len(error_mandatory):
            error_message = "The following columns are mandatory and not present in the SDRF: {}".format(
                ", ".join(error_mandatory)
            )
            return LogicError(error_message, error_type=logging.ERROR)
        return None

    @staticmethod
    def validate_columns_order(panda_sdrf):
        error_columns_order = []
        if "assay name" in list(panda_sdrf):
            cnames = list(panda_sdrf)
            index = cnames.index("assay name")
            factor_tag = False
            for column in cnames:
                if ("comment" in column or "technology type" in column) and cnames.index(column) < index:
                    error_message = "The column " + column + "cannot be before the assay name"
                    error_columns_order.append(LogicError(error_message, error_type=logging.ERROR))
                if (
                    "characteristics" in column or ("material type" in column and "factor value" not in column)
                ) and cnames.index(column) > index:
                    error_message = "The column " + column + "cannot be after the assay name"
                    error_columns_order.append(LogicError(error_message, error_type=logging.ERROR))
                if "factor value" in column and not factor_tag:
                    factor_index = cnames.index(column)
                    factor_tag = True
            if factor_tag:
                temp = []
                error = []
                for column in cnames[factor_index:]:
                    if "comment" in column or "characteristics" in column:
                        error.extend(temp)
                        temp = []
                    elif "factor value" in column:
                        temp.append(column)
                if len(error):
                    error_message = "The following factor column should be last: {}".format(", ".join(error))
                    error_columns_order.append(LogicError(error_message, error_type=logging.ERROR))
            if error_columns_order:
                return error_columns_order
        return None

    def _get_column_pairs(self, panda_sdrf):
        column_pairs = []
        columns_to_pair = self.columns
        errors = []

        for column in columns_to_pair:
            if column.name not in panda_sdrf and column._optional is False:
                message = f"The column {column.name} is not present in the SDRF"
                errors.append(LogicError(message, error_type=logging.ERROR))
            elif column.name in panda_sdrf:
                column_pairs.append((panda_sdrf[column.name], column))
        return column_pairs, errors

    def validate_columns(self, panda_sdrf):
        # Iterate over each pair of schema columns and data frame series and run validations
        column_pairs, errors = self._get_column_pairs(panda_sdrf)
        for series, column in column_pairs:
            errors += column.validate(series)
        return sorted(errors, key=lambda e: e.row)

    def check_recommendations(self, panda_sdrf):
        column_pairs, errors = self._get_column_pairs(panda_sdrf)
        warnings = []
        for series, column in column_pairs:
            warnings += column.validate_optional(series)
        return sorted(warnings, key=lambda e: e.row)


default_schema = SDRFSchema(
    [
        SDRFColumn(
            "source name",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "characteristics[organism part]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "characteristics[disease]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "characteristics[organism]",
            [
                LeadingWhitespaceValidation(),
                TrailingWhitespaceValidation(),
                OntologyTerm("ncbitaxon", not_applicable=True),
            ],
            allow_empty=False,
            optional_type=False,
        ),
        SDRFColumn(
            "characteristics[cell type]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=False,
            optional_type=False,
        ),
        SDRFColumn(
            "characteristics[biological replicate]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "assay name",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=False,
            optional_type=False,
        ),
        SDRFColumn(
            "comment[technical replicate]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "comment[fraction identifier]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "comment[data file]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
    ],
    min_columns=7,
)

human_schema = SDRFSchema(
    [
        SDRFColumn(
            "characteristics[cell type]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "characteristics[ancestry category]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "characteristics[age]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            [
                MatchesPatternValidation(
                    r"(?:^(?:\d+y)?(?:\d+m)?(?:\d+d)?$)|(?:not available)|(?:not applicable)", case=False
                )
            ],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "characteristics[sex]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "characteristics[developmental stage]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=True,
        ),
        SDRFColumn(
            "characteristics[individual]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=True,
        ),
    ],
    min_columns=7,
)

vertebrates_chema = SDRFSchema(
    [
        SDRFColumn(
            "characteristics[developmental stage]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        )
    ],
    min_columns=7,
)

nonvertebrates_chema = SDRFSchema(
    [
        SDRFColumn(
            "characteristics[developmental stage]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=True,
        ),
        SDRFColumn(
            "characteristics[strain/breed]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=True,
        ),
    ],
    min_columns=7,
)

plants_chema = SDRFSchema(
    [
        SDRFColumn(
            "characteristics[developmental stage]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=True,
        ),
        SDRFColumn(
            "characteristics[strain/breed]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=True,
        ),
    ],
    min_columns=7,
)

cell_lines_schema = SDRFSchema(
    [
        SDRFColumn(
            "characteristics[cell type]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "characteristics[cell line]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
    ],
    min_columns=7,
)

mass_spectrometry_schema = SDRFSchema(
    [
        SDRFColumn(
            "assay name",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "technology type",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=True,
        ),
        SDRFColumn(
            "comment[fraction identifier]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "comment[label]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation(), OntologyTerm("pride")],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "comment[technical replicate]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "comment[instrument]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation(), OntologyTerm("ms")],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "comment[modification parameters]",
            [
                LeadingWhitespaceValidation(),
                TrailingWhitespaceValidation(),
                OntologyTerm(ontology_name="unimod", not_available=True),
            ],
            allow_empty=True,
            optional_type=True,
        ),
        SDRFColumn(
            "comment[cleavage agent details]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation(), OntologyTerm("ms", not_applicable=True)],
            allow_empty=True,
            optional_type=False,
        ),
        SDRFColumn(
            "comment[fragment mass tolerance]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=True,
        ),
        SDRFColumn(
            "comment[precursor mass tolerance]",
            [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()],
            allow_empty=True,
            optional_type=True,
        ),
    ],
    min_columns=7,
)
