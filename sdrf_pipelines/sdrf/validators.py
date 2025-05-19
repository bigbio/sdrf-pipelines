import logging
from typing import Any, Optional, Type, Union

import pandas as pd
from pydantic import BaseModel, Field

from sdrf_pipelines.ols.ols import OlsClient
from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame
from sdrf_pipelines.sdrf.specification import NORM, NOT_APPLICABLE, NOT_AVAILABLE
from sdrf_pipelines.utils.exceptions import LogicError


class SDRFValidator(BaseModel):
    params: dict[str, Any] = Field(default_factory=dict)

    def __init__(self, params: dict[str, Any] | None = None, **data):
        super().__init__(**data)
        if params:
            self.params = params

    def validate(  # type: ignore[override]
        self, value: Union[str, SDRFDataFrame, pd.DataFrame, pd.Series, list[str]], column_name: str | None = None
    ) -> list[LogicError]:
        """
        Validate a value.

        Args:
            value: The value to validate
            column_name: The name of the column being validated (if applicable)

        Returns:
            List of LogicError objects
        """
        raise NotImplementedError("Subclasses must implement this method")


# Global validator registry
_VALIDATOR_REGISTRY: dict[str, Type[SDRFValidator]] = {}


def register_validator(validator_name=None):
    """Register a validator class in the global registry with an explicit type."""

    def decorator(cls):
        nonlocal validator_name

        # If no type is provided, try to get it from the class
        if validator_name is None:
            # Try to access the type attribute
            if hasattr(cls, "validator_name"):
                validator_name = cls.validator_name
            else:
                # Fallback to class name
                validator_name = cls.__name__.lower()

        # Store the class in the registry
        _VALIDATOR_REGISTRY[validator_name] = cls

        return cls

    # Allow usage both as @register_validator and @register_validator("type_name")
    if isinstance(validator_name, type) and issubclass(validator_name, SDRFValidator):
        cls = validator_name
        validator_name = None
        return decorator(cls)

    return decorator


def get_validator(validator_name: str) -> Optional[Type[SDRFValidator]]:
    """Get a validator class by type."""
    return _VALIDATOR_REGISTRY.get(validator_name)


@register_validator(validator_name="trailing_whitespace_validator")
class TrailingWhitespaceValidator(SDRFValidator):

    def validate(  # type: ignore[override]
        self, value: Union[str, pd.DataFrame, pd.Series, list[str]], column_name: str | None = None
    ) -> list[LogicError]:
        """
        This method validates if the provided value contains a TrailingWhiteSpace and return it
        as LogicError. If the column, row information is present, it returns the other.

        Parameters:
            value: The value to validate
            column_name: The name of the column being validated (if applicable)
        """
        errors = []

        if isinstance(value, str):
            if value and value.rstrip() != value:
                errors.append(
                    LogicError(
                        message="Trailing whitespace detected",
                        value=value,
                        error_type=logging.ERROR,
                    )
                )

        elif isinstance(value, pd.DataFrame):
            for col in value.columns:
                if value[col].dtype == object:  # Check string columns
                    for idx, cell_value in enumerate(value[col]):
                        if isinstance(cell_value, str) and cell_value and cell_value.rstrip() != cell_value:
                            errors.append(
                                LogicError(
                                    message="Trailing whitespace detected",
                                    value=cell_value,
                                    row=idx,
                                    column=col,
                                    error_type=logging.ERROR,
                                )
                            )
        elif isinstance(value, SDRFDataFrame):
            original_columns = value.get_original_columns()
            for col in original_columns:
                if col.rstrip() != col:
                    errors.append(
                        LogicError(
                            message="Trailing whitespace detected in column name",
                            value=col,
                            error_type=logging.ERROR,
                        )
                    )
            for col in value.columns:
                if value[col].dtype == object:  # Check string columns
                    for idx, cell_value in enumerate(value[col]):
                        if isinstance(cell_value, str) and cell_value and cell_value.rstrip() != cell_value:
                            errors.append(
                                LogicError(
                                    message="Trailing whitespace detected",
                                    value=cell_value,
                                    row=idx,
                                    column=col,
                                    error_type=logging.ERROR,
                                )
                            )
        elif isinstance(value, pd.Series):
            if value.dtype == object:  # Check if Series contains strings
                for idx, cell_value in enumerate(value):
                    if isinstance(cell_value, str) and cell_value and cell_value.rstrip() != cell_value:
                        errors.append(
                            LogicError(
                                message="Trailing whitespace detected",
                                value=cell_value,
                                row=idx,
                                error_type=logging.ERROR,
                            )
                        )

        elif isinstance(value, list):
            for idx, item in enumerate(value):
                if isinstance(item, str) and item and item.rstrip() != item:
                    errors.append(
                        LogicError(
                            message="Trailing whitespace detected",
                            value=item,
                            row=idx,
                            error_type=logging.ERROR,
                        )
                    )

        return errors


@register_validator(validator_name="min_columns")
class MinimumColumns(SDRFValidator):
    minimum_columns: int = 12

    def __init__(self, params: dict[str, Any] | None = None, **data: Any):
        super().__init__(**data)

        if params:
            for key, value in params.items():
                if key == "min_columns":
                    self.minimum_columns = int(value)

    def validate(  # type: ignore[override]
        self, value: Union[SDRFDataFrame, pd.DataFrame], column_name: str | None = None
    ) -> list[LogicError]:
        errors = []
        if len(value.columns) < self.minimum_columns:
            errors.append(
                LogicError(
                    message=f"The number of columns is lower than the mandatory number {self.minimum_columns}",
                    error_type=logging.ERROR,
                )
            )
        return errors


@register_validator(validator_name="ontology")
class OntologyValidator(SDRFValidator):
    client: OlsClient = OlsClient()
    term_name: str = "NT"
    ontologies: list[str] = []
    error_level: int = logging.INFO
    use_ols_cache_only: bool = False
    model_config = {"arbitrary_types_allowed": True}

    def __init__(self, params: dict[str, Any] | None = None, **data: Any):
        super().__init__(**data)
        logging.info(params)
        if params:
            for key, value in params.items():
                if key == "ontologies":
                    self.ontologies = value
                if key == "error_level":
                    if value == "warning":
                        self.error_level = logging.WARNING
                    elif value == "error":
                        self.error_level = logging.ERROR
                    else:
                        self.error_level = logging.INFO
                if key == "use_ols_cache_only":
                    self.use_ols_cache_only = value

    def validate(self, series: pd.Series, column_name: str | None = None) -> list[LogicError]:  # type: ignore[override]
        """
        Validate if the term is present in the provided ontology. This method looks in the provided
        ontology _ontology_name

        Parameters:
            series: The pandas Series to validate
            column_name: The name of the column being validated

        Returns:
            List of LogicError for values that don't match the ontology terms
        """
        terms = [self.ontology_term_parser(x) for x in series.unique()]
        labels = []
        for term in terms:
            if self.term_name not in term:
                ontology_terms = None
            else:
                if self.ontologies is not None:
                    ontology_terms = []
                    for ontology_name in self.ontologies:
                        ontology_terms.extend(
                            self.client.search(
                                term=term[self.term_name],
                                ontology=ontology_name,
                                exact=True,
                                use_ols_cache_only=self.use_ols_cache_only,
                            )
                        )
                else:
                    ontology_terms = self.client.search(
                        term=term[self.term_name], exact=True, use_ols_cache_only=self.use_ols_cache_only
                    )

            if ontology_terms is not None:
                query_labels = [o["label"].lower() for o in ontology_terms if "label" in o]
                if term[self.term_name] in query_labels:
                    labels.append(term[self.term_name])
        labels.append(NOT_AVAILABLE)
        labels.append(NOT_APPLICABLE)  # We have to double-check that the column allows this.
        labels.append(NORM)
        validation_indexes = series.apply(lambda cell_value: self.validate_ontology_terms(cell_value, labels))

        # Convert to indexes of the row to LogicErrors
        errors = []
        for idx, value in enumerate(validation_indexes):
            if not value:
                column_info = f" in column '{column_name}'" if column_name else ""
                errors.append(
                    LogicError(
                        message=(
                            f"Term: {series[idx]}{column_info}, is not found in the "
                            f"given ontology list {';'.join(self.ontologies)}"
                        ),
                        row=idx,
                        column=column_name,
                        error_type=self.error_level,
                    )
                )
        return errors

    def validate_ontology_terms(self, cell_value, labels):
        """
        Check if a cell value is in a list of labels or list of string
        :param cell_value: line in a cell
        :param labels: list of labels
        :return:
        """
        cell_value = cell_value.lower()
        term = self.ontology_term_parser(cell_value)
        if term.get(self.term_name) in labels:
            return True
        return False

    def ontology_term_parser(self, cell_value: str):
        """
        Parse a line string and convert it into a dictionary {key -> value}
        :param cell_value: String line
        :return:
        """
        term = {}
        values = cell_value.split(";")
        if len(values) == 1 and "=" not in values[0]:
            term[self.term_name] = values[0].lower()
        else:
            for name in values:
                value_terms = name.split("=", 1)
                if len(value_terms) == 1:
                    raise ValueError("Not a key-value pair: " + name)
                if "=" in value_terms[1] and value_terms[0].lower() != "cs":
                    raise ValueError(
                        f"Invalid term: {name} after splitting by '=', please check the prefix (e.g. AC, NT, " f"TA..)"
                    )
                term[value_terms[0].strip().upper()] = value_terms[1].strip().lower()

        return term


@register_validator(validator_name="pattern")
class PatternValidator(SDRFValidator):
    """Validator that checks if values match a regular expression pattern."""

    def validate(self, series: pd.Series, column_name: str) -> list[LogicError]:  # type: ignore[override]
        """
        Validate if values in the series match the specified regex pattern.

        Parameters:
            series: The pandas Series to validate
            column_name: The name of the column being validated

        Returns:
            List of LogicError for values that don't match the pattern
        """

        pattern = self.params["pattern"]
        case = self.params.get("case_sensitive", False)

        series = series.astype(str)
        not_matched = series[~series.str.match(pat=pattern, case=case)].reset_index(drop=True)
        errors = []

        for idx, value in enumerate(not_matched.values, start=1):
            column_info = f" in column '{column_name}'"
            errors.append(
                LogicError(
                    message=f"Value '{value}'{column_info} does not match the required pattern: {pattern}",
                    row=idx,
                    column=column_name,
                    error_type=logging.ERROR,
                )
            )

        return errors


@register_validator(validator_name="column_order")
class ColumnOrderValidator(SDRFValidator):
    """Validator that checks if columns are in the correct order in the SDRF file."""

    def validate(  # type: ignore[override]
        self, df: Union[SDRFDataFrame, pd.DataFrame], column_name: str | None = None
    ) -> list[LogicError]:
        """
        Validate if columns are in the correct order in the SDRF file.

        Parameters:
            df: The pandas DataFrame to validate
            column_name: Not used for this validator as it operates on the entire DataFrame

        Returns:
            List of LogicError for column order issues
        """
        error_columns_order = []

        if "assay name" in list(df):
            cnames = list(df)
            assay_index = cnames.index("assay name")

            # Check that all characteristics columns are before assay name
            characteristics_after_assay = [col for col in cnames[assay_index:] if "characteristics" in col]
            if characteristics_after_assay:
                error_message = f"All characteristics columns must be before 'assay name'. Found after: {', '.join(characteristics_after_assay)}"
                error_columns_order.append(LogicError(message=error_message, error_type=logging.ERROR))
            factor_tag = False

            for idx, column in enumerate(cnames):
                error_message, error_type = "", None

                if idx < assay_index:
                    if "comment" in column:
                        error_message = f"The column '{column}' cannot be before the assay name"
                        error_type = logging.ERROR
                    if "technology type" in column:
                        error_message = f"The column '{column}' must be immediately after the assay name"
                        if assay_index - idx > 1:
                            error_type = logging.ERROR
                        else:
                            error_type = logging.WARNING
                else:
                    if "characteristics" in column or ("material type" in column and "factor value" not in column):
                        error_message = f"The column '{column}' cannot be after the assay name"
                        error_type = logging.ERROR
                    if "technology type" in column and idx > assay_index + 1:
                        error_message = f"The column '{column}' must be immediately after the assay name"
                        error_type = logging.ERROR

                if error_type is not None:
                    error_columns_order.append(LogicError(message=error_message, error_type=error_type))

                if "factor value" in column and not factor_tag:
                    factor_index = idx
                    factor_tag = True

            if factor_tag:
                temp: list[str] = []
                error = []
                for column in cnames[factor_index:]:
                    if "comment" in column or "characteristics" in column:
                        error.extend(temp)
                        temp = []
                    elif "factor value" in column:
                        temp.append(column)

                if len(error):
                    error_message = f"The following factor column should be last: {', '.join(error)}"
                    error_columns_order.append(LogicError(message=error_message, error_type=logging.ERROR))

        return error_columns_order


@register_validator(validator_name="empty_cells")
class EmptyCellValidator(SDRFValidator):
    """Validator that checks for empty cells in the SDRF file."""

    def validate(  # type: ignore[override]
        self, df: Union[pd.DataFrame, SDRFDataFrame], column_name: str | None = None
    ) -> list[LogicError]:
        """
        Check for empty cells in the SDRF. This method will return a list of errors if any empty cell is found.

        Parameters:
            df: The pandas DataFrame to validate
            column_name: Not used for this validator as it operates on the entire DataFrame

        Returns:
            List of LogicError for empty cells
        """
        errors = []

        def validate_string(cell_value):
            if pd.isna(cell_value):
                return False
            if not isinstance(cell_value, str):
                cell_value = str(cell_value)
            return cell_value != "nan" and len(cell_value.strip()) > 0

        validation_results = df.map(validate_string)

        # Get the indices where the validation fails
        failed_indices = [
            (row, col)
            for row in validation_results.index
            for col in validation_results.columns
            if not validation_results.at[row, col]
        ]

        for row, col in failed_indices:
            message = f"Empty value found Row: {row}, Column: {col}"
            errors.append(LogicError(message=message, error_type=logging.ERROR))

        return errors
