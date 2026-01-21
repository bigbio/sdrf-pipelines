import logging
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field

from sdrf_pipelines.ols.ols import OLS_AVAILABLE

if OLS_AVAILABLE:
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
        self, value: str | SDRFDataFrame | pd.DataFrame | pd.Series | list[str], column_name: str | None = None
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
_VALIDATOR_REGISTRY: dict[str, type[SDRFValidator]] = {}


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


def get_validator(validator_name: str) -> type[SDRFValidator] | None:
    """Get a validator class by type."""
    return _VALIDATOR_REGISTRY.get(validator_name)


@register_validator(validator_name="unique_values_validator")
class UniqueValuesValidator(SDRFValidator):
    def validate(self, series: pd.Series, column_name: str | None = None) -> list[LogicError]:  # type: ignore[override]
        """
        Validate if values in the series are unique.

        Parameters:
            series: The pandas Series to validate
            column_name: The name of the column being validated

        Returns:
            List of LogicError for duplicate values
        """
        duplicates = series[series.duplicated(keep=False)].unique()
        errors = []

        for value in duplicates:
            duplicate_indices = series[series == value].index.tolist()
            row_info = ", ".join(str(idx) for idx in duplicate_indices)
            column_info = f" in column '{column_name}'" if column_name else ""
            errors.append(
                LogicError(
                    message=f"Value '{value}'{column_info} is duplicated at rows: {row_info}",
                    row=-1,
                    column=column_name,
                    error_type=logging.ERROR,
                )
            )

        return errors


@register_validator(validator_name="single_cardinality_validator")
class SingleCardinalityValidator(SDRFValidator):
    def validate(self, series: pd.Series, column_name: str | None = None) -> list[LogicError]:  # type: ignore[override]
        """
        Validate if the series has single cardinality (i.e., all values are the same).
        Parameters:
            series: The pandas Series to validate
            column_name: The name of the column being validated
        """

        unique_values = series.dropna().unique()
        errors = []

        if len(unique_values) > 1:
            column_info = f" in column '{column_name}'" if column_name else ""
            errors.append(
                LogicError(
                    message=f"Column{column_info} has multiple unique values: {', '.join(map(str, unique_values))}",
                    row=-1,
                    column=column_name,
                    error_type=logging.ERROR,
                )
            )

        return errors


@register_validator(validator_name="trailing_whitespace_validator")
class TrailingWhitespaceValidator(SDRFValidator):
    def _has_trailing_whitespace(self, text: str) -> bool:
        return isinstance(text, str) and bool(text) and text.rstrip() != text

    def _create_trailing_whitespace_error(
        self,
        value: str,
        row: int | None = None,
        column: str | None = None,
        message: str = "Trailing whitespace detected",
    ) -> LogicError:
        return LogicError(
            message=message,
            value=value,
            row=row if row is not None else -1,
            column=column,
            error_type=logging.ERROR,
        )

    def _validate_string(self, value: str) -> list[LogicError]:
        if self._has_trailing_whitespace(value):
            return [self._create_trailing_whitespace_error(value)]
        return []

    def _validate_dataframe(self, value: pd.DataFrame) -> list[LogicError]:
        errors = []
        for col in value.select_dtypes("object").columns:
            for idx, cell_value in enumerate(value[col]):
                if self._has_trailing_whitespace(cell_value):
                    errors.append(self._create_trailing_whitespace_error(cell_value, row=idx, column=col))
        return errors

    def _validate_sdrf_dataframe(self, value: SDRFDataFrame) -> list[LogicError]:
        errors = []

        # Check column names
        original_columns = value.get_original_columns()
        for col in original_columns:
            if col.rstrip() != col:
                errors.append(
                    self._create_trailing_whitespace_error(col, message="Trailing whitespace detected in column name")
                )

        # Check cell values
        for col in value.columns:
            if value[col].dtype == object:
                for idx, cell_value in enumerate(value[col]):
                    if self._has_trailing_whitespace(cell_value):
                        errors.append(self._create_trailing_whitespace_error(cell_value, row=idx, column=col))

        return errors

    def _validate_series(self, value: pd.Series) -> list[LogicError]:
        errors = []
        if value.dtype == object:
            for idx, cell_value in enumerate(value):
                if self._has_trailing_whitespace(cell_value):
                    errors.append(self._create_trailing_whitespace_error(cell_value, row=idx))
        return errors

    def _validate_list(self, value: list[str]) -> list[LogicError]:
        errors = []
        for idx, item in enumerate(value):
            if self._has_trailing_whitespace(item):
                errors.append(self._create_trailing_whitespace_error(item, row=idx))
        return errors

    def validate(  # type: ignore[override]
        self, value: str | pd.DataFrame | pd.Series | list[str], column_name: str | None = None
    ) -> list[LogicError]:
        """
        This method validates if the provided value contains a TrailingWhiteSpace and return it
        as LogicError. If the column, row information is present, it returns the other.

        Parameters:
            value: The value to validate
            column_name: The name of the column being validated (if applicable)
        """
        match value:
            case str():
                return self._validate_string(value)
            case pd.DataFrame():
                return self._validate_dataframe(value)
            case SDRFDataFrame():
                return self._validate_sdrf_dataframe(value)
            case pd.Series():
                return self._validate_series(value)
            case list():
                return self._validate_list(value)
            case _:
                return []


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
        self, value: SDRFDataFrame | pd.DataFrame, column_name: str | None = None
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


# Only register OntologyValidator if OLS dependencies are available
if OLS_AVAILABLE:

    @register_validator(validator_name="ontology")
    class OntologyValidator(SDRFValidator):
        client: OlsClient = OlsClient()
        term_name: str = "NT"
        ontologies: list[str] = Field(default_factory=list)
        error_level: int = logging.INFO
        use_ols_cache_only: bool = False
        model_config = {"arbitrary_types_allowed": True}

        def __init__(self, params: dict[str, Any] | None = None, **data: Any):
            super().__init__(**data)
            logging.debug(params)
            if params:
                for key, value in params.items():
                    if key == "ontologies":
                        self.ontologies = value
                    elif key == "error_level":
                        if value == "warning":
                            self.error_level = logging.WARNING
                        elif value == "error":
                            self.error_level = logging.ERROR
                        else:
                            self.error_level = logging.INFO
                    elif key == "use_ols_cache_only":
                        self.use_ols_cache_only = value

        def validate(self, value: pd.Series, column_name: str | None = None) -> list[LogicError]:  # type: ignore[override]
            """
            Validate if the term is present in the provided ontology. This method looks in the provided
            ontology _ontology_name

            Parameters:
                value: The pandas Series to validate
                column_name: The name of the column being validated

            Returns:
                List of LogicError for values that don't match the ontology terms
            """
            logger = logging.getLogger(__name__)

            def _validate_cell(cell_value, labels):
                try:
                    return self.validate_ontology_terms(cell_value, labels)
                except (ValueError, KeyError, AttributeError):
                    # Expected validation exceptions - return False
                    return False
                except Exception as e:
                    # Unexpected exception - log and re-raise
                    logger.error(f"Unexpected error validating cell value '{cell_value}': {type(e).__name__}: {e}")
                    raise

            errors = []
            terms = []
            for x in value.unique():
                # Skip empty values - they are handled by empty_cells validator
                if not x or str(x).strip() == "":
                    continue
                try:
                    term = self.ontology_term_parser(x)
                    terms.append(term)
                except ValueError as e:
                    column_info = f" in column '{column_name}'" if column_name else ""
                    errors.append(
                        LogicError(
                            message=f"Term: {x}{column_info}, is not a valid ontology term. Error: {str(e)}",
                            row=-1,
                            column=column_name,
                            error_type=logging.ERROR,
                        )
                    )
                    continue

            labels = []
            for term in terms:
                if self.term_name not in term:
                    ontology_terms = None
                else:
                    if self.ontologies:
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

            validation_indexes = value.apply(lambda cell_value: _validate_cell(cell_value, labels))

            # Convert to indexes of the row to LogicErrors
            for idx, val in enumerate(validation_indexes):
                if not val:
                    cell_value = value.iloc[idx]
                    # Skip empty values - they are handled by empty_cells validator
                    if not cell_value or str(cell_value).strip() == "":
                        continue
                    column_info = f" in column '{column_name}'" if column_name else ""
                    errors.append(
                        LogicError(
                            message=(
                                f"Term: {cell_value}{column_info}, is not found in the "
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
                            f"Invalid term: {name} after splitting by '=', please check the prefix (e.g. AC, NT, TA..)"
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

    def _check_characteristics_before_assay(self, cnames: list[str], assay_index: int) -> list[LogicError]:
        errors = []
        characteristics_after_assay = [col for col in cnames[assay_index:] if "characteristics" in col]
        if characteristics_after_assay:
            error_message = (
                f"All characteristics columns must be before 'assay name'. "
                f"Found after: {', '.join(characteristics_after_assay)}"
            )
            errors.append(LogicError(message=error_message, error_type=logging.ERROR))
        return errors

    def _validate_column_before_assay(self, column: str, idx: int, assay_index: int) -> tuple[str, int | None]:
        error_message, error_type = "", None

        if "comment" in column:
            error_message = f"The column '{column}' cannot be before the assay name"
            error_type = logging.ERROR
        elif "technology type" in column:
            error_message = f"The column '{column}' must be immediately after the assay name"
            if assay_index - idx > 1:
                error_type = logging.ERROR
            else:
                error_type = logging.WARNING

        return error_message, error_type

    def _validate_column_after_assay(self, column: str, idx: int, assay_index: int) -> tuple[str, int | None]:
        error_message, error_type = "", None

        if "characteristics" in column or ("material type" in column and "factor value" not in column):
            error_message = f"The column '{column}' cannot be after the assay name"
            error_type = logging.ERROR
        elif "technology type" in column and idx > assay_index + 1:
            error_message = f"The column '{column}' must be immediately after the assay name"
            error_type = logging.ERROR

        return error_message, error_type

    def _validate_individual_columns(self, cnames: list[str], assay_index: int) -> tuple[list[LogicError], int | None]:
        errors = []
        factor_index = None

        for idx, column in enumerate(cnames):
            if idx < assay_index:
                error_message, error_type = self._validate_column_before_assay(column, idx, assay_index)
            else:
                error_message, error_type = self._validate_column_after_assay(column, idx, assay_index)

            if error_type is not None:
                errors.append(LogicError(message=error_message, error_type=error_type))

            if "factor value" in column and factor_index is None:
                factor_index = idx

        return errors, factor_index

    def _validate_factor_columns(self, cnames: list[str], factor_index: int) -> list[LogicError]:
        errors = []
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
            errors.append(LogicError(message=error_message, error_type=logging.ERROR))

        return errors

    def validate(  # type: ignore[override]
        self, df: SDRFDataFrame | pd.DataFrame, column_name: str | None = None
    ) -> list[LogicError]:
        """
        Validate if columns are in the correct order in the SDRF file.

        Parameters:
            df: The pandas DataFrame to validate
            column_name: Not used for this validator as it operates on the entire DataFrame

        Returns:
            List of LogicError for column order issues
        """
        error_columns_order: list[LogicError] = []

        if "assay name" not in list(df):
            return error_columns_order

        cnames = list(df)
        assay_index = cnames.index("assay name")

        error_columns_order.extend(self._check_characteristics_before_assay(cnames, assay_index))

        column_errors, factor_index = self._validate_individual_columns(cnames, assay_index)
        error_columns_order.extend(column_errors)

        if factor_index is not None:
            error_columns_order.extend(self._validate_factor_columns(cnames, factor_index))

        return error_columns_order


@register_validator(validator_name="combination_of_columns_no_duplicate_validator")
class CombinationOfColumnsNoDuplicateValidator(SDRFValidator):
    def validate(self, df: SDRFDataFrame, column_name: str | None = None) -> list[LogicError]:  # type: ignore[override]
        """
        Validate if the combination of values in the specified columns are unique.

        Parameters:
            df: The pandas DataFrame to validate
            column_name: Not used for this validator as it operates on the entire DataFrame

        Returns:
            List of LogicError for duplicate combinations
        """
        if "column_name" not in self.params:
            raise ValueError("column_name must be provided either as an argument or in params")
        column_name = self.params["column_name"]
        if isinstance(column_name, str):
            columns = [col.strip() for col in column_name.split(",")]
        elif isinstance(column_name, list):
            columns = column_name
        else:
            raise ValueError("column_name must be a string or a list of strings")
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            return [
                LogicError(
                    message=f"Columns not found in DataFrame: {', '.join(missing_columns)}",
                    error_type=logging.ERROR,
                )
            ]
        inner_df = df.df
        duplicates = inner_df[inner_df.duplicated(subset=columns, keep=False)]
        errors = []

        if not duplicates.empty:
            grouped = duplicates.groupby(columns).apply(lambda x: x.index.tolist())
            for combo, indices in grouped.items():
                row_info = ", ".join(str(idx) for idx in indices)
                column_info = f" in columns '{', '.join(columns)}'"
                errors.append(
                    LogicError(
                        message=f"Combination '{combo}'{column_info} is duplicated at rows: {row_info}",
                        row=-1,
                        column=", ".join(columns),
                        error_type=logging.ERROR,
                    )
                )
        if "column_name_warning" in self.params:
            column_name_warning = self.params["column_name_warning"]
            if isinstance(column_name_warning, str):
                columns_warning = [col.strip() for col in column_name_warning.split(",")]
            elif isinstance(column_name_warning, list):
                columns_warning = column_name_warning
            else:
                raise ValueError("column_name_warning must be a string or a list of strings")
            missing_columns_warning = [col for col in columns_warning if col not in df.columns]
            if missing_columns_warning:
                return [
                    LogicError(
                        message=f"Columns not found in DataFrame: {', '.join(missing_columns_warning)}",
                        error_type=logging.ERROR,
                    )
                ]
            duplicates_warning = inner_df[inner_df.duplicated(subset=columns_warning, keep=False)]
            if not duplicates_warning.empty:
                grouped_warning = duplicates_warning.groupby(columns_warning).apply(lambda x: x.index.tolist())
                for combo, indices in grouped_warning.items():
                    row_info = ", ".join(str(idx) for idx in indices)
                    column_info = f" in columns '{', '.join(columns_warning)}'"
                    errors.append(
                        LogicError(
                            message=f"Combination '{combo}'{column_info} is duplicated at rows: {row_info}",
                            row=-1,
                            column=", ".join(columns_warning),
                            error_type=logging.WARNING,
                        )
                    )

        return errors


@register_validator(validator_name="empty_cells")
class EmptyCellValidator(SDRFValidator):
    """Validator that checks for empty cells in the SDRF file."""

    def validate(  # type: ignore[override]
        self, df: pd.DataFrame | SDRFDataFrame, column_name: str | None = None
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
