import logging
import re
from typing import Any, ClassVar

import pandas as pd
from pydantic import BaseModel, Field

from sdrf_pipelines.config import config
from sdrf_pipelines.ols.ols import OLS_AVAILABLE

if OLS_AVAILABLE:
    from sdrf_pipelines.ols.ols import OlsClient
from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame
from sdrf_pipelines.sdrf.specification import NORM, NOT_APPLICABLE, NOT_AVAILABLE
from sdrf_pipelines.utils.error_codes import ErrorCode
from sdrf_pipelines.utils.exceptions import LogicError


def _is_string_like_dtype(series: pd.Series) -> bool:
    """Check if a pandas Series has a string-like dtype.

    Handles both legacy object dtype and newer StringDtype (including pyarrow-backed strings).
    This provides compatibility with Python 3.12+ where pandas may use StringDtype by default.

    Args:
        series: The pandas Series to check

    Returns:
        True if the series contains string-like data, False otherwise
    """
    if series.dtype == object:
        return True
    try:
        return pd.api.types.is_string_dtype(series)
    except (AttributeError, TypeError):
        return False


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


@register_validator(validator_name="values")
class ValuesValidator(SDRFValidator):
    """Validate that column values are from an allowed list."""

    def validate(self, series: pd.Series, column_name: str | None = None) -> list[LogicError]:  # type: ignore[override]
        """
        Validate if values in the series are from the allowed list.

        Parameters:
            series: The pandas Series to validate
            column_name: The name of the column being validated

        Returns:
            List of LogicError for invalid values
        """
        allowed_values = self.params.get("values", [])
        error_level = self.params.get("error_level", "warning")
        description = self.params.get("description", "")

        if not allowed_values:
            return []

        # Normalize values for comparison (case-insensitive)
        allowed_lower = {str(v).lower() for v in allowed_values}
        errors = []

        for idx, value in series.items():
            str_value = str(value).strip()
            # Skip empty/NA values
            if str_value.lower() in ("", "nan", "not applicable", "not available"):
                continue
            if str_value.lower() not in allowed_lower:
                level = logging.WARNING if error_level == "warning" else logging.ERROR
                # Format allowed values for readability
                if len(allowed_values) <= 5:
                    allowed_str = ", ".join(f"'{v}'" for v in allowed_values)
                else:
                    allowed_str = ", ".join(f"'{v}'" for v in allowed_values[:5]) + f"... ({len(allowed_values)} total)"
                desc_info = f" {description}" if description else ""
                errors.append(
                    LogicError.from_code(
                        ErrorCode.INVALID_VALUE,
                        value=str(value),
                        row=idx if isinstance(idx, int) else -1,
                        column=column_name,
                        error_type=level,
                        suggestion=f"Use one of: {allowed_str}",
                        description=desc_info,
                    )
                )

        return errors


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
            # Convert to 1-based row numbers for user display
            row_info = ", ".join(str(idx + 1) for idx in duplicate_indices)
            errors.append(
                LogicError.from_code(
                    ErrorCode.DUPLICATE_VALUE,
                    value=str(value),
                    rows=row_info,
                    column=column_name,
                    error_type=logging.ERROR,
                    suggestion="Values must be unique. Check for copy-paste errors.",
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
            errors.append(
                LogicError.from_code(
                    ErrorCode.SINGLE_CARDINALITY_VIOLATED,
                    column=column_name,
                    values=", ".join(map(str, unique_values)),
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
        is_column_name: bool = False,
    ) -> LogicError:
        error_code = ErrorCode.TRAILING_WHITESPACE_COLUMN_NAME if is_column_name else ErrorCode.TRAILING_WHITESPACE
        return LogicError.from_code(
            error_code,
            value=value,
            row=row if row is not None else -1,
            column=column,
            error_type=logging.ERROR,
            suggestion="Remove trailing spaces/tabs from the value. Check your spreadsheet for extra whitespace.",
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
                errors.append(self._create_trailing_whitespace_error(col, is_column_name=True))

        # Check cell values
        for col in value.columns:
            col_series = value[col]
            if isinstance(col_series, pd.Series) and _is_string_like_dtype(col_series):
                for idx, cell_value in enumerate(col_series):
                    if self._has_trailing_whitespace(cell_value):
                        errors.append(self._create_trailing_whitespace_error(cell_value, row=idx, column=col))

        return errors

    def _validate_series(self, value: pd.Series) -> list[LogicError]:
        errors = []
        if _is_string_like_dtype(value):
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
    minimum_columns: int = config.validation.minimum_columns

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
                LogicError.from_code(
                    ErrorCode.INSUFFICIENT_COLUMNS,
                    actual=len(value.columns),
                    required=self.minimum_columns,
                    error_type=logging.ERROR,
                    suggestion=(
                        "Ensure your SDRF includes all required columns. "
                        "Run 'parse_sdrf validate -s <file>' to see which columns are missing."
                    ),
                )
            )
        return errors


# Only register OntologyValidator if OLS dependencies are available
if OLS_AVAILABLE:

    @register_validator(validator_name="ontology")
    class OntologyValidator(SDRFValidator):
        client: OlsClient = Field(default_factory=OlsClient)
        term_name: str = "NT"
        ontologies: list[str] = Field(default_factory=list)
        error_level: int = logging.INFO
        use_ols_cache_only: bool = False
        allow_not_applicable: bool = True
        allow_not_available: bool = True
        description: str = ""
        examples: list[str] = Field(default_factory=list)
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
                    elif key == "allow_not_applicable":
                        self.allow_not_applicable = value
                    elif key == "allow_not_available":
                        self.allow_not_available = value
                    elif key == "description":
                        self.description = value
                    elif key == "examples":
                        self.examples = value if isinstance(value, list) else [value]

        def validate(  # type: ignore[override]
            self, value: pd.Series, column_name: str | None = None
        ) -> list[LogicError]:
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
                except ValueError:
                    errors.append(
                        LogicError.from_code(
                            ErrorCode.INVALID_ONTOLOGY_TERM_FORMAT,
                            value=x,
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
            # Only allow sentinel values when the column definition permits them
            if self.allow_not_available:
                labels.append(NOT_AVAILABLE)
            if self.allow_not_applicable:
                labels.append(NOT_APPLICABLE)
            labels.append(NORM)

            validation_indexes = value.apply(lambda cell_value: _validate_cell(cell_value, labels))

            # Convert to indexes of the row to LogicErrors
            for idx, val in enumerate(validation_indexes):
                if not val:
                    cell_value = value.iloc[idx]
                    # Skip empty values - they are handled by empty_cells validator
                    if not cell_value or str(cell_value).strip() == "":
                        continue
                    # Build suggestion with description and examples
                    suggestion_parts = []
                    if self.description:
                        suggestion_parts.append(self.description)
                    else:
                        suggestion_parts.append(f"Value should be a valid term from: {', '.join(self.ontologies)}")
                    if self.examples:
                        example_str = ", ".join(f"'{ex}'" for ex in self.examples[:3])
                        if len(self.examples) > 3:
                            example_str += f" (and {len(self.examples) - 3} more)"
                        suggestion_parts.append(f"Examples: {example_str}")
                    suggestion = ". ".join(suggestion_parts)

                    errors.append(
                        LogicError.from_code(
                            ErrorCode.ONTOLOGY_TERM_NOT_FOUND,
                            value=cell_value,
                            column=column_name,
                            ontologies=";".join(self.ontologies),
                            row=idx,
                            error_type=self.error_level,
                            suggestion=suggestion,
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
    """Validator that checks if values match a regular expression pattern.

    Params:
        pattern: The regex pattern to match
        case_sensitive: Whether the match is case-sensitive (default: False)
        allow_empty: Whether to skip empty values (default: True)
    """

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
        allow_empty = self.params.get("allow_empty", True)

        series = series.astype(str)

        # Filter out empty values if allow_empty is True
        if allow_empty:
            non_empty_series = series[series.str.strip() != ""]
        else:
            non_empty_series = series

        # Skip sentinel values — SchemaValidator enforces allow_not_applicable/allow_not_available flags
        non_empty_series = non_empty_series[
            ~non_empty_series.str.strip().str.lower().isin(["not applicable", "not available"])
        ]

        not_matched = non_empty_series[~non_empty_series.str.match(pat=pattern, case=case)]
        errors = []

        # Build suggestion from YAML params (description and examples)
        description = self.params.get("description")
        examples = self.params.get("examples", [])

        # Build suggestion message
        suggestion_parts = []

        if description:
            suggestion_parts.append(description)
        else:
            # Fall back to pattern hints for common patterns
            pattern_hints = {
                r"^\d+[yYmMdD]": "Age format like '25y', '6m', '30d' or combinations like '25y6m'",
                r"not available|not applicable": "Use 'not available' or 'not applicable' for missing values",
                r"NT=.+;AC=": "Ontology term format: NT=term_name;AC=ONTOLOGY:accession",
            }
            for hint_pattern, hint_text in pattern_hints.items():
                if hint_pattern in pattern:
                    suggestion_parts.append(hint_text)
                    break
            if not suggestion_parts:
                suggestion_parts.append(f"Value must match the pattern: {pattern}")

        # Add examples if available
        if examples:
            example_str = ", ".join(f"'{ex}'" for ex in examples[:3])  # Show up to 3 examples
            if len(examples) > 3:
                example_str += f" (and {len(examples) - 3} more)"
            suggestion_parts.append(f"Examples: {example_str}")

        suggestion = ". ".join(suggestion_parts)

        for idx, value in not_matched.items():
            # Convert to 1-based row number for user display
            row_num = int(idx) + 1 if isinstance(idx, (int, float)) else -1
            errors.append(
                LogicError.from_code(
                    ErrorCode.PATTERN_MISMATCH,
                    value=str(value),
                    row=row_num,
                    column=column_name,
                    error_type=logging.ERROR,
                    suggestion=suggestion,
                    pattern=pattern,
                )
            )

        return errors


@register_validator(validator_name="number_with_unit")
class NumberWithUnitValidator(SDRFValidator):
    """Validate values in number-unit format (e.g., '10 ppm', '0.5 Da').

    Params:
        units: list[str] — allowed unit strings (required)
        allow_negative: bool — allow negative numbers (default: False)
        allow_decimal: bool — allow decimal numbers (default: True)
        special_values: list[str] — additional allowed literal values (default: [])
        error_level: str — "error" or "warning" (default: "error")
        description: str — human-readable description
        examples: list[str] — example valid values
    """

    def validate(self, series: pd.Series, column_name: str | None = None) -> list[LogicError]:  # type: ignore[override]
        units = self.params.get("units", [])
        allow_negative = self.params.get("allow_negative", False)
        allow_decimal = self.params.get("allow_decimal", True)
        special_values = self.params.get("special_values", [])
        allow_not_available = self.params.get("allow_not_available", False)
        allow_not_applicable = self.params.get("allow_not_applicable", False)
        error_level = self.params.get("error_level", "error")
        description = self.params.get("description", "")
        examples = self.params.get("examples", [])

        if not units:
            return []

        sign = "-?" if allow_negative else ""
        decimal = r"(\.\d+)?" if allow_decimal else ""
        units_pattern = "|".join(re.escape(u) for u in units)
        pattern = rf"^{sign}\d+{decimal}\s*({units_pattern})$"

        special = list(special_values)
        if allow_not_available:
            special.append("not available")
        if allow_not_applicable:
            special.append("not applicable")
        if special:
            pattern += "|" + "|".join(rf"^{re.escape(s)}$" for s in special)

        series = series.astype(str)
        non_empty = series[series.str.strip() != ""]
        not_matched = non_empty[~non_empty.str.match(pat=pattern, case=False)]

        errors = []
        level = logging.WARNING if error_level == "warning" else logging.ERROR
        units_str = ", ".join(units)

        suggestion_parts = []
        if description:
            suggestion_parts.append(description)
        else:
            suggestion_parts.append(f"Expected format: <number> <unit> where unit is one of: {units_str}")
        if special_values:
            suggestion_parts.append(f"Also accepts: {', '.join(special_values)}")
        if examples:
            example_str = ", ".join(f"'{ex}'" for ex in examples[:3])
            suggestion_parts.append(f"Examples: {example_str}")
        suggestion = ". ".join(suggestion_parts)

        for idx, value in not_matched.items():
            row_num = int(idx) + 1 if isinstance(idx, (int, float)) else -1
            errors.append(
                LogicError.from_code(
                    ErrorCode.PATTERN_MISMATCH,
                    value=str(value),
                    row=row_num,
                    column=column_name,
                    error_type=level,
                    suggestion=suggestion,
                    pattern=pattern,
                )
            )

        return errors


@register_validator(validator_name="mz_value")
class MzValueValidator(SDRFValidator):
    """Validate m/z values in the format ``{number} m/z`` or ``{number}m/z``.

    Accepts both ``400 m/z`` (preferred, consistent with ``10 ppm``) and
    ``400m/z`` (legacy, seen in existing datasets). Optional whitespace
    between number and unit.

    Params:
        description: str — human-readable description
        examples: list[str] — example valid values
    """

    _MZ_PATTERN = re.compile(r"^(\d+(\.\d+)?)\s*m/z$", re.IGNORECASE)

    def validate(self, series: pd.Series, column_name: str | None = None) -> list[LogicError]:  # type: ignore[override]
        description = self.params.get("description", "m/z value with unit, e.g. '400 m/z' or '400m/z'")
        examples = self.params.get("examples", ["400 m/z", "1200 m/z", "350.5m/z"])

        series = series.astype(str)
        non_empty = series[series.str.strip() != ""]
        non_empty = non_empty[~non_empty.str.strip().str.lower().isin(["not applicable", "not available"])]

        errors = []
        suggestion_parts = [description]
        if examples:
            example_str = ", ".join(f"'{ex}'" for ex in examples[:3])
            suggestion_parts.append(f"Examples: {example_str}")
        suggestion = ". ".join(suggestion_parts)

        for idx, value in non_empty.items():
            if not self._MZ_PATTERN.match(value.strip()):
                row_num = int(idx) + 1 if isinstance(idx, (int, float)) else -1
                errors.append(
                    LogicError.from_code(
                        ErrorCode.PATTERN_MISMATCH,
                        value=str(value),
                        row=row_num,
                        column=column_name,
                        error_type=logging.ERROR,
                        suggestion=suggestion,
                        pattern=self._MZ_PATTERN.pattern,
                    )
                )
        return errors


@register_validator(validator_name="mz_range_interval")
class MzRangeIntervalValidator(SDRFValidator):
    """Validate m/z range intervals in the format ``{number} m/z-{number} m/z``.

    Accepts both spaced (``400 m/z-1200 m/z``, preferred) and compact
    (``400m/z-1200m/z``, legacy) notation. Validates that the lower bound
    is less than the upper bound.

    Params:
        description: str — human-readable description
        examples: list[str] — example valid values
    """

    _INTERVAL_PATTERN = re.compile(r"^(\d+(\.\d+)?)\s*m/z\s*-\s*(\d+(\.\d+)?)\s*m/z$", re.IGNORECASE)

    def validate(self, series: pd.Series, column_name: str | None = None) -> list[LogicError]:  # type: ignore[override]
        description = self.params.get("description", "m/z range interval, e.g. '400 m/z-1200 m/z' or '400m/z-1200m/z'")
        examples = self.params.get("examples", ["400 m/z-1200 m/z", "100m/z-2000m/z"])

        series = series.astype(str)
        non_empty = series[series.str.strip() != ""]
        non_empty = non_empty[~non_empty.str.strip().str.lower().isin(["not applicable", "not available"])]

        errors = []
        suggestion_parts = [description]
        if examples:
            example_str = ", ".join(f"'{ex}'" for ex in examples[:3])
            suggestion_parts.append(f"Examples: {example_str}")
        suggestion = ". ".join(suggestion_parts)

        for idx, value in non_empty.items():
            row_num = int(idx) + 1 if isinstance(idx, (int, float)) else -1
            m = self._INTERVAL_PATTERN.match(value.strip())
            if not m:
                errors.append(
                    LogicError.from_code(
                        ErrorCode.PATTERN_MISMATCH,
                        value=str(value),
                        row=row_num,
                        column=column_name,
                        error_type=logging.ERROR,
                        suggestion=suggestion,
                        pattern=self._INTERVAL_PATTERN.pattern,
                    )
                )
            else:
                lower = float(m.group(1))
                upper = float(m.group(3))
                if lower >= upper:
                    errors.append(
                        LogicError.from_code(
                            ErrorCode.INVALID_VALUE,
                            value=str(value),
                            row=row_num,
                            column=column_name,
                            error_type=logging.ERROR,
                            suggestion=f"Lower bound ({lower}) must be less than upper bound ({upper})",
                        )
                    )
        return errors


@register_validator(validator_name="accession")
class AccessionValidator(SDRFValidator):
    """Validate accession identifiers with prefix + suffix format.

    Params:
        format: str — predefined format ("biosample", "cellosaurus", "proteomexchange")
        prefix: str — custom prefix (literal or simple regex)
        suffix: str — custom suffix regex (default: r"\\d+")
        error_level: str — "error" or "warning" (default: "error")
        description: str — human-readable description
        examples: list[str] — example valid values
    """

    PREDEFINED_FORMATS: ClassVar[dict[str, dict[str, str]]] = {
        "biosample": {"prefix": r"SAM(N|EA|D)", "suffix": r"\d+"},
        "cellosaurus": {"prefix": r"CVCL_", "suffix": r"[A-Z0-9]+"},
        "proteomexchange": {"prefix": r"PXD", "suffix": r"\d+"},
    }

    def validate(self, series: pd.Series, column_name: str | None = None) -> list[LogicError]:  # type: ignore[override]
        fmt = self.params.get("format")
        prefix = self.params.get("prefix")
        suffix = self.params.get("suffix", r"\d+")
        allow_not_available = self.params.get("allow_not_available", False)
        allow_not_applicable = self.params.get("allow_not_applicable", False)
        error_level = self.params.get("error_level", "error")
        description = self.params.get("description", "")
        examples = self.params.get("examples", [])

        if fmt:
            if fmt not in self.PREDEFINED_FORMATS:
                return [
                    LogicError(
                        message=(
                            f"Unknown accession format '{fmt}'. "
                            f"Known formats: {', '.join(self.PREDEFINED_FORMATS.keys())}"
                        ),
                        column=column_name,
                        error_type=logging.ERROR,
                    )
                ]
            preset = self.PREDEFINED_FORMATS[fmt]
            prefix = preset["prefix"]
            suffix = preset["suffix"]
        elif not prefix:
            return [
                LogicError(
                    message="AccessionValidator requires either 'format' or 'prefix' param",
                    column=column_name,
                    error_type=logging.ERROR,
                )
            ]

        pattern = rf"^{prefix}{suffix}$"

        special = []
        if allow_not_available:
            special.append("not available")
        if allow_not_applicable:
            special.append("not applicable")
        if special:
            special_pattern = "|".join(rf"^{re.escape(s)}$" for s in special)
            pattern = rf"(?:{pattern})|(?i:(?:{special_pattern}))"

        series = series.astype(str)
        non_empty = series[series.str.strip() != ""]
        not_matched = non_empty[~non_empty.str.match(pat=pattern, case=True)]

        errors = []
        level = logging.WARNING if error_level == "warning" else logging.ERROR

        suggestion_parts = []
        if description:
            suggestion_parts.append(description)
        elif fmt:
            suggestion_parts.append(f"Expected {fmt} accession format")
        else:
            suggestion_parts.append(f"Expected format: {prefix}<{suffix}>")
        if examples:
            example_str = ", ".join(f"'{ex}'" for ex in examples[:3])
            suggestion_parts.append(f"Examples: {example_str}")
        suggestion = ". ".join(suggestion_parts)

        for idx, value in not_matched.items():
            row_num = int(idx) + 1 if isinstance(idx, (int, float)) else -1
            errors.append(
                LogicError.from_code(
                    ErrorCode.PATTERN_MISMATCH,
                    value=str(value),
                    row=row_num,
                    column=column_name,
                    error_type=level,
                    suggestion=suggestion,
                    pattern=pattern,
                )
            )

        return errors


@register_validator(validator_name="identifier")
class IdentifierValidator(SDRFValidator):
    """Validate alphanumeric identifiers with optional special values.

    Params:
        charset: str — character class regex (default: "[A-Za-z0-9_-]")
        special_values: list[str] — additional allowed literal values (default: [])
        error_level: str — "error" or "warning" (default: "error")
        description: str — human-readable description
    """

    def validate(self, series: pd.Series, column_name: str | None = None) -> list[LogicError]:  # type: ignore[override]
        charset = self.params.get("charset", "[A-Za-z0-9_-]")
        special_values = self.params.get("special_values", [])
        allow_not_available = self.params.get("allow_not_available", False)
        allow_not_applicable = self.params.get("allow_not_applicable", False)
        error_level = self.params.get("error_level", "error")
        description = self.params.get("description", "")

        pattern = rf"^{charset}+$"

        special = list(special_values)
        if allow_not_available:
            special.append("not available")
        if allow_not_applicable:
            special.append("not applicable")
        if special:
            special_escaped = [re.escape(s) for s in special]
            pattern += "|" + "|".join(rf"^{s}$" for s in special_escaped)

        series = series.astype(str)
        non_empty = series[series.str.strip() != ""]
        not_matched = non_empty[~non_empty.str.match(pat=pattern, case=False)]

        errors = []
        level = logging.WARNING if error_level == "warning" else logging.ERROR

        suggestion = description or f"Expected alphanumeric identifier matching {charset}"
        if special_values:
            suggestion += f". Also accepts: {', '.join(special_values)}"

        for idx, value in not_matched.items():
            row_num = int(idx) + 1 if isinstance(idx, (int, float)) else -1
            errors.append(
                LogicError.from_code(
                    ErrorCode.PATTERN_MISMATCH,
                    value=str(value),
                    row=row_num,
                    column=column_name,
                    error_type=level,
                    suggestion=suggestion,
                    pattern=pattern,
                )
            )

        return errors


@register_validator(validator_name="date")
class DateValidator(SDRFValidator):
    """Validate ISO 8601 date values at variable precision.

    Params:
        format: str — date format ("iso8601")
        precision: list[str] — allowed precision levels: "year", "month", "day"
        error_level: str — "error" or "warning" (default: "warning")
    """

    def validate(self, series: pd.Series, column_name: str | None = None) -> list[LogicError]:  # type: ignore[override]
        precision = self.params.get("precision", ["year", "month", "day"])
        allow_not_available = self.params.get("allow_not_available", False)
        allow_not_applicable = self.params.get("allow_not_applicable", False)
        error_level = self.params.get("error_level", "warning")

        branches = []
        if "day" in precision:
            branches.append(r"\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])")
        if "month" in precision:
            branches.append(r"\d{4}-(0[1-9]|1[0-2])")
        if "year" in precision:
            branches.append(r"\d{4}")

        pattern = "^(" + "|".join(branches) + ")$"

        special = []
        if allow_not_available:
            special.append("not available")
        if allow_not_applicable:
            special.append("not applicable")
        if special:
            pattern += "|" + "|".join(rf"^{re.escape(s)}$" for s in special)

        series = series.astype(str)
        non_empty = series[series.str.strip() != ""]
        not_matched = non_empty[~non_empty.str.match(pat=pattern, case=False)]

        errors = []
        level = logging.WARNING if error_level == "warning" else logging.ERROR
        precision_str = ", ".join(precision)
        suggestion = f"Expected ISO 8601 date (precision: {precision_str}). Examples: 2024, 2024-01, 2024-01-15"

        for idx, value in not_matched.items():
            row_num = int(idx) + 1 if isinstance(idx, (int, float)) else -1
            errors.append(
                LogicError.from_code(
                    ErrorCode.PATTERN_MISMATCH,
                    value=str(value),
                    row=row_num,
                    column=column_name,
                    error_type=level,
                    suggestion=suggestion,
                    pattern=pattern,
                )
            )

        return errors


@register_validator(validator_name="structured_kv")
class StructuredKVValidator(SDRFValidator):
    """Validate semicolon-separated key=value pair formats.

    Params:
        separator: str — separator between pairs (default: ";")
        fields: list[dict] — list of {"key": str, "value": str} field definitions
        error_level: str — "error" or "warning" (default: "error")
        description: str — human-readable description
    """

    def validate(self, series: pd.Series, column_name: str | None = None) -> list[LogicError]:  # type: ignore[override]
        separator = self.params.get("separator", ";")
        fields = self.params.get("fields", [])
        allow_not_available = self.params.get("allow_not_available", False)
        allow_not_applicable = self.params.get("allow_not_applicable", False)
        error_level = self.params.get("error_level", "error")
        description = self.params.get("description", "")

        if not fields:
            return []

        series = series.astype(str)
        errors = []
        level = logging.WARNING if error_level == "warning" else logging.ERROR

        expected_parts = [f"{f['key']}=<{f.get('value', '...')}>" for f in fields]
        expected_format = separator.join(expected_parts)
        suggestion = description or f"Expected format: {expected_format}"

        for idx, value in series.items():
            str_value = str(value).strip()
            if str_value == "" or str_value.lower() == "nan":
                continue
            if allow_not_available and str_value.lower() == "not available":
                continue
            if allow_not_applicable and str_value.lower() == "not applicable":
                continue

            parts = str_value.split(separator)
            parsed = {}
            for part in parts:
                part = part.strip()
                if "=" not in part:
                    continue
                key, val = part.split("=", 1)
                parsed[key.strip()] = val.strip()

            valid = True
            for field_def in fields:
                key = field_def["key"]
                value_pattern = field_def.get("value", ".+")
                if key not in parsed:
                    valid = False
                    break
                if not re.match(rf"^{value_pattern}$", parsed[key]):
                    valid = False
                    break

            if not valid:
                row_num = int(idx) + 1 if isinstance(idx, (int, float)) else -1
                errors.append(
                    LogicError.from_code(
                        ErrorCode.PATTERN_MISMATCH,
                        value=str_value,
                        row=row_num,
                        column=column_name,
                        error_type=level,
                        suggestion=suggestion,
                    )
                )

        return errors


@register_validator(validator_name="semver")
class SemverValidator(SDRFValidator):
    """Validate semantic version strings.

    Params:
        prefix: str — optional prefix (e.g., "v")
        allow_prerelease: bool — allow -alpha, -rc.1 suffixes (default: True)
        error_level: str — "error" or "warning" (default: "error")
    """

    def validate(self, series: pd.Series, column_name: str | None = None) -> list[LogicError]:  # type: ignore[override]
        prefix = self.params.get("prefix", "")
        allow_prerelease = self.params.get("allow_prerelease", True)
        error_level = self.params.get("error_level", "error")

        prefix_escaped = re.escape(prefix) if prefix else ""
        prerelease = r"(-[\w.]+)?" if allow_prerelease else ""
        pattern = rf"^{prefix_escaped}\d+\.\d+\.\d+{prerelease}$"

        series = series.astype(str)
        non_empty = series[series.str.strip() != ""]
        # Skip sentinel values — SchemaValidator enforces allow_not_applicable/allow_not_available flags
        non_empty = non_empty[~non_empty.str.strip().str.lower().isin(["not applicable", "not available"])]
        not_matched = non_empty[~non_empty.str.match(pat=pattern, case=False)]

        errors = []
        level = logging.WARNING if error_level == "warning" else logging.ERROR
        example = f"{prefix}1.0.0" if prefix else "1.0.0"
        suggestion = f"Expected semantic version format: {example}"

        for idx, value in not_matched.items():
            row_num = int(idx) + 1 if isinstance(idx, (int, float)) else -1
            errors.append(
                LogicError.from_code(
                    ErrorCode.PATTERN_MISMATCH,
                    value=str(value),
                    row=row_num,
                    column=column_name,
                    error_type=level,
                    suggestion=suggestion,
                    pattern=pattern,
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
            errors.append(
                LogicError.from_code(
                    ErrorCode.CHARACTERISTICS_AFTER_ASSAY,
                    columns=", ".join(characteristics_after_assay),
                    error_type=logging.ERROR,
                )
            )
        return errors

    def _validate_column_before_assay(
        self, column: str, idx: int, assay_index: int
    ) -> tuple[ErrorCode | None, int | None]:
        error_code, error_type = None, None

        if "comment" in column:
            error_code = ErrorCode.COMMENT_BEFORE_ASSAY
            error_type = logging.ERROR
        elif "technology type" in column:
            error_code = ErrorCode.TECHNOLOGY_TYPE_MISPLACED
            if assay_index - idx > 1:
                error_type = logging.ERROR
            else:
                error_type = logging.WARNING

        return error_code, error_type

    def _validate_column_after_assay(
        self, column: str, idx: int, assay_index: int
    ) -> tuple[ErrorCode | None, int | None]:
        error_code, error_type = None, None

        if "characteristics" in column or ("material type" in column and "factor value" not in column):
            error_code = ErrorCode.COLUMN_ORDER_INVALID
            error_type = logging.ERROR
        elif "technology type" in column and idx > assay_index + 1:
            error_code = ErrorCode.TECHNOLOGY_TYPE_MISPLACED
            error_type = logging.ERROR

        return error_code, error_type

    def _validate_individual_columns(self, cnames: list[str], assay_index: int) -> tuple[list[LogicError], int | None]:
        errors = []
        factor_index = None

        for idx, column in enumerate(cnames):
            if idx < assay_index:
                error_code, error_type = self._validate_column_before_assay(column, idx, assay_index)
            else:
                error_code, error_type = self._validate_column_after_assay(column, idx, assay_index)

            if error_type is not None and error_code is not None:
                errors.append(LogicError.from_code(error_code, column=column, error_type=error_type))

            if "factor value" in column and factor_index is None:
                factor_index = idx

        return errors, factor_index

    def _validate_factor_columns(self, cnames: list[str], factor_index: int) -> list[LogicError]:
        errors = []
        temp: list[str] = []
        error_cols: list[str] = []

        for column in cnames[factor_index:]:
            if "comment" in column or "characteristics" in column:
                error_cols.extend(temp)
                temp = []
            elif "factor value" in column:
                temp.append(column)

        if error_cols:
            errors.append(
                LogicError.from_code(
                    ErrorCode.FACTOR_COLUMN_NOT_LAST,
                    columns=", ".join(error_cols),
                    error_type=logging.ERROR,
                )
            )

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

        cnames: list[str] = [str(c) for c in df.columns]
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
                LogicError.from_code(
                    ErrorCode.COLUMNS_NOT_FOUND,
                    columns=", ".join(missing_columns),
                    error_type=logging.ERROR,
                )
            ]
        # Handle both SDRFDataFrame and plain DataFrame
        inner_df: pd.DataFrame = df.df if isinstance(df, SDRFDataFrame) else df
        duplicates = inner_df[inner_df.duplicated(subset=columns, keep=False)]
        errors: list[LogicError] = []

        if not duplicates.empty:
            grouped = duplicates.groupby(columns).apply(lambda x: x.index.tolist())
            for combo, indices in grouped.items():
                row_info = ", ".join(str(idx) for idx in indices)
                errors.append(
                    LogicError.from_code(
                        ErrorCode.DUPLICATE_COMBINATION,
                        value=str(combo),
                        columns=", ".join(columns),
                        rows=row_info,
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
                    LogicError.from_code(
                        ErrorCode.COLUMNS_NOT_FOUND,
                        columns=", ".join(missing_columns_warning),
                        error_type=logging.ERROR,
                    )
                ]
            duplicates_warning = inner_df[inner_df.duplicated(subset=columns_warning, keep=False)]
            if not duplicates_warning.empty:
                grouped_warning = duplicates_warning.groupby(columns_warning).apply(lambda x: x.index.tolist())
                for combo, indices in grouped_warning.items():
                    row_info = ", ".join(str(idx) for idx in indices)
                    errors.append(
                        LogicError.from_code(
                            ErrorCode.DUPLICATE_COMBINATION,
                            value=str(combo),
                            columns=", ".join(columns_warning),
                            rows=row_info,
                            error_type=logging.WARNING,
                        )
                    )

        return errors


@register_validator(validator_name="empty_cells")
class EmptyCellValidator(SDRFValidator):
    """Validator that checks for empty cells in required columns of the SDRF file.

    Params:
        required_columns: List of column names that are required and should not have empty values.
                         If not provided, the validator will be skipped (no columns checked).
    """

    def validate(  # type: ignore[override]
        self, df: pd.DataFrame | SDRFDataFrame, column_name: str | None = None
    ) -> list[LogicError]:
        """
        Check for empty cells in required columns only.

        Parameters:
            df: The pandas DataFrame to validate
            column_name: Not used for this validator as it operates on the entire DataFrame

        Returns:
            List of LogicError for empty cells in required columns
        """
        errors: list[LogicError] = []

        # Get required columns from params - only check these columns
        required_columns = self.params.get("required_columns", [])
        if not required_columns:
            # No required columns specified, skip validation
            return errors

        # Only check columns that exist in the DataFrame and are required
        columns_to_check = [col for col in required_columns if col in df.columns]
        if not columns_to_check:
            return errors

        def validate_string(cell_value):
            if pd.isna(cell_value):
                return False
            if not isinstance(cell_value, str):
                cell_value = str(cell_value)
            return cell_value != "nan" and len(cell_value.strip()) > 0

        # Only validate the required columns
        df_subset = df[columns_to_check]
        validation_results = df_subset.map(validate_string)

        # Get the indices where the validation fails
        failed_indices = [
            (row, col)
            for row in validation_results.index
            for col in validation_results.columns
            if not validation_results.at[row, col]
        ]

        inner_df = df.df if isinstance(df, SDRFDataFrame) else df
        source_name_col = "source name" if "source name" in inner_df.columns else None

        for row_idx, col in failed_indices:
            row_display = int(row_idx) + 1 if isinstance(row_idx, (int, float)) else row_idx
            source_name = "n/a"
            if source_name_col is not None:
                val = inner_df.loc[row_idx, source_name_col]
                source_name = str(val).strip() if pd.notna(val) and str(val).strip() else "n/a"

            errors.append(
                LogicError.from_code(
                    ErrorCode.EMPTY_CELL,
                    row=row_display,
                    column=col,
                    source_name=source_name,
                    error_type=logging.ERROR,
                )
            )

        return errors
