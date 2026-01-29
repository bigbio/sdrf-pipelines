"""Schema validator for validating SDRF data against schema definitions."""

import logging
from typing import Any

import pandas as pd

from sdrf_pipelines.ols.ols import OLS_AVAILABLE
from sdrf_pipelines.sdrf.schemas.models import (
    ColumnDefinition,
    RequirementLevel,
    SchemaDefinition,
    ValidatorConfig,
)
from sdrf_pipelines.sdrf.schemas.registry import SchemaRegistry
from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame
from sdrf_pipelines.sdrf.specification import NOT_APPLICABLE, NOT_AVAILABLE
from sdrf_pipelines.sdrf.validators import SDRFValidator, get_validator
from sdrf_pipelines.utils.exceptions import LogicError


class SchemaValidator:
    """Class for validating SDRF data against schemas."""

    def __init__(self, registry: SchemaRegistry):
        """Initialize the validator with a schema registry.

        Args:
            registry: SchemaRegistry instance containing loaded schemas
        """
        self.registry = registry

    def _create_validator_instance(
        self, validator_config: ValidatorConfig, skip_ontology: bool = False
    ) -> SDRFValidator | None:
        """Create a validator instance from a configuration.

        Args:
            validator_config: The validator configuration
            skip_ontology: If True, skip ontology validators

        Returns:
            A validator instance or None if skipped/unavailable
        """
        validator_name = validator_config.validator_name
        validator_params = validator_config.params

        if skip_ontology and validator_name == "ontology":
            logging.debug("Skipping ontology validator as requested by skip_ontology flag")
            return None

        validator_class = get_validator(validator_name)
        if not validator_class:
            if validator_name == "ontology" and not OLS_AVAILABLE:
                logging.warning(
                    "Ontology validator '%s' is not available because OLS dependencies are not installed. "
                    "Install them with: pip install sdrf-pipelines[ontology]",
                    validator_name,
                )
            else:
                logging.warning("Validator type '%s' not found in registry", validator_name)
            return None

        return validator_class(params=validator_params)

    def _apply_global_validators(
        self,
        df: pd.DataFrame | SDRFDataFrame,
        schema: SchemaDefinition,
        use_ols_cache_only: bool,
        skip_ontology: bool,
    ) -> list[LogicError]:
        """Apply global (schema-level) validators."""
        errors = []
        required_columns = [col.name for col in schema.columns if col.requirement == RequirementLevel.REQUIRED]

        for validator_config in schema.validators:
            validator_config.params["use_ols_cache_only"] = use_ols_cache_only
            if validator_config.validator_name == "empty_cells":
                validator_config.params["required_columns"] = required_columns
            validator = self._create_validator_instance(validator_config, skip_ontology=skip_ontology)
            if validator:
                errors.extend(validator.validate(df, column_name=None))
        return errors

    def _validate_required_columns(
        self, df: pd.DataFrame | SDRFDataFrame, schema: SchemaDefinition
    ) -> list[LogicError]:
        """Validate that all required columns are present."""
        errors = []
        required_columns = [col.name for col in schema.columns if col.requirement == RequirementLevel.REQUIRED]
        for col_name in required_columns:
            if col_name not in df.columns:
                errors.append(
                    LogicError(
                        message=f"Required column '{col_name}' is missing from the SDRF file",
                        column=col_name,
                        error_type=logging.ERROR,
                        suggestion=f"Add the column '{col_name}' to your SDRF file with appropriate values.",
                    )
                )
        return errors

    def _apply_column_validators(
        self,
        column_series: pd.Series,
        column_def: ColumnDefinition,
        use_ols_cache_only: bool,
        skip_ontology: bool,
    ) -> list[LogicError]:
        """Apply validators to a specific column."""
        errors = []
        for validator_config in column_def.validators:
            validator_config.params["use_ols_cache_only"] = use_ols_cache_only
            validator = self._create_validator_instance(validator_config, skip_ontology=skip_ontology)
            if validator:
                col_errors = validator.validate(column_series, column_name=column_def.name)
                errors.extend(col_errors)
        return errors

    def _validate_not_applicable_values(
        self, column_series: pd.Series, column_def: ColumnDefinition
    ) -> list[LogicError]:
        """Validate that 'not applicable' values are allowed for this column."""
        errors = []
        if not column_def.allow_not_applicable:
            str_series = column_series.fillna("").astype(str)
            not_applicable_values = str_series[str_series.str.lower().str.contains(NOT_APPLICABLE)]
            if not not_applicable_values.empty:
                row_indices = not_applicable_values.index.tolist()
                errors.append(
                    LogicError(
                        message=(
                            f"Column '{column_def.name}' contains 'not applicable' values at "
                            f"{len(row_indices)} row(s), but this column requires actual values"
                        ),
                        column=column_def.name,
                        error_type=logging.ERROR,
                        suggestion=(
                            "This column requires a real value. If the information is truly not applicable, "
                            "consider using a different template or contact the data submitters."
                        ),
                    )
                )
        return errors

    def _validate_not_available_values(
        self, column_series: pd.Series, column_def: ColumnDefinition
    ) -> list[LogicError]:
        """Validate that 'not available' values are allowed for this column."""
        errors = []
        if not column_def.allow_not_available:
            str_series = column_series.fillna("").astype(str)
            not_available_values = str_series[str_series.str.lower().str.contains(NOT_AVAILABLE)]
            if not not_available_values.empty:
                row_indices = not_available_values.index.tolist()
                errors.append(
                    LogicError(
                        message=(
                            f"Column '{column_def.name}' contains 'not available' values at "
                            f"{len(row_indices)} row(s), but this column requires actual values"
                        ),
                        column=column_def.name,
                        error_type=logging.ERROR,
                        suggestion=(
                            "Please provide actual values for this column. 'Not available' is not accepted. "
                            "If the data is truly unavailable, contact the data submitters."
                        ),
                    )
                )
        return errors

    def _process_column_validation(
        self,
        df: pd.DataFrame | SDRFDataFrame,
        column_def: ColumnDefinition,
        use_ols_cache_only: bool,
        skip_ontology: bool,
    ) -> list[LogicError]:
        """Process validation for a single column."""
        errors = []
        if column_def.name in df.columns:
            column_series = df[column_def.name]

            errors.extend(
                self._apply_column_validators(column_series, column_def, use_ols_cache_only, skip_ontology)
            )
            errors.extend(self._validate_not_applicable_values(column_series, column_def))
            errors.extend(self._validate_not_available_values(column_series, column_def))

        return errors

    def validate(
        self,
        df: pd.DataFrame | SDRFDataFrame,
        schema_name: str,
        use_ols_cache_only: bool = False,
        skip_ontology: bool = False,
    ) -> list[LogicError]:
        """Validate a DataFrame against a schema.

        Args:
            df: The DataFrame to validate
            schema_name: Name of the schema to validate against
            use_ols_cache_only: If True, use only cached OLS data
            skip_ontology: If True, skip ontology term validation

        Returns:
            List of validation errors
        """
        schema = self.registry.get_schema(schema_name)
        if not schema:
            raise ValueError(f"Schema '{schema_name}' not found in registry")

        errors: list[LogicError] = []

        errors.extend(self._apply_global_validators(df, schema, use_ols_cache_only, skip_ontology))
        errors.extend(self._validate_required_columns(df, schema))

        for column_def in schema.columns:
            errors.extend(
                self._process_column_validation(df, column_def, use_ols_cache_only, skip_ontology)
            )

        return errors

    def validate_with_multiple_schemas(
        self, df: pd.DataFrame, schema_names: list[str]
    ) -> dict[str, list[LogicError]]:
        """Validate a DataFrame against multiple schemas.

        Args:
            df: The DataFrame to validate
            schema_names: List of schema names to validate against

        Returns:
            Dict mapping schema names to their validation errors
        """
        results = {}
        for schema_name in schema_names:
            try:
                errors = self.validate(df, schema_name)
                results[schema_name] = errors
            except Exception as e:
                logging.error("Error validating against schema '%s': %s", schema_name, e)
                results[schema_name] = [LogicError(message=f"Validation error: {e}", error_type=logging.ERROR)]

        return results

    def validate_with_best_schema(self, df: pd.DataFrame) -> dict[str, Any]:
        """Validate a DataFrame and determine the best matching schema.

        Args:
            df: The DataFrame to validate

        Returns:
            Dict with 'best_schema', 'errors', and 'all_results'
        """
        all_results = {}
        schema_scores = {}

        for schema_name in self.registry.get_schema_names():
            errors = self.validate(df, schema_name)
            all_results[schema_name] = errors
            schema_scores[schema_name] = len(errors)

        if not schema_scores:
            return {"best_schema": None, "errors": [], "all_results": {}}

        best_schema = min(schema_scores.items(), key=lambda x: x[1])[0]

        return {
            "best_schema": best_schema,
            "errors": all_results[best_schema],
            "all_results": all_results,
        }
