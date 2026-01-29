import copy
import json
import logging
import os
from collections import OrderedDict
from enum import Enum
from typing import Any

import pandas as pd
import yaml
from pydantic import BaseModel, Field

from sdrf_pipelines.ols.ols import OLS_AVAILABLE
from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame
from sdrf_pipelines.sdrf.specification import NOT_APPLICABLE, NOT_AVAILABLE
from sdrf_pipelines.sdrf.validators import SDRFValidator, get_validator
from sdrf_pipelines.utils.exceptions import LogicError

_VALIDATOR_REGISTRY: dict[str, type[SDRFValidator]] = {}


class RequirementLevel(str, Enum):
    REQUIRED = "required"
    RECOMMENDED = "recommended"
    OPTIONAL = "optional"


class ValidatorConfig(BaseModel):
    validator_name: str
    params: dict[str, Any] = Field(default_factory=dict)


class ColumnDefinition(BaseModel):
    name: str
    description: str
    requirement: RequirementLevel
    allow_not_applicable: bool = False
    allow_not_available: bool = False
    validators: list[ValidatorConfig] = Field(default_factory=list)


class SchemaDefinition(BaseModel):
    name: str
    description: str
    validators: list[ValidatorConfig] = Field(default_factory=list)
    columns: list[ColumnDefinition] = Field(default_factory=list)


class MergeStrategy(str, Enum):
    FIRST = "first"
    LAST = "last"
    COMBINE = "combine"


class SchemaRegistry:
    """Registry for SDRF schema definitions.

    Loads schemas from the sdrf-templates submodule by default. The templates
    follow a versioned directory structure: {template_name}/{version}/{template_name}.yaml

    For backwards compatibility, legacy schema names are mapped to new template names:
        - "minimum" → "base"
        - "default" → "ms-proteomics"
        - "cell_lines" → "cell-lines"
        - "nonvertebrates" → "invertebrates"
    """

    # Mapping of legacy schema names to new versioned template names
    LEGACY_NAME_MAPPING = {
        "minimum": "base",
        "default": "ms-proteomics",
        "cell_lines": "cell-lines",
        "nonvertebrates": "invertebrates",
    }

    def __init__(
        self, schema_dir: str | None = None, use_versioned: bool = True, template_versions: dict[str, str] | None = None
    ):
        """Initialize the schema registry.

        Args:
            schema_dir: Path to schema directory. If None, uses default location (sdrf-templates submodule).
            use_versioned: If True (default), load from versioned sdrf-templates structure.
            template_versions: Dict mapping template names to specific versions.
                             If None, uses latest versions from templates.yaml manifest.
        """
        self.schemas: dict[str, SchemaDefinition] = {}
        self.raw_schema_data: dict[str, dict[str, Any]] = {}  # Store raw schema data for inheritance resolution
        self.use_versioned = use_versioned
        self.template_versions = template_versions or {}
        self.manifest: dict[str, Any] = {}

        if schema_dir is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            schema_dir = os.path.join(current_dir, "sdrf-templates")
            if not use_versioned:
                logging.warning(
                    "use_versioned=False is deprecated. "
                    "Templates are now loaded from sdrf-templates submodule by default."
                )
        self.schema_dir = schema_dir

        # Load schemas if directory is provided
        if schema_dir:
            self.load_schemas()

    def _load_schema_file(self, schema_path: str) -> dict[str, Any]:
        """Load a schema file and return the raw data."""
        with open(schema_path, encoding="utf-8") as f:
            if schema_path.endswith(".json"):
                return json.load(f)
            else:  # YAML file
                return yaml.safe_load(f)

    def _load_manifest(self) -> dict[str, Any]:
        """Load the templates.yaml manifest file."""
        manifest_path = os.path.join(self.schema_dir, "templates.yaml")
        if os.path.exists(manifest_path):
            return self._load_schema_file(manifest_path)
        return {}

    def _get_template_version(self, template_name: str) -> str | None:
        """Get the version to use for a template."""
        # First check if a specific version was requested
        if template_name in self.template_versions:
            return self.template_versions[template_name]

        # Then check the manifest for latest version
        if self.manifest and "templates" in self.manifest:
            template_info = self.manifest["templates"].get(template_name, {})
            return template_info.get("latest")

        return None

    def _load_versioned_schemas(self):
        """Load schemas from versioned directory structure (sdrf-templates format)."""
        self.manifest = self._load_manifest()

        # Iterate through template directories
        for template_name in os.listdir(self.schema_dir):
            template_dir = os.path.join(self.schema_dir, template_name)
            if not os.path.isdir(template_dir) or template_name.startswith(".") or template_name == "scripts":
                continue

            # Get the version to load
            version = self._get_template_version(template_name)
            if version is None:
                # If no version specified and no manifest, try to find any version
                version_dirs = [d for d in os.listdir(template_dir) if os.path.isdir(os.path.join(template_dir, d))]
                if version_dirs:
                    version = sorted(version_dirs)[-1]  # Use latest by sorting
                else:
                    continue

            # Load the schema from the versioned directory
            version_dir = os.path.join(template_dir, version)
            schema_file = os.path.join(version_dir, f"{template_name}.yaml")

            if os.path.exists(schema_file):
                self.raw_schema_data[template_name] = self._load_schema_file(schema_file)
                logging.info("Loaded template '%s' version '%s'", template_name, version)

    def _load_flat_schemas(self):
        """Load schemas from flat directory structure (original format)."""
        for filename in os.listdir(self.schema_dir):
            if filename.endswith((".json", ".yaml", ".yml")):
                schema_name = os.path.splitext(filename)[0]
                schema_path = os.path.join(self.schema_dir, filename)
                self.raw_schema_data[schema_name] = self._load_schema_file(schema_path)

    def load_schemas(self):
        """Load all schemas from the schema directory."""
        if not os.path.exists(self.schema_dir):
            raise FileNotFoundError(f"Schema directory not found: {self.schema_dir}")

        # Load raw schema data based on directory structure
        if self.use_versioned:
            self._load_versioned_schemas()
        else:
            self._load_flat_schemas()

        # Process schemas with extension/inheritance
        for schema_name, raw_data in self.raw_schema_data.items():
            processed_schema = self._process_schema_inheritance(schema_name, raw_data)
            schema = SchemaDefinition(**processed_schema)
            self.schemas[schema_name] = schema
            logging.info("Added schema '%s' to registry", schema_name)

    def _process_schema_inheritance(self, schema_name: str, schema_data: dict[str, Any]) -> dict[str, Any]:
        """Process schema inheritance by merging with parent schemas."""
        # Create a copy to avoid modifying the original data
        processed_data = schema_data.copy()

        # If this schema extends another one
        parent_schema_name = schema_data.get("extends")
        if parent_schema_name:
            if parent_schema_name not in self.raw_schema_data:
                raise ValueError(f"Schema '{schema_name}' extends non-existent schema '{parent_schema_name}'")

            # Get the processed parent schema (recursively handling multi-level inheritance)
            parent_schema = self._process_schema_inheritance(
                parent_schema_name, self.raw_schema_data[parent_schema_name]
            )

            # Merge parent and child schemas
            processed_data = self._merge_schemas(parent_schema, processed_data)

        return processed_data

    def _merge_basic_properties(self, result: dict[str, Any], child_schema: dict[str, Any]) -> None:
        for key in ["name", "description"]:
            if key in child_schema:
                result[key] = child_schema[key]

    def _merge_schema_validators(self, result: dict[str, Any], child_schema: dict[str, Any]) -> None:
        """Merge validators from child schema, replacing validators with the same name."""
        if "validators" not in child_schema:
            return

        # Build a map of existing validators by name
        existing_validators = {}
        for i, v in enumerate(result.get("validators", [])):
            existing_validators[v["validator_name"]] = i

        for child_validator in child_schema["validators"]:
            validator_name = child_validator["validator_name"]
            if validator_name in existing_validators:
                # Replace existing validator with child's version
                idx = existing_validators[validator_name]
                result["validators"][idx] = child_validator
            else:
                # Add new validator
                result["validators"].append(child_validator)
                existing_validators[validator_name] = len(result["validators"]) - 1

    def _ensure_column_defaults(self, columns: list[dict[str, Any]]) -> None:
        for col in columns:
            if "allow_not_applicable" not in col:
                col["allow_not_applicable"] = False
            if "allow_not_available" not in col:
                col["allow_not_available"] = False

    def _merge_column_validators(
        self, merged_col: dict[str, Any], parent_col: dict[str, Any], child_col: dict[str, Any]
    ) -> None:
        if "validators" in child_col:
            if "validators" not in merged_col:
                merged_col["validators"] = []

            parent_col_validators = parent_col.get("validators", [])
            child_col_validators = child_col["validators"]
            for validator in child_col_validators + parent_col_validators:
                if validator not in merged_col["validators"]:
                    merged_col["validators"].append(validator)

    def _merge_single_column(self, parent_col: dict[str, Any], child_col: dict[str, Any]) -> dict[str, Any]:
        merged_col = parent_col.copy()
        merged_col.update(child_col)

        for key in ["allow_not_applicable", "allow_not_available"]:
            if key in child_col:
                merged_col[key] = child_col[key]

        self._merge_column_validators(merged_col, parent_col, child_col)
        return merged_col

    def _merge_columns(self, result: dict[str, Any], child_schema: dict[str, Any]) -> None:
        self._ensure_column_defaults(result["columns"])

        if "columns" not in child_schema:
            return

        if "columns" not in result:
            result["columns"] = []

        parent_columns_by_name = {col["name"]: (i, col) for i, col in enumerate(result["columns"])}

        for child_col in child_schema["columns"]:
            col_name = child_col["name"]
            if col_name in parent_columns_by_name:
                idx, parent_col = parent_columns_by_name[col_name]
                merged_col = self._merge_single_column(parent_col, child_col)
                result["columns"][idx] = merged_col
            else:
                result["columns"].append(child_col)

    def _merge_schemas(self, parent_schema: dict[str, Any], child_schema: dict[str, Any]) -> dict[str, Any]:
        # Use deepcopy to avoid mutating the parent schema (important for inheritance chains)
        result = copy.deepcopy(parent_schema)

        self._merge_basic_properties(result, child_schema)
        self._merge_schema_validators(result, child_schema)
        self._merge_columns(result, child_schema)

        result["extends"] = child_schema.get("extends")
        return result

    def add_schema(self, schema_name: str, schema_data: dict[str, Any]):
        """Add a schema to the registry from a dictionary."""
        # Store raw data
        self.raw_schema_data[schema_name] = schema_data

        # Process inheritance if needed
        processed_data = self._process_schema_inheritance(schema_name, schema_data)

        # Create and store the schema
        schema = SchemaDefinition(**processed_data)
        self.schemas[schema_name] = schema
        logging.info("Added schema '%s' to registry", schema_name)

    def get_schema(self, schema_name: str) -> SchemaDefinition | None:
        """Get a schema by name.

        Supports legacy schema names for backwards compatibility:
            - "minimum" → "base"
            - "default" → "ms-proteomics"
            - "cell_lines" → "cell-lines"
            - "nonvertebrates" → "invertebrates"
        """
        # Try direct lookup first
        if schema_name in self.schemas:
            return self.schemas.get(schema_name)

        # Try legacy name mapping
        mapped_name = self.LEGACY_NAME_MAPPING.get(schema_name)
        if mapped_name and mapped_name in self.schemas:
            logging.info("Mapped legacy schema name '%s' to '%s'", schema_name, mapped_name)
            return self.schemas.get(mapped_name)

        return None

    def get_schema_names(self) -> list[str]:
        """Get all schema names in the registry."""
        return list(self.schemas.keys())

    def _categorize_column(self, col_name: str, col: ColumnDefinition, sections: dict[str, OrderedDict]) -> None:
        """Categorize a column into the appropriate section."""
        if col_name.startswith("characteristics["):
            section = "characteristics"
        elif col_name.startswith("comment["):
            section = "comment"
        elif col_name.startswith("factor value["):
            section = "factor value"
        elif col_name.startswith("source name"):
            section = "source name"
        else:
            section = "special"

        if col_name not in sections[section]:
            sections[section][col_name] = []
        sections[section][col_name].append(col)

    def _collect_columns_from_schemas(self, schema_names: list[str], sections: dict[str, OrderedDict]) -> None:
        """Collect columns from all schemas into sections."""
        seen_columns = set()
        for schema_name in schema_names:
            schema = self.get_schema(schema_name)
            if schema:
                for col in schema.columns:
                    if col.name not in seen_columns:
                        seen_columns.add(col.name)
                        self._categorize_column(col.name, col, sections)

    def _merge_section_columns(self, sections: dict[str, OrderedDict], strategy: MergeStrategy) -> None:
        """Merge columns within each section."""
        for section in sections:
            if sections[section]:
                for col_name in sections[section]:
                    if len(sections[section][col_name]) == 1:
                        sections[section][col_name] = sections[section][col_name][0]
                    else:
                        merged_col = merge_column_defs(sections[section][col_name], strategy=strategy)
                        sections[section][col_name] = merged_col

    def _build_ordered_columns(self, sections: dict[str, OrderedDict]) -> list[str]:
        """Build an ordered list of column names from sections."""
        ordered_columns = []
        for section in ["source name", "characteristics", "special", "comment", "factor value"]:
            for col_name in sections[section]:
                ordered_columns.append(col_name)
        return ordered_columns

    def compile_columns_from_schemas(
        self, schema_names: list[str], strategy: MergeStrategy = MergeStrategy.COMBINE
    ) -> tuple[list[str], dict[str, OrderedDict[str, ColumnDefinition]]]:
        """Compile a unique list of columns from multiple schemas.
        Args:
            schema_names: List of schema names to compile columns from
            registry: SchemaRegistry instance containing the schemas
        Returns:
            Tuple of (ordered list of column names, dict of sections with column definitions)
        """
        sections: dict[str, Any] = {
            "source name": OrderedDict(),
            "characteristics": OrderedDict(),
            "special": OrderedDict(),
            "comment": OrderedDict(),
            "factor value": OrderedDict(),
        }

        self._collect_columns_from_schemas(schema_names, sections)
        self._merge_section_columns(sections, strategy)
        ordered_columns = self._build_ordered_columns(sections)

        return ordered_columns, sections

    def combine_schemas(
        self, schema_names: list[str], strategy: MergeStrategy = MergeStrategy.COMBINE
    ) -> SchemaDefinition:
        """
        Combine multiple schemas into one, merging columns and validators.

        Args:
            schema_names: List of schema names to combine
            registry: SchemaRegistry instance containing the schemas
            strategy: Strategy for merging column definitions ('first', 'last', 'combine')
        Returns:
            Combined SchemaDefinition
        """
        if not schema_names:
            raise ValueError("No schema names provided for combination")

        base_schema = self.get_schema(schema_names[0])
        if not base_schema:
            raise ValueError(f"Schema '{schema_names[0]}' not found in registry")

        combined_schema = SchemaDefinition(
            name="CombinedSchema",
            description="Combined schema from: " + ", ".join(schema_names),
            validators=[],
            columns=[],
        )

        # Combine global validators
        existing_validators = {}
        for schema_name in schema_names:
            schema = self.get_schema(schema_name)
            if schema:
                for v in schema.validators:
                    if v.validator_name not in existing_validators:
                        existing_validators[v.validator_name] = v
                        combined_schema.validators.append(v)
            else:
                raise ValueError(f"Schema '{schema_name}' not found in registry")

        # Compile and merge columns
        _, merge_column_definitions = self.compile_columns_from_schemas(schema_names, strategy)
        columns = []
        for section in ["source name", "characteristics", "special", "comment", "factor value"]:
            for col_name in merge_column_definitions[section]:
                columns.append(merge_column_definitions[section][col_name])

        combined_schema.columns = columns

        return combined_schema


class SchemaValidator:
    """Class for validating SDRF data against schemas."""

    def __init__(self, registry: SchemaRegistry):
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

        # Skip ontology validators if requested
        if skip_ontology and validator_name == "ontology":
            logging.debug("Skipping ontology validator as requested by skip_ontology flag")
            return None

        validator_class = get_validator(validator_name)
        if not validator_class:
            # Check if this is an ontology validator and OLS is not available
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
        self, df: pd.DataFrame | SDRFDataFrame, schema: SchemaDefinition, use_ols_cache_only: bool, skip_ontology: bool
    ) -> list[LogicError]:
        errors = []
        # Get required columns for empty_cells validator
        required_columns = [col.name for col in schema.columns if col.requirement == RequirementLevel.REQUIRED]

        for validator_config in schema.validators:
            validator_config.params["use_ols_cache_only"] = use_ols_cache_only
            # Pass required columns to empty_cells validator
            if validator_config.validator_name == "empty_cells":
                validator_config.params["required_columns"] = required_columns
            validator = self._create_validator_instance(validator_config, skip_ontology=skip_ontology)
            if validator:
                errors.extend(validator.validate(df, column_name=None))
        return errors

    def _validate_required_columns(
        self, df: pd.DataFrame | SDRFDataFrame, schema: SchemaDefinition
    ) -> list[LogicError]:
        errors = []
        required_columns = [col.name for col in schema.columns if col.requirement == RequirementLevel.REQUIRED]
        for col_name in required_columns:
            if col_name not in df.columns:
                errors.append(
                    LogicError(
                        message=f"Required column '{col_name}' is missing",
                        error_type=logging.ERROR,
                    )
                )
        return errors

    def _apply_column_validators(
        self,
        column_series: pd.Series,
        column_def: ColumnDefinition,
        use_ols_cache_only: bool,
        skip_ontology: bool,
        debug_col,
    ) -> list[LogicError]:
        errors = []
        for validator_config in column_def.validators:
            validator_config.params["use_ols_cache_only"] = use_ols_cache_only
            validator = self._create_validator_instance(validator_config, skip_ontology=skip_ontology)
            debug_col(f"created validator for column {repr('characteristics[age]')} {repr(validator)}")
            if validator:
                col_errors = validator.validate(column_series, column_name=column_def.name)
                debug_col(f"ERRORS FOUND IN {[e.message for e in col_errors]}")
                errors.extend(col_errors)
        return errors

    def _validate_not_applicable_values(
        self, column_series: pd.Series, column_def: ColumnDefinition
    ) -> list[LogicError]:
        errors = []
        if not column_def.allow_not_applicable:
            str_series = column_series.fillna("").astype(str)
            not_applicable_values = str_series[str_series.str.lower().str.contains(NOT_APPLICABLE)]
            if not not_applicable_values.empty:
                errors.append(
                    LogicError(
                        message=(
                            f"Column '{column_def.name}' contains 'not applicable' values, "
                            "which are not allowed for this column"
                        ),
                        error_type=logging.ERROR,
                    )
                )
        return errors

    def _validate_not_available_values(
        self, column_series: pd.Series, column_def: ColumnDefinition
    ) -> list[LogicError]:
        errors = []
        if not column_def.allow_not_available:
            str_series = column_series.fillna("").astype(str)
            not_available_values = str_series[str_series.str.lower().str.contains(NOT_AVAILABLE)]
            if not not_available_values.empty:
                errors.append(
                    LogicError(
                        message=(
                            f"Column '{column_def.name}' contains 'not available' values, "
                            "which are not allowed for this column"
                        ),
                        error_type=logging.ERROR,
                    )
                )
        return errors

    def _process_column_validation(
        self,
        df: pd.DataFrame | SDRFDataFrame,
        column_def: ColumnDefinition,
        use_ols_cache_only: bool,
        skip_ontology: bool,
        debug_col,
    ) -> list[LogicError]:
        errors = []
        if column_def.name in df.columns:
            logging.debug(f"\nfound column {repr('characteristics[age]')}")
            column_series = df[column_def.name]

            errors.extend(
                self._apply_column_validators(column_series, column_def, use_ols_cache_only, skip_ontology, debug_col)
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

        logging.debug(f"{schema.columns=}")
        logging.debug(f"{df.columns=}")
        col_to_debug = "characteristics[age]"

        def debug_col(msg):
            if column_def.name == col_to_debug:
                print(msg)

        for column_def in schema.columns:
            logging.debug(f"\nprocessing schema column {column_def}")
            errors.extend(self._process_column_validation(df, column_def, use_ols_cache_only, skip_ontology, debug_col))

        return errors

    def validate_with_multiple_schemas(self, df: pd.DataFrame, schema_names: list[str]) -> dict[str, list[LogicError]]:
        """Validate a DataFrame against multiple schemas."""
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
        """Validate a DataFrame and determine the best matching schema."""
        all_results = {}
        schema_scores = {}

        for schema_name in self.registry.get_schema_names():
            errors = self.validate(df, schema_name)
            all_results[schema_name] = errors

            # Calculate a simple score - fewer errors is better
            schema_scores[schema_name] = len(errors)

        if not schema_scores:
            return {"best_schema": None, "errors": [], "all_results": {}}

        # Find schema with lowest error count
        best_schema = min(schema_scores.items(), key=lambda x: x[1])[0]

        return {
            "best_schema": best_schema,
            "errors": all_results[best_schema],
            "all_results": all_results,
        }


def load_and_validate_sdrf(
    sdrf_path: str,
    schema_dir: str | None = None,
    schema_name: str | None = None,
    use_versioned: bool = False,
    template_versions: dict[str, str] | None = None,
):
    """
    Load an SDRF file and validate it against a schema.

    Args:
        sdrf_path: Path to the SDRF file
        schema_dir: Path to directory containing schema files
        schema_name: Name of the schema to use for validation, if None will use best matching schema
        use_versioned: If True, load templates from versioned sdrf-templates structure
        template_versions: Dict mapping template names to specific versions to use

    Returns:
        Tuple of (DataFrame, validation results)
    """
    df = pd.read_csv(sdrf_path, sep="\t")

    # Initialize registry and load schemas
    registry = SchemaRegistry(schema_dir, use_versioned=use_versioned, template_versions=template_versions)
    validator = SchemaValidator(registry)

    if schema_name:
        # Validate against specific schema
        errors = validator.validate(df, schema_name)
        return df, {"schema": schema_name, "errors": errors}
    else:
        # Find best matching schema
        results = validator.validate_with_best_schema(df)
        return df, results


def _merge_fields_first_strategy(merged: ColumnDefinition, col_def: ColumnDefinition) -> None:
    """Merge fields using FIRST strategy - use first non-null value."""
    for field in ["description", "requirement", "allow_not_applicable", "allow_not_available"]:
        if getattr(merged, field) is None and getattr(col_def, field) is not None:
            setattr(merged, field, getattr(col_def, field))


def _merge_fields_last_strategy(merged: ColumnDefinition, col_def: ColumnDefinition) -> None:
    """Merge fields using LAST strategy - use last non-null value."""
    for field in ["description", "requirement", "allow_not_applicable", "allow_not_available"]:
        if getattr(col_def, field) is not None:
            setattr(merged, field, getattr(col_def, field))


def _merge_fields_combine_strategy(merged: ColumnDefinition, col_def: ColumnDefinition) -> None:
    """Merge fields using COMBINE strategy - combine descriptions and use stricter requirement level."""
    # Merge description field
    if getattr(col_def, "description") and getattr(col_def, "description") not in (None, ""):
        if getattr(merged, "description"):
            combined = f"{getattr(merged, 'description')}; {getattr(col_def, 'description')}"
            setattr(merged, "description", combined)
        else:
            setattr(merged, "description", getattr(col_def, "description"))

    # Merge requirement field
    if getattr(col_def, "requirement") and getattr(col_def, "requirement") != RequirementLevel.OPTIONAL:
        setattr(merged, "requirement", getattr(col_def, "requirement"))

    # Merge allow_not_applicable and allow_not_available fields
    for field in ["allow_not_applicable", "allow_not_available"]:
        if getattr(col_def, field):
            setattr(merged, field, True)


def _merge_validators(merged: ColumnDefinition, col_def: ColumnDefinition) -> None:
    """Merge validators without duplicates."""
    existing_validators = {v.validator_name: v for v in merged.validators}
    for v in col_def.validators:
        if v.validator_name not in existing_validators:
            merged.validators.append(v)


def merge_column_defs(
    col_defs: list[ColumnDefinition], strategy: MergeStrategy = MergeStrategy.LAST
) -> ColumnDefinition:
    """
    Merge multiple ColumnDefinition instances into one, combining validators.
    Args:
        col_defs: List of ColumnDefinition instances to merge
        strategy: Strategy for merging basic properties ('first', 'last', 'combine')
    Returns:
        Merged ColumnDefinition
    """

    if not col_defs:
        raise ValueError("No column definitions to merge")

    merged = col_defs[0].copy(deep=True)

    for col_def in col_defs[1:]:
        # Merge basic properties based on strategy
        if strategy == MergeStrategy.FIRST:
            _merge_fields_first_strategy(merged, col_def)
        elif strategy == MergeStrategy.LAST:
            _merge_fields_last_strategy(merged, col_def)
        elif strategy == MergeStrategy.COMBINE:
            _merge_fields_combine_strategy(merged, col_def)

        # Merge validators without duplicates
        _merge_validators(merged, col_def)

    return merged


def schema_to_tsv(schema: SchemaDefinition) -> str:
    """
    Convert sdrf schema definition to TSV format.

    Args:
        schema: SchemaDefinition instance
    Returns:
        TSV string with header line
    """
    header_list = []
    for col in schema.columns:
        header_list.append(col.name)
    tsv_content = "\t".join(header_list) + "\n"
    return tsv_content
