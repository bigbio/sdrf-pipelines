import os
import json
import yaml
from enum import Enum
from typing import List, Dict, Any, Optional

from pydantic import BaseModel

from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame
from sdrf_pipelines.sdrf.validators import *
from sdrf_pipelines.utils.exceptions import LogicError
_VALIDATOR_REGISTRY: Dict[str, Type[SDRFValidator]] = {}

class RequirementLevel(str, Enum):
    REQUIRED = "required"
    RECOMMENDED = "recommended"
    OPTIONAL = "optional"

class ValidatorConfig(BaseModel):
    validator_name: str
    params: Dict[str, Any] = {}

class ColumnDefinition(BaseModel):
    name: str
    description: str
    requirement: RequirementLevel
    allow_not_applicable: bool = False
    allow_not_available: bool = False
    validators: List[ValidatorConfig] = []

class SchemaDefinition(BaseModel):
    name: str
    description: str
    validators: List[ValidatorConfig] = []
    columns: List[ColumnDefinition] = []

class SchemaRegistry:

    def __init__(self, schema_dir: Optional[str] = None):
        self.schemas: Dict[str, SchemaDefinition] = {}
        self.raw_schema_data: Dict[str, Dict[str, Any]] = (
            {}
        )  # Store raw schema data for inheritance resolution
        if schema_dir is None:
            # Use the default schema directory
            current_dir = os.path.dirname(os.path.abspath(__file__))
            schema_dir = os.path.join(current_dir, "schemas")
        self.schema_dir = schema_dir

        # Load schemas if directory is provided
        if schema_dir:
            self.load_schemas()

    def _load_schema_file(self, schema_path: str) -> Dict[str, Any]:
        """Load a schema file and return the raw data."""
        with open(schema_path, "r") as f:
            if schema_path.endswith(".json"):
                return json.load(f)
            else:  # YAML file
                return yaml.safe_load(f)

    def load_schemas(self):
        """Load all schemas from the schema directory."""
        if not os.path.exists(self.schema_dir):
            raise FileNotFoundError(f"Schema directory not found: {self.schema_dir}")

        # First pass: Load all raw schema data
        for filename in os.listdir(self.schema_dir):
            if filename.endswith((".json", ".yaml", ".yml")):
                schema_name = os.path.splitext(filename)[0]
                schema_path = os.path.join(self.schema_dir, filename)
                self.raw_schema_data[schema_name] = self._load_schema_file(schema_path)

        # Second pass: Process schemas with extension/inheritance
        for schema_name, raw_data in self.raw_schema_data.items():
            processed_schema = self._process_schema_inheritance(schema_name, raw_data)
            schema = SchemaDefinition(**processed_schema)
            self.schemas[schema_name] = schema
            logging.info(f"Added schema '{schema_name}' to registry")

    def _process_schema_inheritance(
        self, schema_name: str, schema_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process schema inheritance by merging with parent schemas."""
        # Create a copy to avoid modifying the original data
        processed_data = schema_data.copy()

        # If this schema extends another one
        parent_schema_name = schema_data.get("extends")
        if parent_schema_name:
            if parent_schema_name not in self.raw_schema_data:
                raise ValueError(
                    f"Schema '{schema_name}' extends non-existent schema '{parent_schema_name}'"
                )

            # Get the processed parent schema (recursively handling multi-level inheritance)
            parent_schema = self._process_schema_inheritance(
                parent_schema_name, self.raw_schema_data[parent_schema_name]
            )

            # Merge parent and child schemas
            processed_data = self._merge_schemas(parent_schema, processed_data)

        return processed_data

    def _merge_schemas(
        self, parent_schema: Dict[str, Any], child_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Merge a child schema with its parent schema."""
        result = parent_schema.copy()

        # Override basic properties with child values
        for key in ["name", "description"]:
            if key in child_schema:
                result[key] = child_schema[key]

        # Merge validators (append child validators to parent validators)
        if "validators" in child_schema:
            if "validators" not in result:
                result["validators"] = []
            result["validators"].extend(child_schema["validators"])

        # Merge columns:
        # Add allow_not_applicable and allow_not_available to columns if not present
        for col in result["columns"]:
            if "allow_not_applicable" not in col:
                col["allow_not_applicable"] = False
            if "allow_not_available" not in col:
                col["allow_not_available"] = False
        # 1. Keep all parent columns that aren't overridden by child
        # 2. Add child columns, overriding parent columns with the same name
        if "columns" in child_schema:
            if "columns" not in result:
                result["columns"] = []

            # Create a lookup of columns by name for faster access
            parent_columns_by_name = {
                col["name"]: (i, col) for i, col in enumerate(result["columns"])
            }

            for child_col in child_schema["columns"]:
                col_name = child_col["name"]
                if col_name in parent_columns_by_name:
                    # Override existing column
                    idx, parent_col = parent_columns_by_name[col_name]
                    merged_col = parent_col.copy()
                    merged_col.update(child_col)

                    # Handle allow_not_applicable and allow_not_available
                    if "allow_not_applicable" in child_col:
                        merged_col["allow_not_applicable"] = child_col["allow_not_applicable"]
                    if "allow_not_available" in child_col:
                        merged_col["allow_not_available"] = child_col["allow_not_available"]

                    # Special handling for validators - append child validators to parent validators
                    if "validators" in child_col:
                        if "validators" not in merged_col:
                            merged_col["validators"] = []
                        parent_validators = parent_col.get("validators", [])
                        # Replace parent validators with merged ones
                        merged_col["validators"] = (
                            parent_validators + child_col["validators"]
                        )

                    result["columns"][idx] = merged_col
                else:
                    # Add new column
                    result["columns"].append(child_col)

        # Preserve extends field for reference but it's not needed for processing anymore
        result["extends"] = child_schema.get("extends")

        return result

    def add_schema(self, schema_name: str, schema_data: Dict[str, Any]):
        """Add a schema to the registry from a dictionary."""
        # Store raw data
        self.raw_schema_data[schema_name] = schema_data

        # Process inheritance if needed
        processed_data = self._process_schema_inheritance(schema_name, schema_data)

        # Create and store the schema
        schema = SchemaDefinition(**processed_data)
        self.schemas[schema_name] = schema
        logging.info(f"Added schema '{schema_name}' to registry")

    def get_schema(self, schema_name: str) -> Optional[SchemaDefinition]:
        """Get a schema by name."""
        return self.schemas.get(schema_name)

    def get_schema_names(self) -> List[str]:
        """Get all schema names in the registry."""
        return list(self.schemas.keys())

class SchemaValidator:
    """Class for validating SDRF data against schemas."""

    def __init__(self, registry: SchemaRegistry):
        self.registry = registry

    def _create_validator_instance(
        self, validator_config: ValidatorConfig
    ) -> Optional[SDRFValidator]:
        """Create a validator instance from a configuration."""
        validator_name = validator_config.validator_name
        validator_params = validator_config.params

        validator_class = get_validator(validator_name)
        if not validator_class:
            logging.warning(f"Validator type '{validator_name}' not found in registry")
            return None

        return validator_class(params=validator_params)

    def validate(
        self, df: Union[pd.DataFrame, SDRFDataFrame], schema_name: str
    ) -> List[LogicError]:
        """Validate a DataFrame against a schema."""
        schema = self.registry.get_schema(schema_name)
        if not schema:
            raise ValueError(f"Schema '{schema_name}' not found in registry")

        errors = []

        if isinstance(df, SDRFDataFrame):
            df = df.df

        # Apply global validators
        for validator_config in schema.validators:
            validator = self._create_validator_instance(validator_config)
            if validator:
                errors.extend(validator.validate(df))

        # Validate required columns exist
        required_columns = [
            col.name
            for col in schema.columns
            if col.requirement == RequirementLevel.REQUIRED
        ]
        for col_name in required_columns:
            if col_name not in df.columns:
                errors.append(
                    LogicError(
                        message=f"Required column '{col_name}' is missing",
                        error_type=logging.ERROR,
                    )
                )

        # Apply column-specific validators
        for column_def in schema.columns:
            if column_def.name in df.columns:
                column_series = df[column_def.name]

                # Apply specific validators defined for this column
                for validator_config in column_def.validators:
                    validator = self._create_validator_instance(validator_config)
                    if validator:
                        errors.extend(validator.validate(column_series))

        return errors

    def validate_with_multiple_schemas(
        self, df: pd.DataFrame, schema_names: List[str]
    ) -> Dict[str, List[LogicError]]:
        """Validate a DataFrame against multiple schemas."""
        results = {}
        for schema_name in schema_names:
            try:
                errors = self.validate(df, schema_name)
                results[schema_name] = errors
            except Exception as e:
                logging.error(
                    f"Error validating against schema '{schema_name}': {str(e)}"
                )
                results[schema_name] = [
                    LogicError(
                        message=f"Validation error: {str(e)}", error_type=logging.ERROR
                    )
                ]

        return results

    def validate_with_best_schema(self, df: pd.DataFrame) -> Dict[str, Any]:
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
    sdrf_path: str, schema_dir: str = None, schema_name: str = None
):
    """
    Load an SDRF file and validate it against a schema.

    Args:
        sdrf_path: Path to the SDRF file
        schema_dir: Path to directory containing schema files
        schema_name: Name of the schema to use for validation, if None will use best matching schema

    Returns:
        Tuple of (DataFrame, validation results)
    """
    df = pd.read_csv(sdrf_path, sep="\t")

    # Initialize registry and load schemas
    registry = SchemaRegistry(schema_dir)
    validator = SchemaValidator(registry)

    if schema_name:
        # Validate against specific schema
        errors = validator.validate(df, schema_name)
        return df, {"schema": schema_name, "errors": errors}
    else:
        # Find best matching schema
        results = validator.validate_with_best_schema(df)
        return df, results