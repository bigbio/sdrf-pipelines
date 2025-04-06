"""
Schema loader for SDRF Pipelines v2.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional, Type

import yaml
from pydantic import BaseModel, create_model

from sdrf_pipelines.sdrf.validators.base import create_field_with_validators
from sdrf_pipelines.utils.exceptions import LogicError


class SchemaField(BaseModel):
    """Schema field definition."""

    name: str
    sdrf_name: str
    description: str
    required: bool = True
    validators: List[Dict[str, Any]] = []


class SchemaDefinition(BaseModel):
    """Schema definition."""

    name: str
    description: str
    extends: Optional[str] = None
    fields: List[SchemaField]
    validators: List[Dict[str, Any]] = []

    @property
    def min_columns(self) -> int:
        """
        Get the minimum number of columns in an Schema

        Returns:
            Minimum number of columns
        """

        return len(self.fields)


class SchemaLoader:
    """Schema loader for SDRF Pipelines v2."""

    def __init__(self, schema_dir: str = None):
        """
        Initialize the schema loader.

        Parameters:
            schema_dir: Directory containing schema files
        """
        if schema_dir is None:
            # Use the default schema directory
            current_dir = os.path.dirname(os.path.abspath(__file__))
            schema_dir = os.path.join(current_dir, "schemas")

        self.schema_dir = schema_dir
        self.schemas: Dict[str, SchemaDefinition] = {}
        self.models: Dict[str, Type[BaseModel]] = {}

        # Load all schemas
        self._load_schemas()

    def _load_schemas(self):
        """Load all schemas from the schema directory."""
        for filename in os.listdir(self.schema_dir):
            if filename.endswith(".json") or filename.endswith(".yaml") or filename.endswith(".yml"):
                schema_name = os.path.splitext(filename)[0]
                schema_path = os.path.join(self.schema_dir, filename)

                with open(schema_path, "r") as f:
                    if filename.endswith(".json"):
                        schema_data = json.load(f)
                    else:  # YAML file
                        schema_data = yaml.safe_load(f)

                schema = SchemaDefinition(**schema_data)
                schema.validators.extend(schema_data.get("validators", []))

                self.schemas[schema_name] = schema

    def get_schema(self, name: str) -> SchemaDefinition:
        """
        Get a schema by name.

        Parameters:
            name: Schema name

        Returns:
            Schema definition

        Raises:
            ValueError: If the schema is not found
        """
        if name not in self.schemas:
            raise ValueError(f"Schema '{name}' not found")

        return self.schemas[name]

    def get_model(self, name: str) -> Type[BaseModel]:
        """
        Get a Pydantic model for a schema.

        Parameters:
            name: Schema name

        Returns:
            Pydantic model

        Raises:
            ValueError: If the schema is not found
        """
        # Check if the model is already created
        if name in self.models:
            return self.models[name]

        # Get the schema
        schema = self.get_schema(name)

        # Get the parent model if the schema extends another schema
        parent_model = None
        if schema.extends:
            parent_model = self.get_model(schema.extends)

        # Create field definitions
        field_definitions = {}
        # Add fields from the parent model
        if parent_model:
            for field_name, field_info in parent_model.model_fields.items():
                field_definitions[field_name] = (field_info.annotation, field_info)

        # Add fields from the schema
        for field in schema.fields:
            # Create field with validators
            validators = {}
            for validator in field.validators:
                validator_type = validator["type"]
                validator_params = validator["params"]

                if validator_type == "whitespace":
                    validators["validate_whitespace"] = True
                elif validator_type == "pattern":
                    validators["pattern"] = validator_params.get("pattern")
                    validators["case_sensitive"] = validator_params.get("case_sensitive", True)
                elif validator_type == "ontology":
                    # Handle both single ontology and multiple ontologies
                    if "ontology_name" in validator_params:
                        validators["ontology"] = validator_params.get("ontology_name")
                    if "ontologies" in validator_params:
                        validators["ontologies"] = validator_params.get("ontologies")

                    # Add other ontology parameters
                    validators["allow_not_available"] = validator_params.get("allow_not_available", False)
                    validators["allow_not_applicable"] = validator_params.get("allow_not_applicable", False)

                    # Add description and examples if provided
                    if "description" in validator_params:
                        validators["ontology_description"] = validator_params.get("description")
                    if "examples" in validator_params:
                        validators["ontology_examples"] = validator_params.get("examples")

            # Print the field name for debugging
            print(f"Adding field: {field.name}")

            # Create the field
            field_info = create_field_with_validators(
                description=field.description, sdrf_name=field.sdrf_name, **validators
            )

            # Add the field to the definitions
            field_type = str
            if not field.required:
                field_type = Optional[str]

            # Make sure we're not overriding a field from the parent model
            if field.name in field_definitions:
                print(f"Warning: Field {field.name} already exists in parent model, overriding")

            field_definitions[field.name] = (field_type, field_info)

        # Create the model
        model_name = f"{name.capitalize()}Record"
        model = create_model(model_name, __doc__=schema.description, **field_definitions)
        model.validators = schema.validators

        # Print the model fields for debugging
        print(f"Model {model_name} fields: {list(model.model_fields.keys())}")

        # Add methods to the model
        def from_dict(cls, data: Dict[str, Any]):
            """
            Create a record from a dictionary.

            Parameters:
                data: Dictionary with record data

            Returns:
                Record instance
            """
            # Convert SDRF column names to model field names
            field_data = {}
            for key, value in data.items():
                key = key.lower()
                if key == "source name":
                    field_name = "source_name"
                elif key.startswith("characteristics["):
                    # Convert 'characteristics[organism]' to 'characteristics_organism'
                    attribute = key.replace("characteristics[", "").replace("]", "")
                    # Replace spaces with underscores in the attribute name
                    attribute = attribute.replace(" ", "_")
                    field_name = f"characteristics_{attribute}"
                elif key.startswith("comment["):
                    # Convert 'comment[data file]' to 'comment_data_file'
                    attribute = key.replace("comment[", "").replace("]", "").replace(" ", "_")
                    field_name = f"comment_{attribute}"
                elif key.startswith("factor value["):
                    # Convert 'factor value[treatment]' to 'factor_value_treatment'
                    attribute = key.replace("factor value[", "").replace("]", "").replace(" ", "_")
                    field_name = f"factor_value_{attribute}"
                else:
                    # Convert 'assay name' to 'assay_name'
                    field_name = key.replace(" ", "_")
                field_data[field_name] = value

            return cls(**field_data)

        def to_dict(self) -> Dict[str, Any]:
            """
            Convert the record to a dictionary with SDRF column names.

            Returns:
                Dictionary with SDRF column names
            """
            data = {}
            for field_name, field_value in self.model_dump().items():
                # Skip None values
                if field_value is None:
                    continue

                # Convert 'source_name' to 'source name'
                if field_name == "source_name":
                    sdrf_name = "source name"
                elif field_name.startswith("characteristics_"):
                    # Convert 'characteristics_organism' to 'characteristics[organism]'
                    attribute = field_name.replace("characteristics_", "")
                    # Replace underscores with spaces in the attribute name
                    attribute = attribute.replace("_", " ")
                    sdrf_name = f"characteristics[{attribute}]"
                elif field_name.startswith("comment_"):
                    # Convert 'comment_data_file' to 'comment[data file]'
                    attribute = field_name.replace("comment_", "").replace("_", " ")
                    sdrf_name = f"comment[{attribute}]"
                elif field_name.startswith("factor_value_"):
                    # Convert 'factor_value_treatment' to 'factor value[treatment]'
                    attribute = field_name.replace("factor_value_", "").replace("_", " ")
                    sdrf_name = f"factor value[{attribute}]"
                else:
                    # Convert 'assay_name' to 'assay name'
                    sdrf_name = field_name.replace("_", " ")

                data[sdrf_name] = field_value

            return data

        # Define the validate_record method
        def validate_record(self, use_ols_cache_only: bool = False) -> List:
            """
            Validate the record.

            Parameters:
                use_ols_cache_only: Whether to use only the cache for ontology validation

            Returns:
                List of validation errors
            """
            errors = []

            # Get the field values
            field_values = self.model_dump()

            # Validate each field
            for field_name, field_value in field_values.items():
                if field_value is None:
                    continue

                # Get the field info
                field_info = self.model_fields.get(field_name)
                if field_info is None:
                    continue

                # Get the validators
                validators = field_info.json_schema_extra.get("validators", {}) if field_info.json_schema_extra else {}

                # Apply each validator
                for validator_name, validator_func in validators.items():
                    try:
                        validator_func(field_value)
                    except ValueError as e:
                        # Convert field_name to SDRF column name for the error message
                        if field_name == "source_name":
                            sdrf_name = "source name"
                        elif field_name.startswith("characteristics_"):
                            attribute = field_name.replace("characteristics_", "").replace("_", " ")
                            sdrf_name = f"characteristics[{attribute}]"
                        elif field_name.startswith("comment_"):
                            attribute = field_name.replace("comment_", "").replace("_", " ")
                            sdrf_name = f"comment[{attribute}]"
                        elif field_name.startswith("factor_value_"):
                            attribute = field_name.replace("factor_value_", "").replace("_", " ")
                            sdrf_name = f"factor value[{attribute}]"
                        else:
                            sdrf_name = field_name.replace("_", " ")

                        error_message = f"Validation error in field '{sdrf_name}': {str(e)}"
                        errors.append(LogicError(error_message, error_type=logging.ERROR))

            return errors

        # Add the methods to the model
        setattr(model, "from_dict", classmethod(from_dict))
        setattr(model, "to_dict", to_dict)
        setattr(model, "validate_record", validate_record)
        setattr(model, "to_dict", to_dict)

        # Store the model
        self.models[name] = model

        return model


# Create a global schema loader
schema_loader = SchemaLoader()
