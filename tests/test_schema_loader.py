"""
Tests for the schema loader.
"""

import json
import os
import tempfile

from sdrf_pipelines.sdrf.schema_loader import schema_loader, SchemaLoader


def test_schema_loader():
    """Test the schema loader."""
    # Check that the default schema loader is initialized
    assert schema_loader is not None

    # Check that the default schemas are loaded
    assert "default" in schema_loader.schemas
    assert "human" in schema_loader.schemas

    # Check that the models are created
    default_model = schema_loader.get_model("default")
    human_model = schema_loader.get_model("human")

    assert default_model is not None
    assert human_model is not None

    # Check that the human model extends the default model
    assert schema_loader.schemas["human"].extends == "default"


def test_custom_schema():
    """Test loading a custom schema."""
    # Create a temporary directory for the schema
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a custom schema
        custom_schema = {
            "name": "custom",
            "description": "Custom schema for testing",
            "fields": [
                {
                    "name": "source_name",
                    "sdrf_name": "source name",
                    "description": "Source name",
                    "required": True,
                    "validators": [{"type": "whitespace", "params": {}}],
                },
                {
                    "name": "custom_field",
                    "sdrf_name": "custom field",
                    "description": "Custom field",
                    "required": True,
                    "validators": [{"type": "whitespace", "params": {}}],
                },
            ],
        }

        # Write the schema to a file
        schema_path = os.path.join(temp_dir, "custom.json")
        with open(schema_path, "w") as f:
            json.dump(custom_schema, f)

        # Create a schema loader with the custom schema
        loader = SchemaLoader(temp_dir)

        # Check that the schema is loaded
        assert "custom" in loader.schemas

        # Get the model
        custom_model = loader.get_model("custom")

        # Check that the model has the custom field
        assert "custom_field" in custom_model.model_fields

        # Create a record
        record = custom_model(source_name="sample 1", custom_field="value")

        # Check that the record has the right values
        assert record.source_name == "sample 1"
        assert record.custom_field == "value"

        # Convert to dict
        record_dict = record.to_dict()

        # Check that the dict has the right keys
        assert "source name" in record_dict
        assert "custom field" in record_dict

        # Check that the dict has the right values
        assert record_dict["source name"] == "sample 1"
        assert record_dict["custom field"] == "value"


def test_schema_inheritance():
    """Test schema inheritance."""
    # Create a temporary directory for the schema
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a base schema
        base_schema = {
            "name": "base",
            "description": "Base schema for testing",
            "fields": [
                {
                    "name": "source_name",
                    "sdrf_name": "source name",
                    "description": "Source name",
                    "required": True,
                    "validators": [{"type": "whitespace", "params": {}}],
                },
                {
                    "name": "base_field",
                    "sdrf_name": "base field",
                    "description": "Base field",
                    "required": True,
                    "validators": [{"type": "whitespace", "params": {}}],
                },
            ],
        }

        # Create a derived schema
        derived_schema = {
            "name": "derived",
            "description": "Derived schema for testing",
            "extends": "base",
            "fields": [
                {
                    "name": "derived_field",
                    "sdrf_name": "derived field",
                    "description": "Derived field",
                    "required": True,
                    "validators": [{"type": "whitespace", "params": {}}],
                }
            ],
        }

        # Write the schemas to files
        base_path = os.path.join(temp_dir, "base.json")
        with open(base_path, "w") as f:
            json.dump(base_schema, f)

        derived_path = os.path.join(temp_dir, "derived.json")
        with open(derived_path, "w") as f:
            json.dump(derived_schema, f)

        # Create a schema loader with the custom schemas
        loader = SchemaLoader(temp_dir)

        # Check that the schemas are loaded
        assert "base" in loader.schemas
        assert "derived" in loader.schemas

        # Get the models
        base_model = loader.get_model("base")
        derived_model = loader.get_model("derived")

        # Check that the models have the right fields
        assert "source_name" in base_model.model_fields
        assert "base_field" in base_model.model_fields
        assert "derived_field" not in base_model.model_fields

        assert "source_name" in derived_model.model_fields
        assert "base_field" in derived_model.model_fields
        assert "derived_field" in derived_model.model_fields

        # Create a derived record
        record = derived_model(source_name="sample 1", base_field="base value", derived_field="derived value")

        # Check that the record has the right values
        assert record.source_name == "sample 1"
        assert record.base_field == "base value"
        assert record.derived_field == "derived value"

        # Convert to dict
        record_dict = record.to_dict()

        # Check that the dict has the right keys
        assert "source name" in record_dict
        assert "base field" in record_dict
        assert "derived field" in record_dict

        # Check that the dict has the right values
        assert record_dict["source name"] == "sample 1"
        assert record_dict["base field"] == "base value"
        assert record_dict["derived field"] == "derived value"
