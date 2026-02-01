"""Schema registry for loading and managing SDRF schema definitions."""

import copy
import json
import logging
import os
from collections import OrderedDict
from typing import Any

import yaml

from sdrf_pipelines.sdrf.schemas.models import (
    ColumnDefinition,
    MergeStrategy,
    SchemaDefinition,
)
from sdrf_pipelines.sdrf.schemas.utils import merge_column_defs


class SchemaRegistry:
    """Registry for SDRF schema definitions.

    Loads schemas from the sdrf-templates submodule by default. The templates
    follow a versioned directory structure: {template_name}/{version}/{template_name}.yaml

    For backwards compatibility, legacy schema names are mapped to new template names:
        - "minimum" -> "base"
        - "default" -> "ms-proteomics"
        - "cell_lines" -> "cell-lines"
        - "nonvertebrates" -> "invertebrates"
    """

    # Mapping of legacy schema names to new versioned template names
    LEGACY_NAME_MAPPING = {
        "minimum": "base",
        "default": "ms-proteomics",
        "cell_lines": "cell-lines",
        "nonvertebrates": "invertebrates",
    }

    def __init__(
        self,
        schema_dir: str | None = None,
        use_versioned: bool = True,
        template_versions: dict[str, str] | None = None,
    ):
        """Initialize the schema registry.

        Args:
            schema_dir: Path to schema directory. If None, uses default location (sdrf-templates submodule).
            use_versioned: If True (default), load from versioned sdrf-templates structure.
            template_versions: Dict mapping template names to specific versions.
                             If None, uses latest versions from templates.yaml manifest.
        """
        self.schemas: dict[str, SchemaDefinition] = {}
        self.raw_schema_data: dict[str, dict[str, Any]] = {}
        self.use_versioned = use_versioned
        self.template_versions = template_versions or {}
        self.manifest: dict[str, Any] = {}

        if schema_dir is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            # Go up one level from schemas/ to sdrf/, then into sdrf-templates
            schema_dir = os.path.join(os.path.dirname(current_dir), "sdrf-templates")
            if not use_versioned:
                logging.warning(
                    "use_versioned=False is deprecated. "
                    "Templates are now loaded from sdrf-templates submodule by default."
                )
        self.schema_dir = schema_dir

        if schema_dir:
            self.load_schemas()

    def _load_schema_file(self, schema_path: str) -> dict[str, Any]:
        """Load a schema file and return the raw data."""
        with open(schema_path, encoding="utf-8") as f:
            if schema_path.endswith(".json"):
                return json.load(f)
            else:
                return yaml.safe_load(f)

    def _load_manifest(self) -> dict[str, Any]:
        """Load the templates.yaml manifest file."""
        manifest_path = os.path.join(self.schema_dir, "templates.yaml")
        if os.path.exists(manifest_path):
            return self._load_schema_file(manifest_path)
        return {}

    def _get_template_version(self, template_name: str) -> str | None:
        """Get the version to use for a template."""
        if template_name in self.template_versions:
            return self.template_versions[template_name]

        if self.manifest and "templates" in self.manifest:
            template_info = self.manifest["templates"].get(template_name, {})
            return template_info.get("latest")

        return None

    def _load_versioned_schemas(self):
        """Load schemas from versioned directory structure (sdrf-templates format)."""
        self.manifest = self._load_manifest()

        for template_name in os.listdir(self.schema_dir):
            template_dir = os.path.join(self.schema_dir, template_name)
            if not os.path.isdir(template_dir) or template_name.startswith(".") or template_name == "scripts":
                continue

            version = self._get_template_version(template_name)
            if version is None:
                version_dirs = [d for d in os.listdir(template_dir) if os.path.isdir(os.path.join(template_dir, d))]
                if version_dirs:
                    version = sorted(version_dirs)[-1]
                else:
                    continue

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

        if self.use_versioned:
            self._load_versioned_schemas()
        else:
            self._load_flat_schemas()

        for schema_name, raw_data in self.raw_schema_data.items():
            processed_schema = self._process_schema_inheritance(schema_name, raw_data)
            schema = SchemaDefinition(**processed_schema)
            self.schemas[schema_name] = schema
            logging.info("Added schema '%s' to registry", schema_name)

    def _process_schema_inheritance(self, schema_name: str, schema_data: dict[str, Any]) -> dict[str, Any]:
        """Process schema inheritance by merging with parent schemas."""
        processed_data = schema_data.copy()

        parent_schema_name = schema_data.get("extends")
        if parent_schema_name:
            if parent_schema_name not in self.raw_schema_data:
                raise ValueError(f"Schema '{schema_name}' extends non-existent schema '{parent_schema_name}'")

            parent_schema = self._process_schema_inheritance(
                parent_schema_name, self.raw_schema_data[parent_schema_name]
            )
            processed_data = self._merge_schemas(parent_schema, processed_data)

        return processed_data

    def _merge_basic_properties(self, result: dict[str, Any], child_schema: dict[str, Any]) -> None:
        """Merge basic properties from child schema."""
        for key in ["name", "description"]:
            if key in child_schema:
                result[key] = child_schema[key]

    def _merge_schema_validators(self, result: dict[str, Any], child_schema: dict[str, Any]) -> None:
        """Merge validators from child schema, replacing validators with the same name."""
        if "validators" not in child_schema:
            return

        existing_validators = {}
        for i, v in enumerate(result.get("validators", [])):
            existing_validators[v["validator_name"]] = i

        for child_validator in child_schema["validators"]:
            validator_name = child_validator["validator_name"]
            if validator_name in existing_validators:
                idx = existing_validators[validator_name]
                result["validators"][idx] = child_validator
            else:
                result["validators"].append(child_validator)
                existing_validators[validator_name] = len(result["validators"]) - 1

    def _ensure_column_defaults(self, columns: list[dict[str, Any]]) -> None:
        """Ensure columns have default values for allow_not_applicable and allow_not_available."""
        for col in columns:
            if "allow_not_applicable" not in col:
                col["allow_not_applicable"] = False
            if "allow_not_available" not in col:
                col["allow_not_available"] = False

    def _merge_column_validators(
        self, merged_col: dict[str, Any], parent_col: dict[str, Any], child_col: dict[str, Any]
    ) -> None:
        """Merge validators for a single column."""
        if "validators" in child_col:
            if "validators" not in merged_col:
                merged_col["validators"] = []

            parent_col_validators = parent_col.get("validators", [])
            child_col_validators = child_col["validators"]
            for validator in child_col_validators + parent_col_validators:
                if validator not in merged_col["validators"]:
                    merged_col["validators"].append(validator)

    def _merge_single_column(self, parent_col: dict[str, Any], child_col: dict[str, Any]) -> dict[str, Any]:
        """Merge a single column definition from parent and child."""
        merged_col = parent_col.copy()
        merged_col.update(child_col)

        for key in ["allow_not_applicable", "allow_not_available"]:
            if key in child_col:
                merged_col[key] = child_col[key]

        self._merge_column_validators(merged_col, parent_col, child_col)
        return merged_col

    def _merge_columns(self, result: dict[str, Any], child_schema: dict[str, Any]) -> None:
        """Merge columns from child schema into result."""
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
        """Merge parent and child schemas."""
        result = copy.deepcopy(parent_schema)

        self._merge_basic_properties(result, child_schema)
        self._merge_schema_validators(result, child_schema)
        self._merge_columns(result, child_schema)

        result["extends"] = child_schema.get("extends")
        return result

    def add_schema(self, schema_name: str, schema_data: dict[str, Any]):
        """Add a schema to the registry from a dictionary."""
        self.raw_schema_data[schema_name] = schema_data
        processed_data = self._process_schema_inheritance(schema_name, schema_data)
        schema = SchemaDefinition(**processed_data)
        self.schemas[schema_name] = schema
        logging.info("Added schema '%s' to registry", schema_name)

    def get_schema(self, schema_name: str) -> SchemaDefinition | None:
        """Get a schema by name.

        Supports legacy schema names for backwards compatibility.
        """
        if schema_name in self.schemas:
            return self.schemas.get(schema_name)

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
            strategy: Strategy for merging column definitions

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
        """Combine multiple schemas into one, merging columns and validators.

        Args:
            schema_names: List of schema names to combine
            strategy: Strategy for merging column definitions

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

        _, merge_column_definitions = self.compile_columns_from_schemas(schema_names, strategy)
        columns = []
        for section in ["source name", "characteristics", "special", "comment", "factor value"]:
            for col_name in merge_column_definitions[section]:
                columns.append(merge_column_definitions[section][col_name])

        combined_schema.columns = columns

        return combined_schema
