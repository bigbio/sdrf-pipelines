"""Regression tests for schema loading."""

import pytest

from sdrf_pipelines.sdrf.schemas import SchemaRegistry, TemplatesNotFoundError
from sdrf_pipelines.sdrf.schemas import registry as registry_module


def test_schema_registry_normalizes_null_validator_params():
    """Templates with validators that omit params should still load cleanly."""
    registry = SchemaRegistry()

    assert "ms-metabolomics" in registry.schemas

    has_empty_params = any(
        validator.params == {}
        for schema in registry.schemas.values()
        for column in schema.columns
        for validator in column.validators
    )
    assert has_empty_params


def test_missing_templates_directory_raises_clear_error(tmp_path, monkeypatch):
    """Without the sdrf-templates submodule the default registry must say what is missing."""
    missing = tmp_path / "sdrf-templates"
    monkeypatch.setattr(registry_module, "default_schema_dir", lambda: str(missing))

    with pytest.raises(TemplatesNotFoundError, match="git submodule update --init"):
        SchemaRegistry()


def test_empty_templates_directory_raises_clear_error(tmp_path, monkeypatch):
    """An uninitialised submodule leaves an empty directory; that must not load silently."""
    empty = tmp_path / "sdrf-templates"
    empty.mkdir()
    monkeypatch.setattr(registry_module, "default_schema_dir", lambda: str(empty))

    with pytest.raises(TemplatesNotFoundError, match="--recurse-submodules"):
        SchemaRegistry()


def test_templates_not_found_error_is_a_file_not_found_error(tmp_path, monkeypatch):
    """Callers that already catch FileNotFoundError keep working."""
    monkeypatch.setattr(registry_module, "default_schema_dir", lambda: str(tmp_path / "missing"))

    with pytest.raises(FileNotFoundError):
        SchemaRegistry()


def test_custom_empty_schema_dir_is_allowed(tmp_path):
    """A user-supplied schema_dir may be empty; schemas can be added programmatically."""
    registry = SchemaRegistry(schema_dir=str(tmp_path))

    assert registry.get_schema_names() == []


def test_custom_missing_schema_dir_keeps_plain_error(tmp_path):
    """A missing user-supplied schema_dir raises the generic error, not the submodule hint."""
    with pytest.raises(FileNotFoundError, match="Schema directory not found") as excinfo:
        SchemaRegistry(schema_dir=str(tmp_path / "missing"))

    assert not isinstance(excinfo.value, TemplatesNotFoundError)
