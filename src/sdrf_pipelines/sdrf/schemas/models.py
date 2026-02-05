"""Pydantic models and enums for SDRF schema definitions."""

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class RequirementLevel(str, Enum):
    """Requirement level for SDRF columns."""

    REQUIRED = "required"
    RECOMMENDED = "recommended"
    OPTIONAL = "optional"


class MergeStrategy(str, Enum):
    """Strategy for merging schema definitions."""

    FIRST = "first"
    LAST = "last"
    COMBINE = "combine"


class ValidatorConfig(BaseModel):
    """Configuration for a validator."""

    validator_name: str
    params: dict[str, Any] = Field(default_factory=dict)


class ColumnDefinition(BaseModel):
    """Definition of an SDRF column with its validation rules."""

    name: str
    description: str
    requirement: RequirementLevel
    allow_not_applicable: bool = False
    allow_not_available: bool = False
    validators: list[ValidatorConfig] = Field(default_factory=list)


class SchemaDefinition(BaseModel):
    """Complete schema definition for SDRF validation."""

    name: str
    description: str
    version: str = Field(default="1.0.0", description="Template version (semver)")
    validators: list[ValidatorConfig] = Field(default_factory=list)
    columns: list[ColumnDefinition] = Field(default_factory=list)
