import re
from typing import List, Optional

from pydantic import Field
from pydantic.fields import FieldInfo


class SDRFValidator:
    """Base class for SDRF validators."""

    @staticmethod
    def validate_whitespace(value: str) -> str:
        """Validate that there is no leading or trailing whitespace."""
        if value != value.strip():
            raise ValueError("Value contains leading or trailing whitespace")
        return value

    @staticmethod
    def validate_pattern(value: str, pattern: str, case_sensitive: bool = True) -> str:
        """Validate that the value matches a regex pattern."""
        flags = 0 if case_sensitive else re.IGNORECASE
        if not re.match(pattern, value, flags):
            raise ValueError(f"Value does not match pattern: {pattern}")
        return value

    @staticmethod
    def validate_ontology_term(
        value: str,
        ontology_name: Optional[str] = None,
        ontologies: Optional[List[str]] = None,
        allow_not_available: bool = False,
        allow_not_applicable: bool = False,
        use_cache_only: bool = False,
        description: Optional[str] = None,
        examples: Optional[List[str]] = None,
    ) -> str:
        """
        Validate that the value is a valid ontology term.

        This is a placeholder for the actual implementation that will use the OLS client.
        """
        # This will be implemented later with the actual OLS client
        # For now, we'll just return the value
        return value


def create_field_with_validators(
    *,
    description: str,
    validate_whitespace: bool = True,
    pattern: Optional[str] = None,
    case_sensitive: bool = True,
    ontology: Optional[str] = None,
    ontologies: Optional[List[str]] = None,
    allow_not_available: bool = False,
    allow_not_applicable: bool = False,
    ontology_description: Optional[str] = None,
    ontology_examples: Optional[List[str]] = None,
    **kwargs,
) -> FieldInfo:
    """
    Create a Field with the specified validators.

    Args:
        description: Field description
        validate_whitespace: Whether to validate whitespace
        pattern: Regex pattern to validate against
        case_sensitive: Whether the pattern matching is case sensitive
        ontology: Ontology name to validate against
        ontologies: List of ontology names to validate against (alternative to ontology)
        allow_not_available: Whether to allow "not available" as a valid value
        allow_not_applicable: Whether to allow "not applicable" as a valid value
        ontology_description: Description of the ontology validation (for error messages)
        ontology_examples: Examples of valid ontology terms (for error messages)
        **kwargs: Additional arguments to pass to Field

    Returns:
        A Field with the specified validators
    """
    validators = {}

    if validate_whitespace:
        validators["whitespace"] = SDRFValidator.validate_whitespace

    if pattern:
        validators["pattern"] = lambda v: SDRFValidator.validate_pattern(v, pattern, case_sensitive)

    if ontology or ontologies:
        validators["ontology"] = lambda v: SDRFValidator.validate_ontology_term(
            v,
            ontology_name=ontology,
            ontologies=ontologies,
            allow_not_available=allow_not_available,
            allow_not_applicable=allow_not_applicable,
            description=ontology_description,
            examples=ontology_examples,
        )

    return Field(description=description, json_schema_extra={"validators": validators}, **kwargs)
