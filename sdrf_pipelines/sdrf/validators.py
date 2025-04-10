import logging
import pandas as pd
from typing import List, Dict, Any, Union, Type, Optional

from pydantic import BaseModel
from sdrf_pipelines.utils.exceptions import LogicError

class SDRFValidator(BaseModel):
    params: Dict[str, Any] = {}

    def __init__(self, params: Dict[str, Any] = None, **data: Any):
        super().__init__(**data)
        if params:
            self.params = params

    def validate(self, value: Union[str, pd.DataFrame, pd.Series, List[str]]) -> List[LogicError]:
        """Validate a value."""
        raise NotImplementedError("Subclasses must implement this method")

# Global validator registry
_VALIDATOR_REGISTRY: Dict[str, Type[SDRFValidator]] = {}

def register_validator(validator_name=None):
    """Register a validator class in the global registry with an explicit type."""

    def decorator(cls):
        nonlocal validator_name

        # If no type is provided, try to get it from the class
        if validator_name is None:
            # Try to access the type attribute
            if hasattr(cls, 'type'):
                validator_name = cls.type
            else:
                # Fallback to class name
                validator_name = cls.__name__.lower()

        # Store the class in the registry
        _VALIDATOR_REGISTRY[validator_name] = cls
        print(f"Registered {cls.__name__} as {validator_name}")

        return cls

    # Allow usage both as @register_validator and @register_validator("type_name")
    if isinstance(validator_name, type) and issubclass(validator_name, SDRFValidator):
        cls = validator_name
        validator_name = None
        return decorator(cls)

    return decorator

def get_validator(validator_type: str) -> Optional[Type[SDRFValidator]]:
    """Get a validator class by type."""
    return _VALIDATOR_REGISTRY.get(validator_type)


def get_all_validators() -> Dict[str, Type[SDRFValidator]]:
    """Get all registered validators."""
    return _VALIDATOR_REGISTRY.copy()

@register_validator(validator_name="trailing_whitespace_validator")
class TrailingWhitespaceValidator(SDRFValidator):

    def validate(self, value: Union[str, pd.DataFrame, pd.Series, List[str]]) -> List[LogicError]:
        """
        This method validates if the provided value contains a TrailingWhiteSpace and return it
        as LogicError. If the column, row information is present, it returns the other.
        """
        errors = []

        if isinstance(value, str):
            if value and value.rstrip() != value:
                errors.append(LogicError(message="Trailing whitespace detected", value=value, error_type=logging.ERROR))

        elif isinstance(value, pd.DataFrame):
            for col in value.columns:
                if value[col].dtype == object:  # Check string columns
                    for idx, cell_value in enumerate(value[col]):
                        if isinstance(cell_value, str) and cell_value and cell_value.rstrip() != cell_value:
                            errors.append(
                                LogicError(
                                    message="Trailing whitespace detected",
                                    value=cell_value,
                                    row=idx,
                                    column=col,
                                    error_type=logging.ERROR,
                                )
                            )

        elif isinstance(value, pd.Series):
            if value.dtype == object:  # Check if Series contains strings
                for idx, cell_value in enumerate(value):
                    if isinstance(cell_value, str) and cell_value and cell_value.rstrip() != cell_value:
                        errors.append(
                            LogicError(
                                message="Trailing whitespace detected",
                                value=cell_value,
                                row=idx,
                                error_type=logging.ERROR,
                            )
                        )

        elif isinstance(value, list):
            for idx, item in enumerate(value):
                if isinstance(item, str) and item and item.rstrip() != item:
                    errors.append(
                        LogicError(
                            message="Trailing whitespace detected", value=item, row=idx, error_type=logging.ERROR
                        )
                    )

        return errors


@register_validator(validator_name="min_columns")
class MinimumColumns(SDRFValidator):
    minimum_columns: int = 12

    def __init__(self, params: Dict[str, Any] = None, **data: Any):
        super().__init__(**data)

        if params:
            for key, value in params.items():
                if key == "min_columns":
                    self.minimum_columns = int(value)

    def validate(self, value: pd.DataFrame) -> List[LogicError]:
        errors = []
        if len(value.columns) < self.minimum_columns:
            errors.append(
                LogicError(
                    message=f"The number of columns is lower than the mandatory number {self.minimum_columns}",
                    error_type=logging.ERROR,
                )
            )
        return errors


@register_validator(validator_name="ontology")
class OntologyValidator(SDRFValidator):

    def validate(self, value: Union[str, pd.DataFrame, pd.Series, List[str]]) -> List[LogicError]:
        errors = []
        ontologies = self.params.get("ontologies", [])
        allow_not_applicable = self.params.get("allow_not_applicable", False)
        allow_not_available = self.params.get("allow_not_available", False)
        description = self.params.get("description", "")

        if isinstance(value, pd.Series):
            for idx, cell_value in enumerate(value):
                if isinstance(cell_value, str):
                    # Skip validation for allowed values if configured
                    if allow_not_applicable and cell_value.lower() == "not applicable":
                        continue
                    if allow_not_available and cell_value.lower() == "not available":
                        continue

                    # Here you would integrate with your ontology validation system
                    # This is a placeholder - in a real implementation you would check against the specified ontologies

        return errors