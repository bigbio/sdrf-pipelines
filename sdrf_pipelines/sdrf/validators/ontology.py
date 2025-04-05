from typing import Dict, List, Optional

from sdrf_pipelines.ols.ols import OlsClient
from sdrf_pipelines.sdrf.validators.base import SDRFValidator

TERM_NAME = "NT"
NOT_AVAILABLE = "not available"
NOT_APPLICABLE = "not applicable"

# Initialize OLS client
client = OlsClient()


def ontology_term_parser(cell_value: str) -> Dict[str, str]:
    """
    Parse the ontology term from the SDRF cell value.

    Parameters:
        cell_value (str): The cell value containing the ontology term.
    Returns:
        Dict[str, str]: A dictionary containing the parsed ontology term.
    """
    term = {}
    values = cell_value.split(";")
    if len(values) == 1 and "=" not in values[0]:
        term[TERM_NAME] = values[0].lower()
    else:
        for name in values:
            value_terms = name.split("=", 1)
            if len(value_terms) == 1:
                raise ValueError("Not a key-value pair: " + name)
            if "=" in value_terms[1] and value_terms[0].lower() != "cs":
                raise ValueError(
                    f"Invalid term: {name} after splitting by '=', please check the prefix (e.g. AC, NT, TA..)"
                )
            term[value_terms[0].strip().upper()] = value_terms[1].strip().lower()

    return term


class OntologyTermValidator(SDRFValidator):
    """Validator for ontology terms."""

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

        Args:
            value: The value to validate
            ontology_name: The name of the ontology to validate against
            ontologies: List of ontology names to validate against (alternative to ontology_name)
            allow_not_available: Whether to allow "not available" as a valid value
            allow_not_applicable: Whether to allow "not applicable" as a valid value
            use_cache_only: Whether to use only the cache for validation
            description: Description of the validation (for error messages)
            examples: Examples of valid values (for error messages)

        Returns:
            The validated value

        Raises:
            ValueError: If the value is not a valid ontology term
        """
        # Check for special values
        if allow_not_available and value.lower() == NOT_AVAILABLE:
            return value
        if allow_not_applicable and value.lower() == NOT_APPLICABLE:
            return value

        # If both ontology_name and ontologies are provided, use ontologies
        if ontology_name and ontologies:
            ontology_name = None

        # Parse the term
        term = ontology_term_parser(value)

        # If there's no term name, we can't validate
        if TERM_NAME not in term:
            raise ValueError(f"Could not parse term name from: {value}")

        # Search for the term in the ontologies
        found = False
        error_messages = []

        # If ontologies list is provided, search in each ontology
        if ontologies:
            for ontology in ontologies:
                try:
                    ontology_terms = client.search(
                        term[TERM_NAME],
                        ontology=ontology,
                        exact="true",
                        use_ols_cache_only=use_cache_only,
                    )

                    # Check if the term was found
                    if ontology_terms:
                        query_labels = [o["label"].lower() for o in ontology_terms]
                        if term[TERM_NAME] in query_labels:
                            found = True
                            break

                    error_messages.append(f"Term '{term[TERM_NAME]}' not found in ontology {ontology}")
                except Exception as e:
                    error_messages.append(
                        f"Error searching for term '{term[TERM_NAME]}' in ontology {ontology}: {str(e)}"
                    )

        # If ontology_name is provided, search in that ontology
        elif ontology_name:
            try:
                ontology_terms = client.search(
                    term[TERM_NAME],
                    ontology=ontology_name,
                    exact="true",
                    use_ols_cache_only=use_cache_only,
                )

                # Check if the term was found
                if ontology_terms:
                    query_labels = [o["label"].lower() for o in ontology_terms]
                    if term[TERM_NAME] in query_labels:
                        found = True

                if not found:
                    error_messages.append(f"Term '{term[TERM_NAME]}' not found in ontology {ontology_name}")
            except Exception as e:
                error_messages.append(
                    f"Error searching for term '{term[TERM_NAME]}' in ontology {ontology_name}: {str(e)}"
                )

        # If no specific ontology is provided, search in all ontologies
        else:
            try:
                ontology_terms = client.search(term=term[TERM_NAME], exact="true", use_cache_only=use_cache_only)

                # Check if the term was found
                if ontology_terms:
                    query_labels = [o["label"].lower() for o in ontology_terms]
                    if term[TERM_NAME] in query_labels:
                        found = True

                if not found:
                    error_messages.append(f"Term '{term[TERM_NAME]}' not found in any ontology")
            except Exception as e:
                error_messages.append(f"Error searching for term '{term[TERM_NAME]}': {str(e)}")

        # If the term was not found, raise an error
        if not found:
            error_message = "\n".join(error_messages)

            # Add description and examples if provided
            if description:
                error_message = f"{description}\n{error_message}"

            if examples:
                examples_str = ", ".join(examples)
                error_message = f"{error_message}\nExamples of valid values: {examples_str}"

            raise ValueError(error_message)

        return value
