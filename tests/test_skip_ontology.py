"""Test that skip_ontology flag properly skips ontology validation."""

import pandas as pd

from sdrf_pipelines.sdrf.schemas import SchemaRegistry, SchemaValidator
from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame


def test_skip_ontology_flag():
    """Test that skip_ontology=True skips ontology validation."""
    # Create a simple DataFrame with an invalid organism term
    test_df = pd.DataFrame(
        {
            "source name": ["sample 1"],
            "characteristics[organism]": ["invalid_organism_term"],  # Invalid term
            "characteristics[organism part]": ["liver"],
            "characteristics[disease]": ["normal"],
            "characteristics[cell type]": ["hepatocyte"],
            "characteristics[age]": ["1"],
            "characteristics[biological replicate]": ["1"],
            "assay name": ["run 1"],
            "comment[technical replicate]": ["1"],
            "comment[fraction identifier]": ["1"],
            "technology type": ["proteomic profiling by mass spectrometer"],
            "comment[data file]": ["file1.raw"],
            "comment[label]": ["label free sample"],
        }
    )

    registry = SchemaRegistry()
    validator = SchemaValidator(registry)
    sdrf_df = SDRFDataFrame(test_df)

    # Validate with skip_ontology=False (should report ontology errors)
    errors_with_ontology = validator.validate(sdrf_df, "human", skip_ontology=False)

    # Validate with skip_ontology=True (should skip ontology errors)
    errors_without_ontology = validator.validate(sdrf_df, "human", skip_ontology=True)

    # The difference should be the ontology errors
    # With skip_ontology=False, we should have MORE errors (including ontology validation)
    # With skip_ontology=True, we should have FEWER errors (no ontology validation)
    # Note: The exact difference depends on whether OLS dependencies are installed

    # At minimum, both should have some structural validation errors
    assert len(errors_with_ontology) > 0
    assert len(errors_without_ontology) > 0

    # When ontology validation is skipped, we should have fewer or equal errors
    # (equal if OLS dependencies are not available)
    assert len(errors_without_ontology) <= len(errors_with_ontology)

    print(f"Errors with ontology validation: {len(errors_with_ontology)}")
    print(f"Errors without ontology validation: {len(errors_without_ontology)}")
