"""
Tests demonstrating programmatic SDRF validation without using the CLI.

This module shows the recommended way to validate SDRF data directly from Python code.
"""

import logging
from io import StringIO
from pathlib import Path

import pandas as pd

from sdrf_pipelines.sdrf.schemas import SchemaRegistry, SchemaValidator
from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame, read_sdrf

TESTS_DIR = Path(__file__).parent


class TestProgrammaticValidation:
    """Tests demonstrating programmatic SDRF validation."""

    def test_validate_sdrf_from_file(self):
        """Validate an SDRF file using the high-level API."""
        test_file = TESTS_DIR / "data/sample.sdrf.tsv"

        # Using SDRFDataFrame.validate_sdrf() - simplest approach
        sdrf = read_sdrf(test_file)
        errors = sdrf.validate_sdrf(template="default", skip_ontology=True)

        assert isinstance(errors, list)

    def test_validate_sdrf_from_dataframe(self):
        """Validate SDRF data from a pandas DataFrame."""
        df = pd.DataFrame(
            {
                "source name": ["sample_1", "sample_2"],
                "characteristics[organism]": ["Homo sapiens", "Homo sapiens"],
                "characteristics[organism part]": ["liver", "kidney"],
                "characteristics[disease]": ["normal", "normal"],
                "characteristics[cell type]": ["hepatocyte", "epithelial cell"],
                "assay name": ["run_1", "run_2"],
                "technology type": ["mass spectrometry", "mass spectrometry"],
                "comment[data file]": ["file1.raw", "file2.raw"],
                "comment[instrument]": ["Q Exactive", "Q Exactive"],
                "comment[label]": ["label free sample", "label free sample"],
                "comment[fraction identifier]": ["1", "1"],
                "characteristics[biological replicate]": ["1", "2"],
                "comment[technical replicate]": ["1", "1"],
                "comment[proteomics data acquisition method]": ["DDA", "DDA"],
            }
        )

        sdrf = SDRFDataFrame(df)
        errors = sdrf.validate_sdrf(template="default", skip_ontology=True)

        assert isinstance(errors, list)

    def test_validate_with_schema_validator_directly(self):
        """Use SchemaValidator directly for more control."""
        df = pd.DataFrame(
            {
                "source name": ["sample_1"],
                "characteristics[organism]": ["Homo sapiens"],
                "characteristics[organism part]": ["liver"],
                "characteristics[disease]": ["normal"],
                "characteristics[cell type]": ["hepatocyte"],
                "assay name": ["run_1"],
            }
        )

        registry = SchemaRegistry()
        validator = SchemaValidator(registry)

        sdrf = SDRFDataFrame(df)
        errors = validator.validate(sdrf, schema_name="default", skip_ontology=True)

        assert len(errors) > 0
        error_messages = [e.message for e in errors]
        assert any("missing" in msg.lower() for msg in error_messages)

    def test_validate_from_string_content(self):
        """Validate SDRF content provided as a string."""
        sdrf_content = (
            "source name\tcharacteristics[organism]\tcharacteristics[organism part]\t"
            "characteristics[disease]\tcharacteristics[cell type]\tassay name\t"
            "technology type\tcomment[data file]\tcomment[instrument]\tcomment[label]\t"
            "comment[fraction identifier]\tcharacteristics[biological replicate]\t"
            "comment[technical replicate]\tcomment[proteomics data acquisition method]\n"
            "sample_1\tHomo sapiens\tliver\tnormal\thepatocyte\trun_1\t"
            "mass spectrometry\tfile1.raw\tQ Exactive\tlabel free sample\t1\t1\t1\tDDA"
        )

        sdrf = read_sdrf(StringIO(sdrf_content))
        errors = sdrf.validate_sdrf(template="default", skip_ontology=True)

        assert isinstance(errors, list)

    def test_validate_detects_errors(self):
        """Test that validation properly detects common errors."""
        df = pd.DataFrame(
            {
                "source name": ["sample_1"],
                "characteristics[organism]": ["  Homo sapiens  "],  # Whitespace
                "characteristics[organism part]": ["liver"],
                "characteristics[disease]": ["normal"],
                "characteristics[cell type]": ["hepatocyte"],
                "assay name": ["run_1"],
            }
        )

        sdrf = SDRFDataFrame(df)
        errors = sdrf.validate_sdrf(template="default", skip_ontology=True)

        assert len(errors) > 0
        error_errors = [e for e in errors if e.error_type == logging.ERROR]
        assert len(error_errors) > 0

    def test_validate_with_different_templates(self):
        """Test validation with different schema templates."""
        df = pd.DataFrame(
            {
                "source name": ["sample_1"],
                "characteristics[organism]": ["Homo sapiens"],
                "characteristics[organism part]": ["liver"],
                "characteristics[disease]": ["normal"],
                "characteristics[cell type]": ["hepatocyte"],
                "assay name": ["run_1"],
            }
        )

        sdrf = SDRFDataFrame(df)

        for template in ["default", "human", "minimum"]:
            errors = sdrf.validate_sdrf(template=template, skip_ontology=True)
            assert isinstance(errors, list)
