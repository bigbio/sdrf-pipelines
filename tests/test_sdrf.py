import pandas as pd

from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame
from sdrf_pipelines.utils.utils import ValidationProof


def test_sdrf_parse():
    """Test parsing an SDRF file."""
    # Create a test DataFrame
    test_df = pd.DataFrame(
        {
            "source name": ["sample 1", "sample 2"],
            "characteristics[organism]": ["homo sapiens", "homo sapiens"],
            "characteristics[organism part]": ["liver", "brain"],
            "characteristics[disease]": ["normal", "normal"],
            "characteristics[cell type]": ["hepatocyte", "neuron"],
            "characteristics[biological replicate]": ["1", "2"],
            "assay name": ["run 1", "run 2"],
            "comment[technical replicate]": ["1", "1"],
            "comment[fraction identifier]": ["1", "1"],
            "comment[data file]": ["file1.raw", "file2.raw"],
        }
    )

    # Create SDRFDataFrame
    sdrf = SDRFDataFrame(test_df)

    # Check that the columns are correct
    assert set(sdrf.get_dataframe_columns()) == set(test_df.columns)

    # Check that the shape is correct
    assert sdrf.shape == test_df.shape


def test_generate_validation_proof():
    """Test validation proof generation."""
    test_df = pd.DataFrame(
        {
            "source name": ["sample 1", "sample 2"],
            "characteristics[organism]": ["homo sapiens", "homo sapiens"],
            "characteristics[organism part]": ["liver", "brain"],
            "assay name": ["run 1", "run 2"],
            "comment[data file]": ["file1.raw", "file2.raw"],
        }
    )

    sdrf = SDRFDataFrame(test_df)
    template_content = "name: test\ndescription: test template\ncolumns: []"
    validator_version = "1.0.0"
    validation_proof = ValidationProof()
    proof = validation_proof.generate_validation_proof(sdrf, validator_version, template_content)

    assert "sdrf_hash" in proof
    assert "template_hash" in proof
    assert "validator_version" in proof
    assert "timestamp" in proof
    assert "proof_hash" in proof
    assert "salt_used" in proof

    assert "user_salt" not in proof

    assert proof["salt_used"] is False

    assert proof["validator_version"] == validator_version

    user_salt = "test-salt-123"
    proof_with_salt = validation_proof.generate_validation_proof(sdrf, validator_version, template_content, user_salt)

    assert proof_with_salt["salt_used"] is True

    assert "user_salt" not in proof_with_salt

    proof_different_salt = validation_proof.generate_validation_proof(
        sdrf, validator_version, template_content, "different-salt"
    )
    assert proof_with_salt["proof_hash"] != proof_different_salt["proof_hash"]

    fixed_timestamp = "2024-11-11T11:11:11Z"
    proof_same = validation_proof.generate_validation_proof(
        sdrf, validator_version, template_content, user_salt, fixed_timestamp
    )
    proof_with_salt_fixed = validation_proof.generate_validation_proof(
        sdrf, validator_version, template_content, user_salt, fixed_timestamp
    )
    assert proof_same["proof_hash"] == proof_with_salt_fixed["proof_hash"]

    minimum_validator = ValidationProof(template_name="minimum")
    proof_minimum = minimum_validator.generate_validation_proof(
        sdrf, validator_version, user_salt=user_salt, timestamp=fixed_timestamp
    )
    assert proof_minimum["proof_hash"] != proof_with_salt_fixed["proof_hash"]


def test_validation_proof_deterministic():
    """Test that validation proof generation is deterministic."""
    test_df = pd.DataFrame(
        {
            "source name": ["sample 1"],
            "assay name": ["run 1"],
        }
    )

    sdrf = SDRFDataFrame(test_df)
    template_content = "name: test\ndescription: test template"
    validator_version = "1.0.0"
    user_salt = "consistent-salt"
    validation_proof = ValidationProof()
    proof1 = validation_proof.generate_validation_proof(sdrf, validator_version, template_content, user_salt)
    proof2 = validation_proof.generate_validation_proof(sdrf, validator_version, template_content, user_salt)
    assert proof1["sdrf_hash"] == proof2["sdrf_hash"]
    assert proof1["template_hash"] == proof2["template_hash"]
    assert proof1["validator_version"] == proof2["validator_version"]
    assert proof1["salt_used"] == proof2["salt_used"]


def test_validation_proof_hash_components():
    """Test that changing any component changes the proof hash."""
    base_df = pd.DataFrame({"source name": ["sample 1"], "assay name": ["run 1"]})
    modified_df = pd.DataFrame({"source name": ["sample 2"], "assay name": ["run 1"]})

    base_template = "name: test\ndescription: base template"
    modified_template = "name: test\ndescription: modified template"

    sdrf_base = SDRFDataFrame(base_df)
    sdrf_modified = SDRFDataFrame(modified_df)
    validation_proof = ValidationProof()
    base_proof = validation_proof.generate_validation_proof(sdrf_base, "1.0.0", base_template, "salt")

    modified_sdrf_proof = validation_proof.generate_validation_proof(sdrf_modified, "1.0.0", base_template, "salt")
    assert base_proof["proof_hash"] != modified_sdrf_proof["proof_hash"]

    modified_template_proof = validation_proof.generate_validation_proof(sdrf_base, "1.0.0", modified_template, "salt")
    assert base_proof["proof_hash"] != modified_template_proof["proof_hash"]

    modified_version_proof = validation_proof.generate_validation_proof(sdrf_base, "2.0.0", base_template, "salt")
    assert base_proof["proof_hash"] != modified_version_proof["proof_hash"]

    modified_salt_proof = validation_proof.generate_validation_proof(
        sdrf_base, "1.0.0", base_template, "different-salt"
    )
    assert base_proof["proof_hash"] != modified_salt_proof["proof_hash"]
