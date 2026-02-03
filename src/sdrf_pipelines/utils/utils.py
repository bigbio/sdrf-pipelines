import hashlib
import json
from datetime import datetime
from typing import Optional

import yaml

from sdrf_pipelines.sdrf.schemas import SchemaRegistry
from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame


def tsv_line(*value_list: str) -> str:
    """Compose a tab separated value line ending with a newline.

    All arguments supplied will be joined by tabs and the line completed with a newline.
    """
    return "\t".join(value_list) + "\n"


class ValidationProof:
    def __init__(self, schema_registry: Optional[SchemaRegistry] = None, template_name: Optional[str] = None):
        self.template_name = template_name
        self.schema_registry = schema_registry
        if self.schema_registry is None:
            self.schema_registry = SchemaRegistry()

    def generate_validation_proof(
        self,
        sdrf_df: SDRFDataFrame,
        validator_version: str,
        template_content: Optional[str] = None,
        user_salt: Optional[str] = None,
        timestamp: Optional[str] = None,
    ) -> dict:
        """
        Generate a cryptographic proof of validation.

        Args:
            sdrf_df: The SDRF dataframe that was validated
            template_content: Content of the YAML template used for validation
            validator_version: Version of the validator package
            user_salt: Optional user-provided string for additional uniqueness
            timestamp: Optional timestamp string (ISO format). If None, current UTC time is used.

        Returns:
            dict: Validation proof containing hashes and metadata
        """
        sdrf_normalized = sdrf_df.df.to_csv(index=False, sep="\t").encode("utf-8")
        sdrf_hash = hashlib.sha512(sdrf_normalized).hexdigest()
        if not template_content:
            if not self.template_name:
                raise ValueError("Either template_content or template_name must be provided")
            if not self.schema_registry:
                raise ValueError("No schema registry available to retrieve template content")
            # Resolve legacy name if needed
            actual_name = self.template_name
            if actual_name in self.schema_registry.LEGACY_NAME_MAPPING:
                actual_name = self.schema_registry.LEGACY_NAME_MAPPING[actual_name]
            if actual_name not in self.schema_registry.raw_schema_data:
                available = list(self.schema_registry.raw_schema_data.keys())
                raise ValueError(
                    f"Template '{self.template_name}' not found in schema registry. Available templates: {available}"
                )
            template_content = yaml.dump(self.schema_registry.raw_schema_data[actual_name], sort_keys=True)
        template_hash = hashlib.sha512(template_content.encode("utf-8")).hexdigest()

        proof_timestamp = timestamp if timestamp is not None else datetime.utcnow().isoformat() + "Z"
        proof_data_for_hash = {
            "sdrf_hash": sdrf_hash,
            "template_hash": template_hash,
            "validator_version": validator_version,
            "timestamp": proof_timestamp,
            "user_salt": user_salt or "",
        }
        proof_string = json.dumps(proof_data_for_hash, sort_keys=True)
        proof_hash = hashlib.sha512(proof_string.encode("utf-8")).hexdigest()

        return {
            "sdrf_hash": sdrf_hash,
            "template_hash": template_hash,
            "validator_version": validator_version,
            "timestamp": proof_timestamp,
            "proof_hash": proof_hash,
            "salt_used": bool(user_salt),
        }
