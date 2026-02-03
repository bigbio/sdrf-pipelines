# validate-sdrf

Command to validate the SDRF file. The validation is based on the template
provided by the user. User can select the template to be used for
validation. If no template is provided, the default template will be used.
Additionally, the mass spectrometry fields and factor values can be
validated separately. However, if the mass spectrometry validation or factor
value validation is skipped, the user will be warned about it.
@param sdrf_file: SDRF file to be validated @param template: template to be
used for a validation @param use_ols_cache_only: flag to use the OLS cache
for validation of the terms and not OLS internet service @param
skip_ontology: flag to skip ontology term validation @param out: Output file
to write the validation results to (default: stdout)

## Usage

```bash
parse_sdrf validate-sdrf [OPTIONS]
```

## Options

| Option | Description |
|--------|-------------|
| `-s, --sdrf_file TEXT` | SDRF file to be validated |
| `-t, --template TEXT` | select the template that will be use to validate the file (default: default) |
| `--use_ols_cache_only` | Use ols cache for validation of the terms and not OLS internet service |
| `--skip-ontology` | Skip ontology term validation (useful when ontology dependencies are not installed) |
| `-o, --out TEXT` | Output file to write the validation results to (default: stdout) |
| `-po, --proof_out TEXT` | Output file to write the validation proof |
| `--generate_proof` | Generate cryptographic proof of validation |
| `--proof_salt TEXT` | Optional user-provided salt for proof generation |
| `-h, --help` | Show this message and exit. |

