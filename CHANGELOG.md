# History of changes for sdrf-pipelines

## Version 1.0.0 From 0.0.32

### Development
- GitHub action workflow set to use python 3.10 and add mypy and verification of conda recipe into the test suite
- GitHub action workflow remove python 3.9 from testing
- Add isort and mypy into pre-commit hooks
- Usage of poetry for dependency management and packaging

### Major Changes
- Add yaml-defined schemas for validation and schema inheritance with improve support for multiple ontologies
- SchemaRegistry can be used to programmatically use to access all the built-in schemas as well as adding custom schemas
- Remove usage of deprecated pkg_resources
- Add validate-sdrf-simple command to quickly validate a sdrf file
- Add cryptographic proof generation to the `validate-sdrf` command.
- Add new options to several commands, including `convert-openms`, `convert-maxquant`, and `validate-sdrf`.

### Bug Fixes (from v0.0.33)
- Fixed Unimod modification matching bugs in OpenMS module
- Added fallback to match modifications by name when accession lookup fails
- Added `get_by_name()` method to UnimodDatabase for name-based modification lookup
- Updated unimod.xml
- Fixed modification validation in openms_ify_mods method
- Removed debug print statements from OpenMS module
