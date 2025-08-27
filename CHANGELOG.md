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
