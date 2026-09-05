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
- DIA-NN converter (`converters/diann/modifications.py`): correctly handle modifications that target
  multiple residues. DIA-NN keeps only the first residue of a comma-separated site list and
  de-duplicates `--var-mod`/`--fixed-mod` entries by name, so the previous output silently dropped
  every residue except the first (e.g. all hydroxyproline `Oxidation` on `P`, and all `pT`/`pY`
  phosphosites). Sites declared in a single SDRF cell (`TA=S,T,Y`) are now concatenated into one
  DIA-NN site string (`Phospho,79.966331,STY`), and the same modification (same name + mass)
  declared across several SDRF cells (`Oxidation` on `M` and on `P`) is merged into a single entry
  (`Oxidation,15.994915,MP`).
- MHCquant converter (`converters/mhcquant/constants.py`): `convert-mhcquant` no longer fails with
  `Unrecognized instrument` for LTQ Orbitrap Elite / Velos deposits (e.g. PXD012083, PXD004746). They now
  fall back to the `qe` preset family rather than `xl`, because `xl` encodes ion-trap MS2 (0.50025 Da)
  and `comment[ms2 mass analyzer]` already downgrades genuine ion-trap runs to low_res.

### Chores
- Update the `sdrf-templates` submodule to the latest `main`.
- Bump `idna` to 3.18 in `uv.lock` (supersedes Dependabot #304, which targeted 3.15).
- CI: fix the Conda Build workflow — install `conda-build`/`conda-verify` into the `base` env and invoke
  via `conda run -n base`, so the `conda build` subcommand is registered (was failing with
  `conda: error: argument COMMAND: invalid choice: 'build'`).
