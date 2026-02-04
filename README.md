# sdrf-pipelines

[![PyPI version](https://badge.fury.io/py/sdrf-pipelines.svg)](https://badge.fury.io/py/sdrf-pipelines)
![PyPI - Downloads](https://img.shields.io/pypi/dm/sdrf-pipelines)
[![CI](https://github.com/bigbio/sdrf-pipelines/actions/workflows/ci.yml/badge.svg)](https://github.com/bigbio/sdrf-pipelines/actions/workflows/ci.yml)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/ebf85b7ad8304422ab495c3f720bf3ae)](https://www.codacy.com/gh/bigbio/sdrf-pipelines/dashboard)

The official **SDRF-Proteomics validator and converter**. Validate your sample metadata files and convert them to workflow configurations for OpenMS, MaxQuant, MSstats, and more.

## Quick Start with sdrf as a command/line tool

Install python and [pipx](https://pipx.pypa.io/stable/installation/) **or** just [uv](https://docs.astral.sh/uv/getting-started/installation/)

uv:

```bash
# Install
uv tool install sdrf-pipelines[all]

# Validate your SDRF file
parse_sdrf validate-sdrf --sdrf_file your_file.sdrf.tsv
```

pipx:

```bash
# Install
pipx install sdrf-pipelines[all]

# Validate your SDRF file
parse_sdrf validate-sdrf --sdrf_file your_file.sdrf.tsv
```

That's it! Your SDRF file will be validated against the default mass spectrometry template.

## Installation

| Installation | Command | Features |
|-------------|---------|----------|
| **Basic** | `pip install sdrf-pipelines` | Structural validation + all converters |
| **Full** | `pip install sdrf-pipelines[ontology]` | + Ontology term validation (EFO, CL, MS, etc.) |

The basic installation validates column structure, formatting, and uniqueness. Add `[ontology]` to also validate that terms exist in their respective ontologies.

## Usage

For detailed command documentation, see **[COMMANDS.md](COMMANDS.md)** or use the built-in help:

```bash
parse_sdrf --help
parse_sdrf validate-sdrf --help
```

### Validation Examples

```bash
# Basic validation
parse_sdrf validate-sdrf --sdrf_file sample.sdrf.tsv

# Validate with a specific template
parse_sdrf validate-sdrf --sdrf_file sample.sdrf.tsv --template human

# Skip ontology validation for quick structural checks
parse_sdrf validate-sdrf --sdrf_file sample.sdrf.tsv --skip-ontology
```

### Converter Examples

```bash
# Convert to OpenMS format
parse_sdrf convert-openms -s sdrf.tsv

# Convert to MaxQuant format
parse_sdrf convert-maxquant -s sdrf.tsv -f database.fasta -r /path/to/raw/files

# Convert to MSstats annotation
parse_sdrf convert-msstats -s sdrf.tsv -o annotation.csv
```

## Development

We use modern Python tooling for development:

### Package Manager: uv

We use [uv](https://docs.astral.sh/uv/) as our package manager for fast, reliable dependency management.

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv sync

# Install with dev dependencies
uv sync --group dev
```

### Code Quality: ruff + pre-commit

We use [ruff](https://docs.astral.sh/ruff/) for linting and formatting, and [pre-commit](https://pre-commit.com/) for automated checks.

```bash
# Install pre-commit hooks
uv run pre-commit install

# Run all checks manually
uv run pre-commit run --all-files
```

### Running Tests

```bash
uv run pytest
```

For more details, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Citation

If you use this software, please cite:

> Dai C, Füllgrabe A, Pfeuffer J, et al. A proteomics sample metadata representation for multiomics integration and big data analysis. _Nat Commun_ **12**, 5854 (2021). https://doi.org/10.1038/s41467-021-26111-3

For full citation details and BibTeX format, see [CITATION.cff](CITATION.cff).
