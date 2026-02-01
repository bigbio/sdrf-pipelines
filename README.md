# sdrf-pipelines

[![PyPI version](https://badge.fury.io/py/sdrf-pipelines.svg)](https://badge.fury.io/py/sdrf-pipelines)
![PyPI - Downloads](https://img.shields.io/pypi/dm/sdrf-pipelines)
![Python application](https://github.com/bigbio/sdrf-pipelines/workflows/Python%20application/badge.svg)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/ebf85b7ad8304422ab495c3f720bf3ae)](https://www.codacy.com/gh/bigbio/sdrf-pipelines/dashboard)

The official **SDRF-Proteomics validator and converter**. Validate your sample metadata files and convert them to workflow configurations for OpenMS, MaxQuant, MSstats, and more.

## Quick Start

```bash
# Install
pip install sdrf-pipelines

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

## Validation

### Basic Validation

```bash
parse_sdrf validate-sdrf --sdrf_file sample.sdrf.tsv
```

### Using Templates

Templates define validation rules for specific experiment types. Use `--template` to apply additional rules:

```bash
# Human samples - adds ancestry, disease validation
parse_sdrf validate-sdrf --sdrf_file sample.sdrf.tsv --template human

# Cell line experiments
parse_sdrf validate-sdrf --sdrf_file sample.sdrf.tsv --template cell-lines

# Multiple templates (comma-separated)
parse_sdrf validate-sdrf --sdrf_file sample.sdrf.tsv --template human,cell-lines
```

**Available templates:**

| Template | Description |
|----------|-------------|
| `ms-proteomics` | Mass spectrometry proteomics (default) |
| `affinity-proteomics` | Olink, SomaScan experiments |
| `human` | Human samples (adds ancestry, disease) |
| `vertebrates` | Vertebrate samples |
| `invertebrates` | Invertebrate samples |
| `plants` | Plant samples |
| `cell-lines` | Cell line experiments |

### Skip Ontology Validation

For quick structural checks without ontology lookups:

```bash
parse_sdrf validate-sdrf --sdrf_file sample.sdrf.tsv --skip-ontology
```

### Validation Output

The validator reports errors and warnings:

```
ERROR: Column 'characteristics[organism]' value 'human' not found in NCBI Taxonomy
WARNING: Column 'characteristics[cell type]' - consider using ontology term
```

- **Errors** must be fixed for a valid SDRF
- **Warnings** are recommendations that won't fail validation

### Ontology Cache

When using ontology validation, terms are cached locally for faster subsequent runs:

```bash
# Pre-download all ontology caches (optional)
parse_sdrf download-cache

# Download specific ontologies
parse_sdrf download-cache -o efo,cl,ms

# Show cache information
parse_sdrf download-cache --show-info
```

Cache location: `~/.cache/sdrf-pipelines/ontologies/` (Linux/macOS) or `%LOCALAPPDATA%\sdrf-pipelines\ontologies\` (Windows)

## Converters

Convert SDRF files to configuration files for proteomics workflows.

<details>
<summary><strong>OpenMS Converter</strong></summary>

Convert SDRF to OpenMS experimental design files:

```bash
parse_sdrf convert-openms -s sdrf.tsv
```

**Output files:**

1. **Experimental settings** (`openms.tsv`) - one row per raw file with search parameters:

| Filename | FixedModifications | VariableModifications | Label | PrecursorMassTolerance | Enzyme |
|----------|-------------------|----------------------|-------|----------------------|--------|
| sample1.raw | Carbamidomethyl (C) | Oxidation (M) | label free sample | 10 ppm | Trypsin |

2. **Experimental design** (`experimental_design.tsv`) - sample-to-file mapping:

| Fraction_Group | Fraction | Spectra_Filepath | Label | MSstats_Condition | MSstats_BioReplicate |
|---------------|----------|------------------|-------|-------------------|---------------------|
| 1 | 1 | sample1.raw | 1 | control | 1 |
| 2 | 1 | sample2.raw | 1 | treatment | 2 |

</details>

<details>
<summary><strong>MaxQuant Converter</strong></summary>

Convert SDRF to MaxQuant parameter and experimental design files:

```bash
parse_sdrf convert-maxquant -s sdrf.tsv -f database.fasta -r /path/to/raw/files -o1 mqpar.xml -o2 experimentalDesign.txt
```

**Parameters:**

| Flag | Description |
|------|-------------|
| `-s` | SDRF file path |
| `-f` | FASTA database file |
| `-r` | Raw files directory |
| `-o1` | Output mqpar.xml path |
| `-o2` | Output experimental design path |
| `-m` | Enable match between runs (True/False) |
| `-n` | Number of threads |
| `-t` | Temp folder (SSD recommended) |

**Output files:**

1. **mqpar.xml** - MaxQuant parameters (enzyme, modifications, instrument, etc.)
2. **experimentalDesign.txt** - Sample mapping:

| Name | Fraction | Experiment | PTM |
|------|----------|------------|-----|
| sample1.raw | 1 | control_1 | |
| sample2.raw | 1 | treatment_1 | |

</details>

<details>
<summary><strong>MSstats Converter</strong></summary>

Convert SDRF to MSstats annotation file:

```bash
parse_sdrf convert-msstats -s sdrf.tsv -o annotation.csv
```

**Parameters:**

| Flag | Description |
|------|-------------|
| `-s` | SDRF file path |
| `-o` | Output annotation file path |
| `-c` | Condition columns (e.g., factor columns) |
| `-swath` | OpenSWATH to MSstats format |
| `-mq` | MaxQuant to MSstats format |

</details>

<details>
<summary><strong>NormalyzerDE Converter</strong></summary>

Convert SDRF to NormalyzerDE design file:

```bash
parse_sdrf convert-normalyzerde -s sdrf.tsv -o design.tsv
```

**Parameters:**

| Flag | Description |
|------|-------------|
| `-s` | SDRF file path |
| `-o` | Output design file path |
| `-c` | Group columns |
| `-oc` | Output comparisons file (optional) |
| `-mq` | MaxQuant experimental design for sample name mapping |

</details>

## CLI Reference

```
$ parse_sdrf --help

Commands:
  validate-sdrf         Validate an SDRF file
  convert-openms        Convert SDRF to OpenMS format
  convert-maxquant      Convert SDRF to MaxQuant format
  convert-msstats       Convert SDRF to MSstats annotation
  convert-normalyzerde  Convert SDRF to NormalyzerDE design
  split-sdrf            Split SDRF by a column value
  download-cache        Download ontology cache files
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

# OR run ruff directly (usually not necessary)
uv run ruff check .      # Linting
uv run ruff format .     # Formatting
```

### Running Tests

```bash
uv run pytest
```

### Type Checking (if not already done in pre-commit)

```bash
uv run mypy src/sdrf_pipelines
```

For more details, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Citation

If you use this software, please cite:

> Dai C, Füllgrabe A, Pfeuffer J, et al. A proteomics sample metadata representation for multiomics integration and big data analysis. _Nat Commun_ **12**, 5854 (2021). https://doi.org/10.1038/s41467-021-26111-3

For full citation details and BibTeX format, see [CITATION.cff](CITATION.cff).
