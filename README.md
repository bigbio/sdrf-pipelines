# sdrf-pipelines | SDRF Validator | SDRF Converter

![Python application](https://github.com/bigbio/sdrf-pipelines/workflows/Python%20application/badge.svg)
![Python package](https://github.com/bigbio/sdrf-pipelines/workflows/Python%20package/badge.svg)
![Upload Python Package](https://github.com/bigbio/sdrf-pipelines/workflows/Upload%20Python%20Package/badge.svg)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/ebf85b7ad8304422ab495c3f720bf3ae)](https://www.codacy.com/gh/bigbio/sdrf-pipelines/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=bigbio/sdrf-pipelines&amp;utm_campaign=Badge_Grade)
[![PyPI version](https://badge.fury.io/py/sdrf-pipelines.svg)](https://badge.fury.io/py/sdrf-pipelines)
![PyPI - Downloads](https://img.shields.io/pypi/dm/sdrf-pipelines)

**Validate and convert SDRF files with sdrf-pipelines and its `parse_sdrf` CLI.**

This is the official SDRF file validation tool and it can convert SDRF files to different workflow configuration files such as MSstats, OpenMS and MaxQuant.

## Installation

### Basic Installation (Structural Validation + Conversions)

For most users who only need structural validation and pipeline conversions:

```bash
pip install sdrf-pipelines
```

This installs the core dependencies which support:
- ✅ **Structural validation** (8 out of 9 validators: column checks, formatting, uniqueness, etc.)
- ✅ **All converters** (OpenMS, MaxQuant, MSstats, NormalyzerDE)
- ❌ Ontology term validation (requires additional dependencies)

### Full Installation (Including Ontology Validation)

If you need ontology term validation via OLS (Ontology Lookup Service):

```bash
pip install sdrf-pipelines[ontology]
```

This adds dependencies for ontology validation: `fastparquet`, `pooch`, `rdflib`, `requests`

**Ontology Caching Mechanism:**

The ontology validation uses a smart caching system powered by [pooch](https://www.fatiando.org/pooch/):

- **Lazy loading**: Ontology files are only downloaded when first needed during validation
- **Location**: Cache files are stored in the OS-specific cache directory:
  - Linux/macOS: `~/.cache/sdrf-pipelines/ontologies/`
  - Windows: `C:\Users\<user>\AppData\Local\sdrf-pipelines\ontologies\`
- **Integrity**: All files are verified with SHA256 checksums
- **Offline support**: Once cached, validations work without internet access
- **Size**: Total cache is approximately 50-100 MB for all ontologies

**Supported ontologies:** EFO, CL, PRIDE, NCBITAXON, MS, BTO, CLO, HANCESTRO, PATO, UO, DOID, GO

**Pre-download cache files (optional):**

```bash
parse_sdrf download-cache              # Download all ontologies
parse_sdrf download-cache -o efo,cl    # Download specific ontologies
parse_sdrf download-cache --show-info  # Show cache information
parse_sdrf download-cache -f           # Force re-download all files
```

### All Features

To install everything (currently equivalent to `[ontology]`):

```bash
pip install sdrf-pipelines[all]
```

## Validate SDRF files

You can validate an SDRF file by executing the following command:

```bash
parse_sdrf validate-sdrf --sdrf_file {here_the_path_to_sdrf_file}
```

### Skip Ontology Validation

If you only have the base installation (without the `[ontology]` extra) or want to skip ontology term validation:

```bash
parse_sdrf validate-sdrf --sdrf_file {path_to_sdrf} --skip-ontology
```

This will perform all structural validations but skip the ontology term lookup, which is useful for:

- Quick validation checks
- Environments without internet access
- CI/CD pipelines where only structural validation is needed

**Note**: If ontology dependencies are not installed, the tool will automatically skip ontology validation and show a warning.

### New JSON Schema-Based Validation

The SDRF validator now uses a YAML schema-based validation system that makes it easier to define and maintain validation rules. The new system offers several advantages:

#### Key Features

1. **YAML-Defined Schemas**: All validation templates are defined in YAML files:
   - `default.yaml` - Common fields for all SDRF files (includes mass spectrometry fields)
   - `human.yaml` - Human-specific fields
   - `vertebrates.yaml` - Vertebrate-specific fields
   - `nonvertebrates.yaml` - Non-vertebrate-specific fields
   - `plants.yaml` - Plant-specific fields
   - `cell_lines.yaml` - Cell line-specific fields
   - `disease_example.yaml` - Example schema for disease terms with multiple ontologies

2. **Enhanced Ontology Validation**:
   - Support for multiple ontologies per field
   - Rich error messages with descriptions and examples
   - Special value handling for "not available" and "not applicable"

3. **Schema Inheritance**: Templates can extend other templates, making it easy to create specialized validation rules.

#### Example JSON Schema

```json
{
  "name": "characteristics_cell_type",
  "sdrf_name": "characteristics[cell type]",
  "description": "Cell type",
  "required": true,
  "validators": [
    {
      "type": "whitespace",
      "params": {}
    },
    {
      "type": "ontology",
      "params": {
        "ontologies": ["cl", "bto", "clo"],
        "allow_not_applicable": true,
        "allow_not_available": true,
        "description": "The cell type should be a valid Cell Ontology term",
        "examples": ["hepatocyte", "neuron", "fibroblast"]
      }
    }
  ]
}
```

#### Simplified Validation Command

A simplified validation command is also available:

```bash
parse_sdrf validate-sdrf-simple {here_the_path_to_sdrf_file} --template {template_name}
```

This command provides a more straightforward interface for validating SDRF files, without the additional options for skipping specific validations.

#### Creating Custom Validation Templates

You can create your own validation templates by defining JSON schema files. Here's how:

1. Create a JSON file with your validation rules:
   ```json
   {
     "name": "my_template",
     "description": "My custom template",
     "extends": "default",
     "min_columns": 7,
     "fields": [
       {
         "name": "characteristics_my_field",
         "sdrf_name": "characteristics[my field]",
         "description": "My custom field",
         "required": true,
         "validators": [
           {
             "type": "whitespace",
             "params": {}
           },
           {
             "type": "ontology",
             "params": {
               "ontology_name": "my_ontology",
               "allow_not_applicable": true,
               "description": "My field description",
               "examples": ["example1", "example2"]
             }
           }
         ]
       }
     ]
   }
   ```

2. Place the file in the `sdrf_pipelines/sdrf/schemas/` directory.

3. Use your template with the validation command:
   ```bash
   parse_sdrf validate-sdrf --sdrf_file {path_to_sdrf_file} --template my_template
   ```

The template system supports inheritance, so you can extend existing templates to add or override fields.

## Convert SDRF files

`sdrf-pipelines` provides a multitude of converters which take an SDRF file and other inputs to create configuration files consumed by other software.

### Convert to OpenMS

```bash
parse_sdrf convert-openms -s sdrf.tsv
```

#### Description:

-   experiment settings (search engine settings etc.)
-   experimental design

The experimental settings file contains one row for every raw file. Columns contain relevevant parameters like precursor mass tolerance, modifications etc. These settings can usually be derived from the sdrf file.

| URI | Filename | FixedModifications | VariableModifications | Label | PrecursorMassTolerance | PrecursorMassToleranceUnit | FragmentMassTolerance | FragmentMassToleranceUnit | DissociationMethod | Enzyme |
|------| ------------- |-------------|-----|---| ------------- |-------------|-----|---| ------------- |-------------|
| ftp://ftp.pride.ebi.ac.uk/pride/data/archive/XX/PXD324343/A0218_1A_R_FR01.raw | A0218_1A_R_FR01.raw | Acetyl (Protein N-term) | Gln->pyro-glu (Q),Oxidation (M) | label free sample| 10 | ppm | 10 | ppm | HCD | Trypsin |
| ftp://ftp.pride.ebi.ac.uk/pride/data/archive/XX/PXD324343/A0218_1A_R_FR02.raw | A0218_1A_R_FR02.raw | Acetyl (Protein N-term) | Gln->pyro-glu (Q),Oxidation (M) | label free sample| 10 | ppm | 10 | ppm | HCD | Trypsin |


The experimental design file contains information how to unambiguously map a single quantitative value. Most entries can
be derived from the sdrf file. However, definition of conditions might need manual changes.

-   **Fraction_Group** identifier that indicates which fractions belong together. In the case of label-free data, the fraction group identifier has the same cardinality as the sample identifier.
-   The **Fraction** identifier indicates which fraction was measured in this file. In the case of unfractionated data the fraction identifier is 1 for all samples.
-   The **Label** identifier. 1 for label-free, 1 and 2 for SILAC light/heavy, e.g. 1-10 for TMT10Plex
-   The **Spectra_Filepath** (e.g., path = "/data/SILAC_file.mzML")
-   **MSstats_Condition** the condition identifier as used by MSstats
-   **MSstats_BioReplicate** an identifier to indicate replication. (MSstats requires that there are no duplicate entries. E.g., if MSstats_Condition, Fraction_Group group and Fraction number are the same - as in the case of biological or technical replication, one uses the MSstats_BioReplicate to make entries non-unique)

| Fraction_Group| Fraction      | Spectra_Filepath  | Label | MSstats_Condition      | MSstats_BioReplicate  |
| ------------- |-------------|-----|---| ------------- |-----------|
| 1 | 1 | A0218_1A_R_FR01.raw | 1 | 1 | 1 | 1 |
| 1 | 2 | A0218_1A_R_FR02.raw | 1 | 1 | 1 | 1 |
| . | . | ... | . | . | . | . |
| 1 | 15 | A0218_2A_FR15.raw | 1 | 1 | 1 | 1 |
| 2 | 1 | A0218_2A_FR01.raw | 1 | 2 | 2 | 1 |
| . | . | ... | . | . | . | . |
| . | . | ... | . | . | . | . |
| 10 | 15 | A0218_10A_FR15.raw | 1 | 10 | 10 | 1 |

For details, please see the MSstats documentation

### Convert to MaxQuant: Usage

```bash
parse_sdrf convert-maxquant -s sdrf.tsv -f {here_the_path_to_protein_database_file} -m {True or False} -pef {default 0.01} -prf {default 0.01} -t {temporary folder} -r {raw_data_folder} -n {number of threads:default 1} -o1 {parameters(.xml) output file path} -o2 {maxquant experimental design(.txt) output file path}
```
eg.
```bash
parse_sdrf convert-maxquant -s /root/ChengXin/Desktop/sdrf.tsv -f /root/ChengXin/MyProgram/search_spectra/AT/TAIR10_pep_20101214.fasta -r /root/ChengXin/MyProgram/virtuabox/share/raw_data/ -o1 /root/ChengXin/test.xml -o2 /root/ChengXin/test_exp.xml -t /root/ChengXin/MyProgram/virtuabox/share/raw_data/ -pef 0.01 -prf 0.01 -n 4
```


-   -s  : SDRF file
-   -f : fasta file
-   -r : spectra raw file folder
-   -mcf : MaxQuant default configure path (if given, Can add new modifications)
-   -m : via matching between runs to boosts number of identifications
-   -pef : posterior error probability calculation based on target-decoy search
-   -prf : protein score = product of peptide PEPs (one for each sequence)
-   -t : place on SSD (if possible) for faster search，It is recommended not to be the same as the raw file directory
-   -n : each thread needs at least 2 GB of RAM,number of threads should be ≤ number of logical cores available(otherwise, MaxQuant can crash)

#### Description

-   maxquant parameters file (mqpar.xml)
-   maxquant experimental design file (.txt)

The maxquant parameters file mqpar.xml contains the parameters required for maxquant operation.some settings can usually be derived from the sdrf file such as enzyme, fixed modification, variable modification, instrument, fraction and label etc.Set other parameters as default.The current version of maxquant supported by the script is 1.6.10.43

Some parameters are listed：
-   \<fastaFilePath>TAIR10_pep_20101214.fasta\</fastaFilePath>
-   \<matchBetweenRuns>True\</matchBetweenRuns>
-   \<maxQuantVersion>1.6.10.43\</maxQuantVersion>
-   \<tempFolder>C:/Users/test\</tempFolder>
-   \<numThreads>2\</numThreads>
-   \<filePaths>
    -   \<string>C:\Users\search_spectra\AT\130402_08.raw\</string>
    -   \<string>C:\Users\search_spectra\AT\130412_08.raw\</string>
-   \</filePaths>
-   \<experiments>
    -   \<string>sample 1_Tr_1\</string>
    -   \<string>sample 2_Tr_1\</string>
-   \</experiments>
-   \<fractions>
    -   \<short>32767\</short>
    -   \<short>32767\</short>
-   \</fractions>
-   \<paramGroupIndices>
    -   \<int>0\</int>
    -   \<int>1\</int>
-   \</paramGroupIndices>
-   \<msInstrument>0\</msInstrument>
-   \<fixedModifications>
    -   \<string>Carbamidomethyl (C)\</string>
-   \</fixedModifications>
-   \<enzymes>
    -   \<string>Trypsin\</string>
-   \</enzymes>
-   \<variableModifications>
    -   \<string>Oxidation (M)\</string>
    -   \<string>Phospho (Y)\</string>
    -   \<string>Acetyl (Protein N-term)\</string>
    -   \<string>Phospho (T)\</string>
    -   \<string>Phospho (S)\</string>
-   \</variableModifications>

For details, please see the MaxQuant documentation

The maxquant experimental design file contains name,Fraction,Experiement and PTM column.Most entries can be derived from the sdrf file.
-   **Name**  raw data file name.
-   **Fraction**  In the Fraction column you must assign if the corresponding files shown in the left column belong to a fraction of a gel fraction. If your data is not obtained through gel-based pre-fractionation you must assign the same number(default 1) for all files in the column Fraction.
-   **Experiment**  In the column named as Experiment if you want to combine all experimental replicates as a single dataset to be analyzed by MaxQuant, you must enter the same identifier for the files which should be concatenated . However, if you want each individual file to be treated as a different experiment which you want to compare further you should assign different identifiers to each of the files as shown below.

| Name | Fraction | Experiment | PTM |
| :----:| :----: | :----: | :----: |
| 130402_08.raw | 1 | sample 1_Tr_1 |     |
| 130412_08.raw | 1 | sample 2_Tr_1 |     |

### Convert to MSstats annotation file: Usage

```bash
parse_sdrf convert-msstats -s ./testdata/PXD000288.sdrf.tsv -o ./test1.csv
```

-   -s  : SDRF file
-   -c  : Create conditions from provided (e.g., factor) columns as used by MSstats
-   -o  : annotation out file path
-   -swath  : from openswathtomsstats output to msstats default false
-   -mq  : from maxquant output to msstats default false

### Convert to NormalyzerDE design file: Usage

```bash
parse_sdrf convert-normalyzerde -s ./testdata/PXD000288.sdrf.tsv -o ./testPXD000288_design.tsv
```

-   -s  : SDRF file
-   -c  : Create groups from provided (e.g., factor) columns as used by NormalyzerDE, for example `-c ["characteristics[spiked compound]"]` (optional)
-   -o  : NormalyzerDE design out file path
-   -oc  : Out file path for comparisons towards first group (optional)
-   -mq  : Path to MaxQuant experimental design file for mapping MQ sample names. (optional)


# Help

```
$ parse_sdrf --help
Usage: parse_sdrf [OPTIONS] COMMAND [ARGS]...

  This is the main tool that gives access to all commands to convert SDRF
  files into pipelines specific configuration files.

Options:
  --version   Show the version and exit.
  -h, --help  Show this message and exit.

Commands:
  build-index-ontology  Convert an ontology file to an index file
  convert-maxquant      convert sdrf to maxquant parameters file and generate
                        an experimental design file
  convert-msstats       convert sdrf to msstats annotation file
  convert-normalyzerde  convert sdrf to NormalyzerDE design file
  convert-openms        convert sdrf to openms file output
  split-sdrf            Command to split the sdrf file
  validate-sdrf         Command to validate the sdrf file
  validate-sdrf-simple  Simple command to validate the sdrf file

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
