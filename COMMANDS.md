# CLI Commands Reference

> **Note**: This documentation is auto-generated from `parse_sdrf --help`.
> Do not edit manually - changes will be overwritten.

## Overview

```
Usage: parse_sdrf [OPTIONS] COMMAND [ARGS]...

  This tool validates SDRF files and can convert them for use in data analysis
  pipelines.

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
  download-cache        Download ontology cache files from GitHub
  list-templates        List all available SDRF templates with their versions
  split-sdrf            Command to split the sdrf file
  validate-sdrf         Command to validate the sdrf file
  validate-sdrf-simple  Simple command to validate the sdrf file.
```

## Validate SDRF

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

```bash
parse_sdrf validate-sdrf [OPTIONS]
```

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

## Convert to OpenMS

```bash
parse_sdrf convert-openms [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-s, --sdrf TEXT` | SDRF file |
| `-l, --legacy / -m, --modern` | legacy=Create artificial sample column not needed in OpenMS 2.6. |
| `-t1, --onetable / -t2, --twotables` | Create one-table or two-tables format. |
| `-v, --verbose / -q, --quiet` | Output debug information. |
| `-c, --conditionsfromcolumns TEXT` | Create conditions from provided (e.g., factor) columns. |
| `-e, --extension_convert TEXT` | convert extensions of files from one type to other 'raw:mzML,mzml:MZML,d:d'. The original extensions are case insensitive |
| `-h, --help` | Show this message and exit. |

## Convert to MaxQuant

```bash
parse_sdrf convert-maxquant [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-s, --sdrf TEXT` | SDRF file  [required] |
| `-f, --fastafilepath TEXT` | protein database file path  [required] |
| `-mcf, --mqconfdir TEXT` | MaxQuant default configure path |
| `-m, --matchbetweenruns TEXT` | via matching between runs to boosts number of identifications |
| `-pef, --peptidefdr FLOAT` | posterior error probability calculation based on target-decoy search |
| `-prf, --proteinfdr FLOAT` | protein score = product of peptide PEPs (one for each sequence) |
| `-t, --tempfolder TEXT` | temporary folder: place on SSD (if possible) for faster search |
| `-r, --raw_folder TEXT` | spectrum raw data folder  [required] |
| `-n, --numthreads INTEGER` | each thread needs at least 2 GB of RAM,number of threads should be ≤ number of logical cores available (otherwise, MaxQuant can crash) |
| `-o1, --output1 TEXT` | parameters .xml file  output file path |
| `-o2, --output2 TEXT` | maxquant experimental design .txt file |
| `-h, --help` | Show this message and exit. |

## Convert to MSstats

```bash
parse_sdrf convert-msstats [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-s, --sdrf TEXT` | SDRF file  [required] |
| `-c, --conditionsfromcolumns TEXT` | Create conditions from provided (e.g., factor) columns. |
| `-o, --outpath TEXT` | annotation out file path  [required] |
| `-swath, --openswathtomsstats BOOLEAN` | from openswathtomsstats output to msstats |
| `-mq, --maxqtomsstats BOOLEAN` | from maxquant output to msstats |
| `-h, --help` | Show this message and exit. |

## Convert to NormalyzerDE

```bash
parse_sdrf convert-normalyzerde [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-s, --sdrf TEXT` | SDRF file  [required] |
| `-c, --conditionsfromcolumns TEXT` | Create conditions from provided (e.g., factor) columns. |
| `-o, --outpath TEXT` | annotation out file path  [required] |
| `-oc, --outpathcomparisons TEXT` | out file path for comparisons |
| `-mq, --maxquant_exp_design_file TEXT` | Path to maxquant experimental design file for mapping MQ sample names |
| `-h, --help` | Show this message and exit. |

## Split SDRF

```bash
parse_sdrf split-sdrf [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-s, --sdrf_file TEXT` | SDRF file to be split  [required] |
| `-a, --attribute TEXT` | property to split, Multiple attributes are separated by commas  [required] |
| `-p, --prefix TEXT` | file prefix to be added to the sdrf file name |
| `-h, --help` | Show this message and exit. |

## Download Ontology Cache

Download ontology cache files from GitHub for offline validation.
By default, ontology cache files are automatically downloaded on first use
during validation. Use this command to pre-download all or specific ontology
files.
Examples:     parse_sdrf download-cache                    # Download all
ontologies     parse_sdrf download-cache -o efo,cl          # Download only
EFO and CL     parse_sdrf download-cache --show-info        # Show cache
information     parse_sdrf download-cache -f                 # Force re-
download all files

```bash
parse_sdrf download-cache [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-o, --ontology TEXT` | Comma-separated list of specific ontologies to download (e.g., 'efo,cl'). If not specified, downloads all ontologies. |
| `-d, --cache-dir TEXT` | Override the default cache directory location |
| `-i, --show-info` | Show cache information (location, available ontologies, download URLs) without downloading |
| `-f, --force` | Force re-download even if files already exist in cache |
| `-h, --help` | Show this message and exit. |
