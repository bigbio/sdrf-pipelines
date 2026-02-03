# download-cache

Download ontology cache files from GitHub for offline validation.
By default, ontology cache files are automatically downloaded on first use
during validation. Use this command to pre-download all or specific ontology
files.
Examples:     parse_sdrf download-cache                    # Download all
ontologies     parse_sdrf download-cache -o efo,cl          # Download only
EFO and CL     parse_sdrf download-cache --show-info        # Show cache
information     parse_sdrf download-cache -f                 # Force re-
download all files

## Usage

```bash
parse_sdrf download-cache [OPTIONS]
```

## Options

| Option | Description |
|--------|-------------|
| `-o, --ontology TEXT` | Comma-separated list of specific ontologies to download (e.g., 'efo,cl'). If not specified, downloads all ontologies. |
| `-d, --cache-dir TEXT` | Override the default cache directory location |
| `-i, --show-info` | Show cache information (location, available ontologies, download URLs) without downloading |
| `-f, --force` | Force re-download even if files already exist in cache |
| `-h, --help` | Show this message and exit. |

