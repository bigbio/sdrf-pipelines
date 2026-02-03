# convert-openms

## Usage

```bash
parse_sdrf convert-openms [OPTIONS]
```

## Options

| Option | Description |
|--------|-------------|
| `-s, --sdrf TEXT` | SDRF file |
| `-l, --legacy / -m, --modern` | legacy=Create artificial sample column not needed in OpenMS 2.6. |
| `-t1, --onetable / -t2, --twotables` | Create one-table or two-tables format. |
| `-v, --verbose / -q, --quiet` | Output debug information. |
| `-c, --conditionsfromcolumns TEXT` | Create conditions from provided (e.g., factor) columns. |
| `-e, --extension_convert TEXT` | convert extensions of files from one type to other 'raw:mzML,mzml:MZML,d:d'. The original extensions are case insensitive |
| `-h, --help` | Show this message and exit. |

