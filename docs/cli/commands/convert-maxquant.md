# convert-maxquant

## Usage

```bash
parse_sdrf convert-maxquant [OPTIONS]
```

## Options

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

