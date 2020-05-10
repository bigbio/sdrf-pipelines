# sdrf-pipelines

The SDRF pipelines provide a set of tools to validate and convert SDRF files to different workflow configuration files such as MSstats,OpenMS and MaxQuant.

### Installation

```bash
pip install sdrf-pipelines
```

## Validate the SDRF

# How to use it:

Then, you can use the tool by executing the following command:

```bash
parse_sdrf validate-sdrf --sdrf_file {here_the_path_to_sdrf_file}
```

## Convert to OpenMS: Usage

```bash
parse_sdrf convert-openms -s sdrf.tsv
```

### Description:

- experiment settings (search engine settings etc.)
- experimental design

The experimental settings file contains one row for every raw file. Columns contain relevevant parameters like precursor mass tolerance, modifications etc. These settings can usually be derived from the sdrf file.

| URI | Filename | FixedModifications | VariableModifications | Label | PrecursorMassTolerance | PrecursorMassToleranceUnit | FragmentMassTolerance | FragmentMassToleranceUnit | DissociationMethod | Enzyme |
|------| ------------- |-------------|-----|---| ------------- |-------------|-----|---| ------------- |-------------|
| ftp://ftp.pride.ebi.ac.uk/pride/data/archive/XX/PXD324343/A0218_1A_R_FR01.raw | A0218_1A_R_FR01.raw | Acetyl (Protein N-term) | Gln->pyro-glu (Q),Oxidation (M) | label free sample| 10 | ppm | 10 | ppm | HCD | Trypsin |
| ftp://ftp.pride.ebi.ac.uk/pride/data/archive/XX/PXD324343/A0218_1A_R_FR02.raw | A0218_1A_R_FR02.raw | Acetyl (Protein N-term) | Gln->pyro-glu (Q),Oxidation (M) | label free sample| 10 | ppm | 10 | ppm | HCD | Trypsin |


The experimental design file contains information how to unambiguously map a single quantitative value. Most entries can
be derived from the sdrf file. However, definition of conditions might need manual changes.

- **Fraction_Group** identifier that indicates which fractions belong together. In the case of label-free data, the fraction group identifier has the same cardinality as the sample identifier.
- The **Fraction** identifier indicates which fraction was measured in this file. In the case of unfractionated data the fraction identifier is 1 for all samples.
- The **Label** identifier. 1 for label-free, 1 and 2 for SILAC light/heavy, e.g. 1-10 for TMT10Plex
- The **Spectra_Filepath** (e.g., path = "/data/SILAC_file.mzML")
- **MSstats_Condition** the condition identifier as used by MSstats
- **MSstats_BioReplicate** an identifier to indicate replication. (MSstats requires that there are no duplicate entries. E.g., if MSstats_Condition, Fraction_Group group and Fraction number are the same - as in the case of biological or technical replication, one uses the MSstats_BioReplicate to make entries non-unique)

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

## Convert to MaxQuant: Usage

```bash
parse_sdrf convert-maxquant -s sdrf.tsv -f {here_the_path_to_protein_database_file} -m {True or False} -pef {default 0.01} -prf {default 0.01} -t {temporary folder} -r {raw_data_folder} -n {number of threads:default 1} -o1 {parameters(.xml) output file path} -o2 {maxquant experimental design(.txt) output file path}
```
-f : please use single /  
-m : via matching between runs to boosts number of identifications<br>
-pef : posterior error probability calculation based on target-decoy search<br>
-prf : protein score = product of peptide PEPs (one for each sequence)<br>
-t : place on SSD (if possible) for faster search,please use single / <br>
-r : please use single \\ <br>
-n : each thread needs at least 2 GB of RAM,number of threads should be ≤ number of logical cores available <br>
    (otherwise, MaxQuant can crash)

### Description

- maxquant parameters file (mqpar.xml)
- maxquant experimental design file (.txt)

The maxquant parameters file mqpar.xml contains the parameters required for maxquant operation.some settings can usually be derived from the sdrf file such as enzyme, fixed modification, variable modification, instrument, fraction and label etc.Set other parameters as default.

Some parameters are listed：<br>
&nbsp;&nbsp;&nbsp; `<fastaFilePath>TAIR10_pep_20101214.fasta</fastaFilePath>`<br>
&nbsp;&nbsp;&nbsp; `<matchBetweenRuns>True</matchBetweenRuns>`<br>
&nbsp;&nbsp;&nbsp; `<maxQuantVersion>1.6.10.43</maxQuantVersion>`<br>
&nbsp;&nbsp;&nbsp; `<tempFolder>C:/Users/test</tempFolder>`<br>
&nbsp;&nbsp;&nbsp; `<numThreads>2</numThreads>`<br>
&nbsp;&nbsp;&nbsp; ```<filePaths><string>C:\Users\search_spectra\AT\130402_08.raw</string><string>C:\Users\search_spectra\AT\130412_08.raw</string></filePaths>```<br>
&nbsp;&nbsp;&nbsp; `<experiments>
		<string>sample 1_Tr_1</string>
		<string>sample 2_Tr_1</string>
	</experiments>`  
&nbsp;&nbsp;&nbsp;`<fractions>
		<short>32767</short>
		<short>32767</short>
	</fractions>`
