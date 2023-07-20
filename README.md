# sdrf-pipelines

![Python application](https://github.com/bigbio/sdrf-pipelines/workflows/Python%20application/badge.svg)
![Python package](https://github.com/bigbio/sdrf-pipelines/workflows/Python%20package/badge.svg)
![Upload Python Package](https://github.com/bigbio/sdrf-pipelines/workflows/Upload%20Python%20Package/badge.svg)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/ebf85b7ad8304422ab495c3f720bf3ae)](https://www.codacy.com/gh/bigbio/sdrf-pipelines/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=bigbio/sdrf-pipelines&amp;utm_campaign=Badge_Grade)
[![PyPI version](https://badge.fury.io/py/sdrf-pipelines.svg)](https://badge.fury.io/py/sdrf-pipelines)
![PyPI - Downloads](https://img.shields.io/pypi/dm/sdrf-pipelines)

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

## Convert to MaxQuant: Usage

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

### Description

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

## Convert to MSstats annotation file: Usage

```bash
parse_sdrf convert-msstats -s ./testdata/PXD000288.sdrf.tsv -o ./test1.csv
```

-   -s  : SDRF file
-   -c  : Create conditions from provided (e.g., factor) columns as used by MSstats
-   -o  : annotation out file path
-   -swath  : from openswathtomsstats output to msstats default false
-   -mq  : from maxquant output to msstats default false

## Convert to NormalyzerDE design file: Usage

```bash
parse_sdrf convert-normalyzerde -s ./testdata/PXD000288.sdrf.tsv -o ./testPXD000288_design.tsv
```

-   -s  : SDRF file
-   -c  : Create groups from provided (e.g., factor) columns as used by NormalyzerDE, for example `-c ["characteristics[spiked compound]"]` (optional)
-   -o  : NormalyzerDE design out file path
-   -oc  : Out file path for comparisons towards first group (optional)
-   -mq  : Path to MaxQuant experimental design file for mapping MQ sample names. (optional)


# Citations

- Dai C, Füllgrabe A, Pfeuffer J, Solovyeva EM, Deng J, Moreno P, Kamatchinathan S, Kundu DJ, George N, Fexova S, Grüning B, Föll MC, Griss J, Vaudel M, Audain E, Locard-Paulet M, Turewicz M, Eisenacher M, Uszkoreit J, Van Den Bossche T, Schwämmle V, Webel H, Schulze S, Bouyssié D, Jayaram S, Duggineni VK, Samaras P, Wilhelm M, Choi M, Wang M, Kohlbacher O, Brazma A, Papatheodorou I, Bandeira N, Deutsch EW, Vizcaíno JA, Bai M, Sachsenberg T, Levitsky LI, Perez-Riverol Y. A proteomics sample metadata representation for multiomics integration and big data analysis. Nat Commun. 2021 Oct 6;12(1):5854. doi: 10.1038/s41467-021-26111-3. PMID: 34615866; PMCID: PMC8494749. [Manuscript](https://www.nature.com/articles/s41467-021-26111-3)

- Perez-Riverol, Yasset, and European Bioinformatics Community for Mass Spectrometry. "Toward a Sample Metadata Standard in Public Proteomics Repositories." Journal of Proteome Research 19.10 (2020): 3906-3909. [Manuscript](https://pubs.acs.org/doi/abs/10.1021/acs.jproteome.0c00376)
