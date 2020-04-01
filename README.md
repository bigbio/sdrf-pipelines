# sdrf-openms

A repository to convert SDRF proteomics files into openms config files

parse_sdrf.py converts a SDRF proteomics file into 

- the experiment settings (search engine settings etc.)
- the experimental design

The experimental settings file contains one row for every raw file. Columns contain relevevant parameters like precursor mass tolerance, modifications etc.

The experimental design file contains information how to unambiguously map a single quantitative value:
- First column is a "Fraction Group" identifier that indicates which fractions belong together. In the case of label-free data, the fraction group identifier has the same cardinality as the sample identifier.
- The fraction identifier indicates which fraction was measured in this file. In the case of unfractionated data the fraction identifier is 1 for all samples.
- The label identifier. 1 for label-free, 1 and 2 for SILAC light/heavy, e.g. 1-10 for TMT10Plex
- The spectra file (e.g., path = "/data/SILAC_file.mzML") 
- The sample that has been measured (e.g., sample = 1)
- MSStats_Condition the condition identifier as used by MSstats
- MSStats_BioReplicate an identifier to indicate replication. (MSstats requires that there are no duplicate entries. E.g., if condition, fraction group and fraction number are the same - as in the case of biological or technical replication, one uses the MSStats_BioReplicate to make entries non-unique)

For details, please see the MSstats documentation
