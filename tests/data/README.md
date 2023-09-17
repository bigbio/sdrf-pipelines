
# Documenting the testing data

The general layout of the testing data is as follows:

1. A parent directory with either an accession representing where
    the data would come from (e.g. PXD000288) or a generic name
    describing what the data is meant to represent.
    1. In the specific case of the `erroneous` directory, the
        none of the files should pass checking or conversion.
    2. In the specific case of the `reference` directory, the
        files should all pass checking and conversion. and are
        extracted from the reference implementation of the sdrf
        format (https://github.com/bigbio/proteomics-sample-metadata/tree/master/)
2. Within that directoy there can be a file called
    `expected_experimental_design.tsv` which is the expected
    output of the from converting to a design (same for expected_openms.tsv).


```
.
├── PXD000288
│   └── PXD000288.sdrf.tsv
├── PXD001819
│   ├── PXD001819.sdrf.tsv
│   ├── expected_experimental_design.tsv
│   └── expected_openms.tsv
├── PXD015270
│   └── PXD015270-Sample-1.tsv
├── README.md
├── erroneous
│   └── sdrf_error.tsv
├── reference
│   ├── README.md
│   ├── ...
│   └── PXD000126
│       └── PXD000126.sdrf.tsv
└── generic
    └── sdrf.tsv

```