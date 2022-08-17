import re

import pandas as pd

# example:  parse_sdrf convert-msstats -s ./testdata/PXD000288.sdrf.tsv -o ./test1.csv


class Msstats:
    def __init__(self) -> None:
        """Convert sdrf to msstats annotation file (label free sample)."""
        self.warnings = {}

    # Consider unlabeled analysis for now
    def convert_msstats_annotation(
        self, sdrf_file, split_by_columns, annotation_path, openswathtomsstats, maxqtomsstats
    ):
        sdrf = pd.read_csv(sdrf_file, sep="\t")
        sdrf = sdrf.astype(str)
        sdrf.columns = map(str.lower, sdrf.columns)  # convert column names to lower-case
        data = {}
        condition = []
        Experiments = []
        runs = sdrf["comment[data file]"].tolist()
        data["Run"] = runs
        data["IsotopeLabelType"] = ["L"] * len(runs)

        # convert list passed on command line '[assay name,comment[fraction identifier]]' to python list
        if split_by_columns:
            split_by_columns = split_by_columns[1:-1]  # trim '[' and ']'
            split_by_columns = split_by_columns.split(",")
            for i, value in enumerate(split_by_columns):
                split_by_columns[i] = value.lower()
            print("User selected factor columns: " + str(split_by_columns))

        if not split_by_columns:
            # get factor columns (except constant ones)
            factor_cols = [c for ind, c in enumerate(sdrf) if c.startswith("factor value[")]
        else:
            factor_cols = split_by_columns
        for _, row in sdrf.iterrows():
            if not split_by_columns:
                combined_factors = self.combine_factors_to_conditions(factor_cols, row)
            else:
                # take only only entries of splitting columns to generate the conditions
                combined_factors = "_".join(list(row[split_by_columns]))
            condition.append(combined_factors)
        data["Condition"] = condition

        sample_identifier_re = re.compile(r"sample (\d+)$", re.IGNORECASE)
        # get BioReplicate
        BioReplicate = []
        sample_id_map = {}
        sample_id = 1
        value = []

        for _, row in sdrf.iterrows():
            source_name = row["source name"]

            if re.search(sample_identifier_re, source_name) is not None:
                sample = re.search(sample_identifier_re, source_name).group(1)

                # MSstats BioReplicate column needs to be different for samples from different conditions.
                # so we can't just use the technical replicate identifier in sdrf but use the sample identifer
                MSstatsBioReplicate = sample
                if sample not in BioReplicate:
                    BioReplicate.append(sample)
            else:
                warning_message = "No sample number identifier"
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1

                # Solve non-sample id expression models
                if source_name in sample_id_map.keys():
                    sample = sample_id_map[source_name]
                else:
                    sample_id_map[source_name] = sample_id
                    sample = sample_id
                    sample_id += 1
                if sample not in BioReplicate:
                    BioReplicate.append(sample)
                MSstatsBioReplicate = str(BioReplicate.index(sample) + 1)
            value.append(MSstatsBioReplicate)

            if "comment[technical replicate]" in sdrf.columns:
                Experiments.append(row["source name"] + "_" + str(row["comment[technical replicate]"]))
            else:
                Experiments.append(row["source name"] + "_" + "1")

        data["BioReplicate"] = value

        # for OpenSWATH
        if openswathtomsstats:
            data["Filename"] = runs

        # for MaxQuant
        if maxqtomsstats:
            data["Experiment"] = Experiments
        pd.DataFrame(data).to_csv(annotation_path, index=False)

    def combine_factors_to_conditions(self, factor_cols, row):
        all_factors = list(row[factor_cols])
        combined_factors = "_".join(all_factors)
        if combined_factors == "":
            warning_message = "No factors specified. Adding Source Name as factor. Will be used as condition. "
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            combined_factors = row["source name"]
        return combined_factors
