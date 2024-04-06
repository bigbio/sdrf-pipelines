# -*- coding: utf-8 -*-


import csv
import re

import pandas as pd

# Based on msstats class

# example:  parse_sdrf convert-normalyzerde -s ./testdata/PXD000288.sdrf.tsv -o ./normalyzer_design.tsv


class NormalyzerDE:
    def __init__(self) -> None:
        """Convert sdrf to normalyzerde design file (label free quantification assumed)."""
        self.warnings = {}

    # Consider unlabeled analysis for now
    def convert_normalyzerde_design(
        self, sdrf_file, split_by_columns, annotation_path, comparisons_path, maxquant_exp_design_file
    ):
        sdrf = pd.read_csv(sdrf_file, sep="\t")
        sdrf = sdrf.astype(str)
        sdrf.columns = map(str.lower, sdrf.columns)  # convert column names to lower-case
        data = {}
        condition = []
        runs = sdrf["comment[data file]"].tolist()
        source_names = sdrf["source name"].tolist()

        assays = []

        for raw in runs:
            new_assay = raw.replace(".raw", "")
            assays.append(new_assay)

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

        group = []
        # Shorten down condition to QY only if present. Also replace '-' with '_' as reserved for comparisons.
        for con in condition:
            con = con.replace("-", "_")
            if re.search("QY=.*", con) != None:
                match = re.search("QY=(.*)", con)
                groupnew = match[1]
                if groupnew.index(";") > 0:
                    groupnew = groupnew[: groupnew.index(";")]
                group.append(groupnew.replace(" ", "."))
            else:
                group.append(con.replace(" ", "."))

        sample_identifier_re = re.compile(r"sample (\d+)$", re.IGNORECASE)
        # get BioReplicate
        sample_id_map = {}
        sample_id = 1

        replicates = self.get_replicates(sdrf, sample_identifier_re, sample_id_map, sample_id)

        # For MaxQuant mapping
        if maxquant_exp_design_file != "":
            mq_design = pd.read_csv(maxquant_exp_design_file, sep="\t")
            mq_assays = mq_design["Name"].tolist()
            mq_experiments = mq_design["Experiment"].tolist()
            new_samples = []
            for assay in assays:
                new_sample = mq_experiments[mq_assays.index(assay)].replace(" ", ".")
                new_samples.append(new_sample.replace("-", "."))
            data["sample"] = new_samples
        else:
            data["sample"] = assays

        data["Run"] = runs
        data["Assay"] = assays
        data["source_name"] = source_names
        data["technical_replicate"] = replicates
        data["group"] = group
        pd.DataFrame(data).to_csv(annotation_path, index=False, sep="\t")

        # Write out comparisons toward first factor
        if comparisons_path != "":
            comparisons = []
            uniquefactors = sorted(set(group))
            firstfactor = group[0]
            for factor in uniquefactors:
                if factor != firstfactor:
                    comparisons.append(factor + "-" + firstfactor)
            with open(comparisons_path, "w") as target:
                writer = csv.writer(target, delimiter=",")
                writer.writerow(comparisons)

    def get_replicates(self, sdrf, sample_identifier_re="comment[organism]", sample_id_map=None, sample_id=1):
        replicates = []
        value = []
        BioReplicate = []
        for _, row in sdrf.iterrows():
            source_name = row["source name"]

            if re.search(sample_identifier_re, source_name) is not None:
                sample = re.search(sample_identifier_re, source_name).group(1)
                # Bioreplicate not used for NormalyzerDE
                # BioReplicate column needs to be different for samples from different conditions.
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
                replicates.append(str(row["comment[technical replicate]"]))
            else:
                replicates.append("1")

        return replicates

    def combine_factors_to_conditions(self, factor_cols, row):
        all_factors = list(row[factor_cols])
        combined_factors = "_".join(all_factors)
        if combined_factors == "":
            warning_message = "No factors specified. Adding Source Name as factor. Will be used " "as condition. "
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            combined_factors = row["source name"]
        return combined_factors
