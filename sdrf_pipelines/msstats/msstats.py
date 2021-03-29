# -*- coding: utf-8 -*-


import pandas as pd
import re


class Msstats():

    def __init__(self) -> None:
        """Convert sdrf to msstats annotation file (label free sample)."""
        self.warnings = dict()

    # Consider unlabeled analysis for now
    def convert_msstats_annotation(self, sdrf_file, split_by_columns, annotation_path, OpenSWATHtoMSstats, MaxQtoMSstats):
        sdrf = pd.read_csv(sdrf_file, sep='\t')
        sdrf = sdrf.astype(str)
        sdrf.columns = map(str.lower, sdrf.columns)  # convert column names to lower-case
        data = dict()
        condition = list()
        Experiments = list()
        runs = sdrf['comment[data file]'].tolist()
        data['Run'] = runs
        data['IsotopeLabelType'] = ['L'] * len(runs)

        # convert list passed on command line '[assay name,comment[fraction identifier]]' to python list
        if split_by_columns:
            split_by_columns = split_by_columns[1:-1]  # trim '[' and ']'
            split_by_columns = split_by_columns.split(',')
            for i, value in enumerate(split_by_columns):
                split_by_columns[i] = value.lower()
            print('User selected factor columns: ' + str(split_by_columns))

        if not split_by_columns:
            # get factor columns (except constant ones)
            factor_cols = [c for ind, c in enumerate(sdrf) if
                           c.startswith('factor value[')]
        else:
            factor_cols = split_by_columns
        for _, row in sdrf.iterrows():
            if not split_by_columns:
                combined_factors = self.combine_factors_to_conditions(factor_cols, row)
            else:
                # take only only entries of splitting columns to generate the conditions
                combined_factors = "_".join(list(row[split_by_columns]))
            condition.append(combined_factors)
        data['Condition'] = condition

        # get BioReplicate
        BioReplicates = ['1' if x == 'not available' or x == 'not applicable' else
                         x for x in sdrf['characteristics[biological replicate]'].tolist()]
        if len(list(filter(lambda x: re.match(r'sample \d+', x, flags=re.IGNORECASE) is not None,
                           sdrf['source name']))) == 0:
            data['BioReplicate'] = sdrf['source name'].tolist()
        elif BioReplicates == ['1'] * len(runs):
            data['BioReplicate'] = sdrf['source name'].tolist()
        else:
            BioReplicate = 0
            value = []
            indexs = [re.findall(r'sample (\d+)', x, flags=re.IGNORECASE)[0] for x in sdrf['source name']]
            sdrf['sort_index'] = indexs
            sdrf = sdrf.sort_values(by="sort_index", ascending=True)
            sample = []
            for _, row in sdrf.iterrows():
                biorep = row['characteristics[biological replicate]']
                if biorep.lower() == 'not available' or biorep.lower() == 'not applicable':
                    biorep = '1'
                if biorep == '1' and row['source name'] not in sample:
                    sample.append(row['source name'])
                    BioReplicate += 1
                value.append(BioReplicate)
                if 'comment[technical replicate]' in sdrf.columns:
                    Experiments.append(row['source name'] + '_' + str(row['comment[technical replicate]']))
                else:
                    Experiments.append(row['source name'] + '_' + '1')
            data['BioReplicate'] = value

        # for OpenSWATH
        if OpenSWATHtoMSstats:
            data['Filename'] = runs

        # for MaxQuant
        if MaxQtoMSstats:
            data['Experiment'] = Experiments
        pd.DataFrame(data).to_csv(annotation_path, index=False)

    def combine_factors_to_conditions(self, factor_cols, row):
        all_factors = list(row[factor_cols])
        combined_factors = "_".join(all_factors)
        if combined_factors == "":
            warning_message = "No factors specified. Adding Source Name as factor. Will be used " \
                              "as condition. "
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            combined_factors = row['source name']
        return combined_factors

