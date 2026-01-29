"""Experimental design file writers for OpenMS conversion."""

import re

import pandas as pd

from sdrf_pipelines.openms.constants import (
    ITRAQ_4PLEX,
    ITRAQ_8PLEX,
    SILAC_2PLEX,
    SILAC_3PLEX,
    TMT_PLEXES,
)
from sdrf_pipelines.openms.utils import FileToColumnEntries, get_openms_file_name, infer_tmtplex
from sdrf_pipelines.utils.utils import tsv_line


class ExperimentalDesignWriter:
    """Writes experimental design files for OpenMS."""

    def __init__(self):
        self.warnings: dict[str, int] = {}
        self.silac3 = SILAC_3PLEX
        self.silac2 = SILAC_2PLEX
        self.itraq4plex = ITRAQ_4PLEX
        self.itraq8plex = ITRAQ_8PLEX

    def _get_label_from_labels(
        self,
        labels: list[str],
        label_set: set,
        label_index: dict,
        raw: str,
        row: pd.Series,
    ) -> str:
        """Determine the numeric label identifier from label strings."""
        labels_str = ",".join(labels)

        if "label free sample" in labels:
            return "1"
        elif "TMT" in labels_str:
            choice = TMT_PLEXES[infer_tmtplex(label_set)]
            label = str(choice[labels[label_index[raw]]])
            label_index[raw] = label_index[raw] + 1
            return label
        elif "SILAC" in labels_str:
            if len(label_set) == 3:
                label = str(self.silac3[labels[label_index[raw]].lower()])
            else:
                label = str(self.silac2[labels[label_index[raw]].lower()])
            return label
        elif "ITRAQ" in labels_str:
            if (
                len(label_set) > 4
                or "ITRAQ113" in label_set
                or "ITRAQ118" in label_set
                or "ITRAQ119" in label_set
                or "ITRAQ121" in label_set
            ):
                label = str(self.itraq8plex[labels[label_index[raw]].lower()])
            else:
                label = str(self.itraq4plex[labels[label_index[raw]].lower()])
            label_index[raw] = label_index[raw] + 1
            return label
        else:
            raise ValueError("Label " + str(row["comment[label]"]) + " is not recognized")

    def _calculate_fraction_group(
        self,
        source_name: str,
        replicate: str,
        source_name_list: list[str],
        source_name2n_reps: dict[str, int],
    ) -> int:
        """Calculate fraction group from source name and replicate."""
        source_name_index = source_name_list.index(source_name)
        offset = 0
        for i in range(source_name_index):
            offset = offset + int(source_name2n_reps[source_name_list[i]])
        return offset + int(replicate)

    def write_two_table_format(
        self,
        output_filename: str,
        sdrf: pd.DataFrame,
        file2technical_rep: dict[str, str],
        source_name_list: list[str],
        source_name2n_reps: dict[str, int],
        file2label: dict[str, list[str]],
        extension_convert: str | None,
        file2fraction: dict[str, str],
        file2combined_factors: dict[str, str],
    ):
        """Write two-table format experimental design file.

        This format has a file table and a separate sample table.
        """
        openms_file_header = ["Fraction_Group", "Fraction", "Spectra_Filepath", "Label", "Sample"]
        f = "\t".join(openms_file_header) + "\n"

        label_index = dict(zip(sdrf["comment[data file]"], [0] * len(sdrf["comment[data file]"])))
        sample_identifier_re = re.compile(r"sample (\d+)$", re.IGNORECASE)
        Fraction_group = {}
        sample_id_map = {}
        sample_id = 1
        pre_frac_group = 1
        raw_frac = {}

        for _, row in sdrf.iterrows():
            raw = row["comment[data file]"]
            source_name = row["source name"]
            replicate = file2technical_rep[raw]

            fraction_group = self._calculate_fraction_group(
                source_name, replicate, source_name_list, source_name2n_reps
            )

            if fraction_group not in raw_frac:
                raw_frac[fraction_group] = [raw]
                if raw in Fraction_group:
                    if fraction_group < Fraction_group[raw]:
                        Fraction_group[raw] = fraction_group
                else:
                    Fraction_group[raw] = fraction_group
                if Fraction_group[raw] > pre_frac_group + 1:
                    Fraction_group[raw] = pre_frac_group + 1
                pre_frac_group = Fraction_group[raw]
            else:
                raw_frac[fraction_group].append(raw)
                Fraction_group[raw] = Fraction_group[raw_frac[fraction_group][0]]

            if re.search(sample_identifier_re, source_name) is not None:
                sample = re.search(sample_identifier_re, source_name).group(1)
            else:
                warning_message = "No sample identifier"
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                if source_name in sample_id_map:
                    sample = sample_id_map[source_name]
                else:
                    sample_id_map[source_name] = sample_id
                    sample = sample_id
                    sample_id += 1

            labels = file2label[raw]
            label_set = set(labels)
            label = self._get_label_from_labels(labels, label_set, label_index, raw, row)

            out = get_openms_file_name(raw, extension_convert)
            f += tsv_line(str(Fraction_group[raw]), file2fraction[raw], out, label, str(sample))

        # Sample table
        f += "\n"
        first_file = sdrf["comment[data file]"].iloc[0]
        labels_lower = ",".join(map(str.lower, file2label[first_file]))

        if "tmt" in labels_lower or "itraq" in labels_lower:
            openms_sample_header = ["Sample", "MSstats_Condition", "MSstats_BioReplicate", "MSstats_Mixture"]
        else:
            openms_sample_header = ["Sample", "MSstats_Condition", "MSstats_BioReplicate"]

        f += "\t".join(openms_sample_header) + "\n"
        sample_row_written = []
        mixture_identifier = 1
        mixture_raw_tag = {}
        mixture_sample_tag = {}
        BioReplicate = []

        for _, row in sdrf.iterrows():
            raw = row["comment[data file]"]
            source_name = row["source name"]

            if re.search(sample_identifier_re, source_name) is not None:
                sample = re.search(sample_identifier_re, source_name).group(1)
                MSstatsBioReplicate = sample
                if sample not in BioReplicate:
                    BioReplicate.append(sample)
            else:
                warning_message = "No sample identifier"
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                sample = sample_id_map[source_name]
                if sample not in BioReplicate:
                    BioReplicate.append(sample)
                MSstatsBioReplicate = str(BioReplicate.index(sample) + 1)

            if file2combined_factors[raw + row["comment[label]"]] is None:
                condition = source_name
            else:
                condition = file2combined_factors[raw + row["comment[label]"]]

            if len(openms_sample_header) == 4:
                if raw not in mixture_raw_tag:
                    if sample not in mixture_sample_tag:
                        mixture_raw_tag[raw] = mixture_identifier
                        mixture_sample_tag[sample] = mixture_identifier
                        mix_id = mixture_identifier
                        mixture_identifier += 1
                    else:
                        mix_id = mixture_sample_tag[sample]
                        mixture_raw_tag[raw] = mix_id
                else:
                    mix_id = mixture_raw_tag[raw]

                if sample not in sample_row_written:
                    f += str(sample) + "\t" + condition + "\t" + MSstatsBioReplicate + "\t" + str(mix_id) + "\n"
                    sample_row_written.append(sample)
            else:
                if sample not in sample_row_written:
                    f += str(sample) + "\t" + condition + "\t" + MSstatsBioReplicate + "\n"
                    sample_row_written.append(sample)

        with open(output_filename, "w+", encoding="utf-8") as of:
            of.write(f)

    def write_one_table_format(
        self,
        output_filename: str,
        legacy: bool,
        sdrf: pd.DataFrame,
        file2technical_rep: dict[str, str],
        source_name_list: list[str],
        source_name2n_reps: dict[str, int],
        file2combined_factors: dict[str, str],
        file2label: dict[str, list[str]],
        extension_convert: str | None,
        file2fraction: dict[str, str],
    ):
        """Write one-table format experimental design file.

        This format combines file and sample information in a single table.
        """
        first_file = sdrf["comment[data file]"].iloc[0]
        cdf = file2label[first_file][0].lower() if file2label[first_file] else ""

        if "tmt" in cdf or "itraq" in cdf:
            if legacy:
                header = [
                    "Fraction_Group", "Fraction", "Spectra_Filepath", "Label", "Sample",
                    "MSstats_Condition", "MSstats_BioReplicate", "MSstats_Mixture",
                ]
            else:
                header = [
                    "Fraction_Group", "Fraction", "Spectra_Filepath", "Label",
                    "MSstats_Condition", "MSstats_BioReplicate", "MSstats_Mixture",
                ]
        else:
            if legacy:
                header = [
                    "Fraction_Group", "Fraction", "Spectra_Filepath", "Label", "Sample",
                    "MSstats_Condition", "MSstats_BioReplicate",
                ]
            else:
                header = [
                    "Fraction_Group", "Fraction", "Spectra_Filepath", "Label",
                    "MSstats_Condition", "MSstats_BioReplicate",
                ]

        experimental_design = tsv_line(*header)
        label_index = dict(zip(sdrf["comment[data file]"], [0] * len(sdrf["comment[data file]"])))
        sample_identifier_re = re.compile(r"sample (\d+)$", re.IGNORECASE)
        Fraction_group = {}
        mixture_identifier = 1
        mixture_raw_tag = {}
        mixture_sample_tag = {}
        BioReplicate = []
        sample_id_map = {}
        sample_id = 1
        pre_frac_group = 1
        raw_frac = {}

        for _, row in sdrf.iterrows():
            raw = row["comment[data file]"]
            source_name = row["source name"]
            replicate = file2technical_rep[raw]

            fraction_group = self._calculate_fraction_group(
                source_name, replicate, source_name_list, source_name2n_reps
            )

            if fraction_group not in raw_frac:
                raw_frac[fraction_group] = [raw]
                if raw in Fraction_group:
                    if fraction_group < Fraction_group[raw]:
                        Fraction_group[raw] = fraction_group
                else:
                    Fraction_group[raw] = fraction_group
                if Fraction_group[raw] > pre_frac_group + 1:
                    Fraction_group[raw] = pre_frac_group + 1
                pre_frac_group = Fraction_group[raw]
            else:
                raw_frac[fraction_group].append(raw)
                Fraction_group[raw] = Fraction_group[raw_frac[fraction_group][0]]

            if re.search(sample_identifier_re, source_name) is not None:
                sample = re.search(sample_identifier_re, source_name).group(1)
                MSstatsBioReplicate = sample
                if sample not in BioReplicate:
                    BioReplicate.append(sample)
            else:
                warning_message = "No sample number identifier"
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                if source_name in sample_id_map:
                    sample = sample_id_map[source_name]
                else:
                    sample_id_map[source_name] = sample_id
                    sample = sample_id
                    sample_id += 1
                if sample not in BioReplicate:
                    BioReplicate.append(sample)
                MSstatsBioReplicate = str(BioReplicate.index(sample) + 1)

            if file2combined_factors[raw + row["comment[label]"]] is None:
                condition = source_name
            else:
                condition = file2combined_factors[raw + row["comment[label]"]]

            labels = file2label[raw]
            label_set = set(labels)
            label = self._get_label_from_labels(labels, label_set, label_index, raw, row)

            out = get_openms_file_name(raw, extension_convert)

            if "MSstats_Mixture" in header:
                if raw not in mixture_raw_tag:
                    if sample not in mixture_sample_tag:
                        mixture_raw_tag[raw] = mixture_identifier
                        mixture_sample_tag[sample] = mixture_identifier
                        mix_id = mixture_identifier
                        mixture_identifier += 1
                    else:
                        mix_id = mixture_sample_tag[sample]
                        mixture_raw_tag[raw] = mix_id
                else:
                    mix_id = mixture_raw_tag[raw]

                if legacy:
                    experimental_design += tsv_line(
                        str(Fraction_group[raw]), file2fraction[raw], out, label,
                        str(sample), condition, MSstatsBioReplicate, str(mix_id),
                    )
                else:
                    experimental_design += tsv_line(
                        str(Fraction_group[raw]), file2fraction[raw], out, label,
                        condition, MSstatsBioReplicate, str(mix_id),
                    )
            else:
                if legacy:
                    experimental_design += tsv_line(
                        str(Fraction_group[raw]), file2fraction[raw], out, label,
                        str(sample), condition, MSstatsBioReplicate,
                    )
                else:
                    experimental_design += tsv_line(
                        str(Fraction_group[raw]), file2fraction[raw], out, label, condition, MSstatsBioReplicate
                    )

        with open(output_filename, "w+", encoding="utf-8") as f:
            f.write(experimental_design)
