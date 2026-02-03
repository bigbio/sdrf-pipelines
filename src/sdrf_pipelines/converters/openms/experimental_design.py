"""Experimental design file writers for OpenMS conversion."""

import re
from dataclasses import dataclass, field

import pandas as pd

from sdrf_pipelines.converters.openms.constants import (
    ITRAQ_4PLEX,
    ITRAQ_8PLEX,
    SILAC_2PLEX,
    SILAC_3PLEX,
    TMT_PLEXES,
)
from sdrf_pipelines.converters.openms.utils import get_openms_file_name, infer_tmtplex
from sdrf_pipelines.utils.utils import tsv_line

# Pattern to extract sample number from source name
SAMPLE_IDENTIFIER_RE = re.compile(r"sample (\d+)$", re.IGNORECASE)

# iTRAQ 8-plex specific labels
ITRAQ_8PLEX_LABELS = {"ITRAQ113", "ITRAQ118", "ITRAQ119", "ITRAQ121"}


@dataclass
class FractionGroupTracker:
    """Tracks fraction group assignments for files."""

    fraction_groups: dict[str, int] = field(default_factory=dict)
    raw_frac: dict[int, list[str]] = field(default_factory=dict)
    pre_frac_group: int = 1

    def get_fraction_group(self, raw: str, fraction_group: int) -> int:
        """Get or assign a fraction group for a raw file."""
        if fraction_group not in self.raw_frac:
            self.raw_frac[fraction_group] = [raw]
            self._assign_new_fraction_group(raw, fraction_group)
        else:
            self.raw_frac[fraction_group].append(raw)
            self.fraction_groups[raw] = self.fraction_groups[self.raw_frac[fraction_group][0]]
        return self.fraction_groups[raw]

    def _assign_new_fraction_group(self, raw: str, fraction_group: int) -> None:
        """Assign a new fraction group, handling gaps in numbering."""
        if raw in self.fraction_groups:
            if fraction_group < self.fraction_groups[raw]:
                self.fraction_groups[raw] = fraction_group
        else:
            self.fraction_groups[raw] = fraction_group

        if self.fraction_groups[raw] > self.pre_frac_group + 1:
            self.fraction_groups[raw] = self.pre_frac_group + 1
        self.pre_frac_group = self.fraction_groups[raw]


@dataclass
class SampleIdTracker:
    """Tracks sample ID assignments for source names."""

    sample_id_map: dict[str, int] = field(default_factory=dict)
    bio_replicates: list[str | int] = field(default_factory=list)
    next_id: int = 1
    warnings: dict[str, int] = field(default_factory=dict)

    def get_sample_info(self, source_name: str) -> tuple[str | int, str]:
        """Get sample ID and bio replicate string for a source name."""
        sample_match = SAMPLE_IDENTIFIER_RE.search(source_name)

        if sample_match is not None:
            sample: str | int = sample_match.group(1)
            self._track_bio_replicate(sample)
            bio_replicate = str(sample)
        else:
            self._add_warning("No sample number identifier")
            sample = self._get_or_assign_id(source_name)
            self._track_bio_replicate(sample)
            bio_replicate = str(self.bio_replicates.index(sample) + 1)

        return sample, bio_replicate

    def _get_or_assign_id(self, source_name: str) -> int:
        """Get existing or assign new sample ID for source name."""
        if source_name not in self.sample_id_map:
            self.sample_id_map[source_name] = self.next_id
            self.next_id += 1
        return self.sample_id_map[source_name]

    def _track_bio_replicate(self, sample: str | int) -> None:
        """Track sample in bio replicates list if not already present."""
        if sample not in self.bio_replicates:
            self.bio_replicates.append(sample)

    def _add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings[message] = self.warnings.get(message, 0) + 1


@dataclass
class MixtureTracker:
    """Tracks mixture assignments for TMT/iTRAQ experiments."""

    mixture_raw_tag: dict[str, int] = field(default_factory=dict)
    mixture_sample_tag: dict[str | int, int] = field(default_factory=dict)
    next_mixture_id: int = 1

    def get_mixture_id(self, raw: str, sample: str | int) -> int:
        """Get or assign a mixture ID for a raw file and sample."""
        if raw not in self.mixture_raw_tag:
            if sample not in self.mixture_sample_tag:
                self.mixture_raw_tag[raw] = self.next_mixture_id
                self.mixture_sample_tag[sample] = self.next_mixture_id
                mix_id = self.next_mixture_id
                self.next_mixture_id += 1
            else:
                mix_id = self.mixture_sample_tag[sample]
                self.mixture_raw_tag[raw] = mix_id
        else:
            mix_id = self.mixture_raw_tag[raw]
        return mix_id


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

        if "TMT" in labels_str:
            return self._get_tmt_label(labels, label_set, label_index, raw)

        if "SILAC" in labels_str:
            return self._get_silac_label(labels, label_set, label_index, raw)

        if "ITRAQ" in labels_str:
            return self._get_itraq_label(labels, label_set, label_index, raw)

        raise ValueError("Label " + str(row["comment[label]"]) + " is not recognized")

    def _get_tmt_label(self, labels: list[str], label_set: set, label_index: dict, raw: str) -> str:
        """Get TMT label identifier."""
        choice = TMT_PLEXES[infer_tmtplex(label_set)]
        label = str(choice[labels[label_index[raw]]])
        label_index[raw] += 1
        return label

    def _get_silac_label(self, labels: list[str], label_set: set, label_index: dict, raw: str) -> str:
        """Get SILAC label identifier."""
        plex_map = self.silac3 if len(label_set) == 3 else self.silac2
        return str(plex_map[labels[label_index[raw]].lower()])

    def _get_itraq_label(self, labels: list[str], label_set: set, label_index: dict, raw: str) -> str:
        """Get iTRAQ label identifier."""
        is_8plex = len(label_set) > 4 or bool(label_set & ITRAQ_8PLEX_LABELS)
        plex_map = self.itraq8plex if is_8plex else self.itraq4plex
        label = str(plex_map[labels[label_index[raw]].lower()])
        label_index[raw] += 1
        return label

    def _calculate_fraction_group(
        self,
        source_name: str,
        replicate: str,
        source_name_list: list[str],
        source_name2n_reps: dict[str, int],
    ) -> int:
        """Calculate fraction group from source name and replicate."""
        source_name_index = source_name_list.index(source_name)
        offset = sum(int(source_name2n_reps[source_name_list[i]]) for i in range(source_name_index))
        return offset + int(replicate)

    def _is_multiplex_label(self, labels_lower: str) -> bool:
        """Check if the labels indicate a multiplexed experiment (TMT/iTRAQ)."""
        return "tmt" in labels_lower or "itraq" in labels_lower

    def _get_condition(self, file2combined_factors: dict[str, str], raw: str, label: str, source_name: str) -> str:
        """Get the condition for a sample."""
        combined_key = raw + label
        return source_name if file2combined_factors[combined_key] is None else file2combined_factors[combined_key]

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
        # Build file table
        file_table = self._build_file_table(
            sdrf, file2technical_rep, source_name_list, source_name2n_reps, file2label, extension_convert, file2fraction
        )

        # Build sample table
        first_file = sdrf["comment[data file]"].iloc[0]
        labels_lower = ",".join(map(str.lower, file2label[first_file]))
        is_multiplex = self._is_multiplex_label(labels_lower)

        sample_table = self._build_sample_table(
            sdrf, file2combined_factors, file_table["sample_id_tracker"], is_multiplex
        )

        # Merge warnings from sample tracker
        self.warnings.update(file_table["sample_id_tracker"].warnings)

        # Write output
        with open(output_filename, "w+", encoding="utf-8") as of:
            of.write(file_table["content"])
            of.write("\n")
            of.write(sample_table)

    def _build_file_table(
        self,
        sdrf: pd.DataFrame,
        file2technical_rep: dict[str, str],
        source_name_list: list[str],
        source_name2n_reps: dict[str, int],
        file2label: dict[str, list[str]],
        extension_convert: str | None,
        file2fraction: dict[str, str],
    ) -> dict:
        """Build the file table portion of the experimental design."""
        header = ["Fraction_Group", "Fraction", "Spectra_Filepath", "Label", "Sample"]
        content = "\t".join(header) + "\n"

        label_index = dict(zip(sdrf["comment[data file]"], [0] * len(sdrf["comment[data file]"])))
        fraction_tracker = FractionGroupTracker()
        sample_tracker = SampleIdTracker()

        for _, row in sdrf.iterrows():
            raw = row["comment[data file]"]
            source_name = row["source name"]
            replicate = file2technical_rep[raw]

            fraction_group = self._calculate_fraction_group(
                source_name, replicate, source_name_list, source_name2n_reps
            )
            frac_group = fraction_tracker.get_fraction_group(raw, fraction_group)
            sample, _ = sample_tracker.get_sample_info(source_name)

            labels = file2label[raw]
            label = self._get_label_from_labels(labels, set(labels), label_index, raw, row)
            out = get_openms_file_name(raw, extension_convert)

            content += tsv_line(str(frac_group), file2fraction[raw], out, label, str(sample))

        return {"content": content, "sample_id_tracker": sample_tracker}

    def _build_sample_table(
        self,
        sdrf: pd.DataFrame,
        file2combined_factors: dict[str, str],
        sample_tracker: SampleIdTracker,
        is_multiplex: bool,
    ) -> str:
        """Build the sample table portion of the experimental design."""
        if is_multiplex:
            header = ["Sample", "MSstats_Condition", "MSstats_BioReplicate", "MSstats_Mixture"]
        else:
            header = ["Sample", "MSstats_Condition", "MSstats_BioReplicate"]

        content = "\t".join(header) + "\n"
        sample_row_written: list[str | int] = []
        mixture_tracker = MixtureTracker()

        for _, row in sdrf.iterrows():
            raw = row["comment[data file]"]
            source_name = row["source name"]
            sample, bio_replicate = sample_tracker.get_sample_info(source_name)
            condition = self._get_condition(file2combined_factors, raw, row["comment[label]"], source_name)

            if sample in sample_row_written:
                continue

            if is_multiplex:
                mix_id = mixture_tracker.get_mixture_id(raw, sample)
                content += tsv_line(str(sample), condition, bio_replicate, str(mix_id))
            else:
                content += tsv_line(str(sample), condition, bio_replicate)

            sample_row_written.append(sample)

        return content

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
        is_multiplex = self._is_multiplex_label(cdf)

        header = self._get_one_table_header(is_multiplex, legacy)
        content = tsv_line(*header)

        label_index = dict(zip(sdrf["comment[data file]"], [0] * len(sdrf["comment[data file]"])))
        fraction_tracker = FractionGroupTracker()
        sample_tracker = SampleIdTracker()
        mixture_tracker = MixtureTracker()

        for _, row in sdrf.iterrows():
            raw = row["comment[data file]"]
            source_name = row["source name"]
            replicate = file2technical_rep[raw]

            fraction_group = self._calculate_fraction_group(
                source_name, replicate, source_name_list, source_name2n_reps
            )
            frac_group = fraction_tracker.get_fraction_group(raw, fraction_group)
            sample, bio_replicate = sample_tracker.get_sample_info(source_name)
            condition = self._get_condition(file2combined_factors, raw, row["comment[label]"], source_name)

            labels = file2label[raw]
            label = self._get_label_from_labels(labels, set(labels), label_index, raw, row)
            out = get_openms_file_name(raw, extension_convert)

            content += self._format_one_table_row(
                frac_group,
                file2fraction[raw],
                out,
                label,
                sample,
                condition,
                bio_replicate,
                is_multiplex,
                legacy,
                mixture_tracker,
                raw,
            )

        # Merge warnings from sample tracker
        self.warnings.update(sample_tracker.warnings)

        with open(output_filename, "w+", encoding="utf-8") as f:
            f.write(content)

    def _get_one_table_header(self, is_multiplex: bool, legacy: bool) -> list[str]:
        """Get the header for one-table format based on experiment type and legacy mode."""
        base_header = ["Fraction_Group", "Fraction", "Spectra_Filepath", "Label"]

        if legacy:
            base_header.append("Sample")

        base_header.extend(["MSstats_Condition", "MSstats_BioReplicate"])

        if is_multiplex:
            base_header.append("MSstats_Mixture")

        return base_header

    def _format_one_table_row(
        self,
        frac_group: int,
        fraction: str,
        filepath: str,
        label: str,
        sample: str | int,
        condition: str,
        bio_replicate: str,
        is_multiplex: bool,
        legacy: bool,
        mixture_tracker: MixtureTracker,
        raw: str,
    ) -> str:
        """Format a single row for the one-table format."""
        base_values = [str(frac_group), fraction, filepath, label]

        if legacy:
            base_values.append(str(sample))

        base_values.extend([condition, bio_replicate])

        if is_multiplex:
            mix_id = mixture_tracker.get_mixture_id(raw, sample)
            base_values.append(str(mix_id))

        return tsv_line(*base_values)
