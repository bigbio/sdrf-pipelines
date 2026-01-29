r"""Convert SDRF files for use with OpenMS.

Example command:

parse_sdrf convert-openms \
    -s .\sdrf-pipelines\sdrf_pipelines\large_sdrf.tsv \
    -c '[characteristics[biological replicate],characteristics[individual]]'
"""

import re
from collections import Counter

import pandas as pd

from sdrf_pipelines.converters.openms.constants import (
    ENZYME_MAPPINGS,
    ITRAQ_4PLEX,
    ITRAQ_8PLEX,
    ITRAQ_DEFAULT_MODS,
    SILAC_2PLEX,
    SILAC_3PLEX,
    TMT_DEFAULT_MODS,
)
from sdrf_pipelines.converters.openms.experimental_design import ExperimentalDesignWriter
from sdrf_pipelines.converters.openms.modifications import ModificationConverter
from sdrf_pipelines.converters.openms.utils import (
    FileToColumnEntries,
    infer_tmtplex,
    parse_tolerance,
)
from sdrf_pipelines.utils.utils import tsv_line


class OpenMS:
    """Convert SDRF files to OpenMS format."""

    def __init__(self) -> None:
        super().__init__()
        self.warnings: dict[str, int] = {}
        self._mod_converter = ModificationConverter()
        self._design_writer = ExperimentalDesignWriter()

        # Keep references for backwards compatibility
        self.enzymes = ENZYME_MAPPINGS
        self.itraq4plex = ITRAQ_4PLEX
        self.itraq8plex = ITRAQ_8PLEX
        self.silac3 = SILAC_3PLEX
        self.silac2 = SILAC_2PLEX

    def openms_ify_mods(self, sdrf_mods):
        """Convert SDRF modifications to OpenMS notation."""
        return self._mod_converter.openms_ify_mods(sdrf_mods)

    def openms_convert(
        self,
        sdrf_file: str,
        one_table: bool = False,
        legacy: bool = False,
        verbose: bool = False,
        split_by_columns: str | None = None,
        extension_convert: str | None = None,
    ):
        """Convert SDRF file to OpenMS format.

        Args:
            sdrf_file: Path to SDRF file
            one_table: If True, write one-table format; otherwise two-table format
            legacy: If True, include legacy Sample column
            verbose: If True, print verbose output
            split_by_columns: Columns to split output by (comma-separated in brackets)
            extension_convert: File extension conversion pattern
        """
        print("PROCESSING: " + sdrf_file + '"')

        if split_by_columns:
            split_by_columns = split_by_columns[1:-1]  # trim '[' and ']'
            split_by_columns_list = split_by_columns.split(",")
            print("User selected factor columns: " + str(split_by_columns_list))

        # Load SDRF file
        sdrf = pd.read_table(sdrf_file)
        null_cols = sdrf.columns[sdrf.isnull().any()]
        if sdrf.isnull().values.any():
            raise ValueError(
                "Encountered empty cells while reading SDRF."
                "Please check your file, e.g. for too many column headers or empty fields"
                f"Columns with empty values: {list(null_cols)}"
            )
        sdrf = sdrf.astype(str)
        sdrf.columns = sdrf.columns.str.lower()

        # Get modification columns
        mod_cols = [c for c in sdrf.columns if c.startswith("comment[modification parameters")]

        # Determine factor columns
        if split_by_columns:
            factor_cols = split_by_columns_list
        else:
            factor_cols = [c for c in sdrf.columns if c.startswith("factor value[") and len(sdrf[c].unique()) >= 1]
            characteristics_cols = [
                c for c in sdrf.columns if c.startswith("characteristics[") and len(sdrf[c].unique()) >= 1
            ]
            characteristics_cols = self._remove_redundant_characteristics(characteristics_cols, sdrf, factor_cols)
            print("Factor columns: " + str(factor_cols))
            print("Characteristics columns (those covered by factor columns removed): " + str(characteristics_cols))

        # Process each row
        source_name_list: list[str] = []
        source_name2n_reps: dict[str, int] = {}
        list_of_combined_factors = []
        f2c = FileToColumnEntries()

        for _, row in sdrf.iterrows():
            raw = row["comment[data file]"]
            source_name = row["source name"]

            # Extract modifications
            all_mods = list(row[mod_cols])
            var_mods = sorted([m for m in all_mods if "MT=variable" in m or "MT=Variable" in m])
            fixed_mods = sorted([m for m in all_mods if "MT=fixed" in m or "MT=Fixed" in m])

            if verbose:
                print(row)

            fixed_mods_string = self.openms_ify_mods(fixed_mods) if fixed_mods else ""
            variable_mods_string = self.openms_ify_mods(var_mods) if var_mods else ""
            f2c.file2mods[raw] = (fixed_mods_string, variable_mods_string)

            f2c.file2source[raw] = source_name
            if source_name not in source_name_list:
                source_name_list.append(source_name)

            # Process tolerances
            self._process_tolerances(row, f2c, raw)

            # Process dissociation method
            self._process_dissociation_method(row, f2c, raw)

            # Process technical replicate
            self._process_technical_replicate(row, f2c, raw, source_name, source_name2n_reps)

            # Process enzyme
            self._process_enzyme(row, f2c, raw)

            # Process fraction
            self._process_fraction(row, f2c, raw)

            # Process label
            self._process_label(row, f2c, raw, sdrf)

            # Process factors/conditions
            if not split_by_columns:
                combined_factors = self._combine_factors_to_conditions(characteristics_cols, factor_cols, row)
            else:
                combined_factors = "|".join(list(row[split_by_columns_list]))

            sdrf["_conditions_from_factors"] = pd.Series([None] * sdrf.shape[0], dtype="object")
            f2c.file2combined_factors[raw + row["comment[label]"]] = combined_factors
            list_of_combined_factors.append(combined_factors)

        sdrf["_conditions_from_factors"] = list_of_combined_factors

        # Collect warnings from modification converter
        self.warnings.update(self._mod_converter.warnings)

        conditions = Counter(f2c.file2combined_factors.values()).keys()
        files_per_condition = Counter(f2c.file2combined_factors.values()).values()
        print("Conditions (" + str(len(conditions)) + "): " + str(conditions))
        print("Files per condition: " + str(files_per_condition))

        # Write output files
        if not split_by_columns:
            self._write_output_files(
                sdrf, f2c, source_name_list, source_name2n_reps, one_table, legacy, extension_convert
            )
        else:
            self._write_split_output_files(
                sdrf, f2c, conditions, source_name_list, source_name2n_reps, one_table, legacy, extension_convert
            )

        # Collect warnings from design writer
        self.warnings.update(self._design_writer.warnings)
        self._report_warnings(sdrf_file)

    def _process_tolerances(self, row, f2c: FileToColumnEntries, raw: str):
        """Process precursor and fragment mass tolerances."""
        if "comment[precursor mass tolerance]" in row:
            pc_tol_str = row["comment[precursor mass tolerance]"].strip()
            tol, unit = parse_tolerance(pc_tol_str)
            if tol is None or unit is None:
                raise ValueError(f"Cannot read precursor mass tolerance: {pc_tol_str}")
            f2c.file2pctol[raw] = tol
            f2c.file2pctolunit[raw] = unit
        else:
            warning_message = "No precursor mass tolerance set. Assuming 10 ppm."
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            f2c.file2pctol[raw] = "10"
            f2c.file2pctolunit[raw] = "ppm"

        if "comment[fragment mass tolerance]" in row:
            f_tol_str = row["comment[fragment mass tolerance]"].strip()
            tol, unit = parse_tolerance(f_tol_str)
            if tol is None or unit is None:
                raise ValueError(f"Cannot read precursor mass tolerance: {f_tol_str}")
            f2c.file2fragtol[raw] = tol
            f2c.file2fragtolunit[raw] = unit
        else:
            warning_message = "No fragment mass tolerance set. Assuming 20 ppm."
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            f2c.file2fragtol[raw] = "20"
            f2c.file2fragtolunit[raw] = "ppm"

    def _process_dissociation_method(self, row, f2c: FileToColumnEntries, raw: str):
        """Process dissociation method."""
        if "comment[dissociation method]" in row:
            search_result = re.search("NT=(.+?)(;|$)", row["comment[dissociation method]"])
            if search_result is not None:
                f2c.file2diss[raw] = search_result.group(1).upper()
            else:
                warning_message = "No dissociation method provided. Assuming HCD."
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                f2c.file2diss[raw] = "HCD"
        else:
            warning_message = "No dissociation method provided. Assuming HCD."
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            f2c.file2diss[raw] = "HCD"

    def _process_technical_replicate(
        self, row, f2c: FileToColumnEntries, raw: str, source_name: str, source_name2n_reps: dict
    ):
        """Process technical replicate information."""
        if "comment[technical replicate]" in row:
            technical_replicate = str(row["comment[technical replicate]"])
            if "not available" in technical_replicate:
                f2c.file2technical_rep[raw] = "1"
            else:
                f2c.file2technical_rep[raw] = technical_replicate
        else:
            f2c.file2technical_rep[raw] = "1"

        # Store highest replicate number for this source name
        if source_name in source_name2n_reps:
            source_name2n_reps[source_name] = max(
                int(source_name2n_reps[source_name]),
                int(f2c.file2technical_rep[raw]),
            )
        else:
            source_name2n_reps[source_name] = int(f2c.file2technical_rep[raw])

    def _process_enzyme(self, row, f2c: FileToColumnEntries, raw: str):
        """Process enzyme/cleavage agent."""
        cleavage_agent_details = row["comment[cleavage agent details]"]
        enzyme_search_result = re.search("NT=(.+?)(;|$)", cleavage_agent_details)

        if enzyme_search_result is not None:
            enzyme = enzyme_search_result.group(1)
        else:
            enzyme = ""

        enzyme = enzyme.capitalize()
        if enzyme in self.enzymes:
            enzyme = self.enzymes[enzyme]

        f2c.file2enzyme[raw] = enzyme

    def _process_fraction(self, row, f2c: FileToColumnEntries, raw: str):
        """Process fraction identifier."""
        if "comment[fraction identifier]" in row:
            fraction = str(row["comment[fraction identifier]"])
            if "not available" in fraction:
                f2c.file2fraction[raw] = "1"
            else:
                f2c.file2fraction[raw] = fraction
        else:
            f2c.file2fraction[raw] = "1"

    def _process_label(self, row, f2c: FileToColumnEntries, raw: str, sdrf: pd.DataFrame):
        """Process label information."""
        search_result_label = re.search("NT=(.+?)(;|$)", row["comment[label]"])
        if search_result_label is not None:
            label = search_result_label.group(1)
            f2c.file2label[raw] = [label]
        else:
            if "TMT" in row["comment[label]"]:
                label = sdrf[sdrf["comment[data file]"] == raw]["comment[label]"].tolist()
            elif "SILAC" in row["comment[label]"]:
                label = sdrf[sdrf["comment[data file]"] == raw]["comment[label]"].tolist()
            elif "label free sample" in row["comment[label]"]:
                label = ["label free sample"]
            elif "ITRAQ" in row["comment[label]"]:
                label = sdrf[sdrf["comment[data file]"] == raw]["comment[label]"].tolist()
            else:
                raise ValueError("Label " + str(row["comment[label]"]) + " is not recognized")
            f2c.file2label[raw] = label

    def _combine_factors_to_conditions(self, characteristics_cols, factor_cols, row):
        """Combine factors to create condition strings."""
        all_factors = list(row[factor_cols])
        combined_factors = "|".join(all_factors)
        if combined_factors == "":
            all_factors = list(row[characteristics_cols])
            combined_factors = "|".join(all_factors)
            if combined_factors == "":
                warning_message = "No factors specified. Adding dummy factor used as condition."
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                combined_factors = None
            else:
                warning_message = (
                    "No factors specified. Adding non-redundant characteristics as factor. Will be used as condition. "
                )
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
        return combined_factors

    def _remove_redundant_characteristics(self, characteristics_cols, sdrf, factor_cols):
        """Remove characteristics columns that duplicate factor columns."""
        redundant = set()
        for c in characteristics_cols:
            c_col = sdrf[c]
            for f in factor_cols:
                f_col = sdrf[f]
                if c_col.equals(f_col):
                    redundant.add(c)
        return [x for x in characteristics_cols if x not in redundant]

    def _write_output_files(
        self, sdrf, f2c, source_name_list, source_name2n_reps, one_table, legacy, extension_convert
    ):
        """Write search settings and experimental design files."""
        self._save_search_settings_to_file("openms.tsv", sdrf, f2c)

        if one_table:
            self._design_writer.write_one_table_format(
                "experimental_design.tsv",
                legacy,
                sdrf,
                f2c.file2technical_rep,
                source_name_list,
                source_name2n_reps,
                f2c.file2combined_factors,
                f2c.file2label,
                extension_convert,
                f2c.file2fraction,
            )
        else:
            self._design_writer.write_two_table_format(
                "experimental_design.tsv",
                sdrf,
                f2c.file2technical_rep,
                source_name_list,
                source_name2n_reps,
                f2c.file2label,
                extension_convert,
                f2c.file2fraction,
                f2c.file2combined_factors,
            )

    def _write_split_output_files(
        self, sdrf, f2c, conditions, source_name_list, source_name2n_reps, one_table, legacy, extension_convert
    ):
        """Write output files split by condition."""
        for index, c in enumerate(conditions):
            split_sdrf = sdrf.loc[sdrf["_conditions_from_factors"] == c]
            output_filename = "openms.tsv." + str(index)
            self._save_search_settings_to_file(output_filename, split_sdrf, f2c)

            output_filename = "experimental_design.tsv." + str(index)
            if one_table:
                self._design_writer.write_one_table_format(
                    output_filename,
                    legacy,
                    split_sdrf,
                    f2c.file2technical_rep,
                    source_name_list,
                    source_name2n_reps,
                    f2c.file2combined_factors,
                    f2c.file2label,
                    extension_convert,
                    f2c.file2fraction,
                )
            else:
                self._design_writer.write_two_table_format(
                    output_filename,
                    split_sdrf,
                    f2c.file2technical_rep,
                    source_name_list,
                    source_name2n_reps,
                    f2c.file2label,
                    extension_convert,
                    f2c.file2fraction,
                    f2c.file2combined_factors,
                )

    def _save_search_settings_to_file(self, output_filename, sdrf, f2c: FileToColumnEntries):
        """Save search settings to TSV file."""
        header = [
            "URI",
            "Filename",
            "FixedModifications",
            "VariableModifications",
            "Proteomics Data Acquisition Method",
            "Label",
            "PrecursorMassTolerance",
            "PrecursorMassToleranceUnit",
            "FragmentMassTolerance",
            "FragmentMassToleranceUnit",
            "DissociationMethod",
            "Enzyme",
        ]
        search_settings = tsv_line(*header)
        raws = []

        for _, row in sdrf.iterrows():
            URI = row["comment[file uri]"]
            raw = row["comment[data file]"]

            if "comment[proteomics data acquisition method]" not in row:
                warning_message = (
                    "The comment[proteomics data acquisition method] column is missing, "
                    "default Data-Dependent Acquisition"
                )
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                acquisition_method = "Data-Dependent Acquisition"
            else:
                acquisition_method = row["comment[proteomics data acquisition method]"]
                if len(acquisition_method.split(";")) > 1:
                    acquisition_method = acquisition_method.split(";")[0].split("=")[1]

            if raw in raws:
                continue
            raws.append(raw)

            labels = f2c.file2label[raw]
            labels_str = ",".join(labels)
            label_set = set(labels)

            if "TMT" in labels_str:
                label = infer_tmtplex(label_set)
                self._add_default_tmt_mods(f2c, raw, label)
            elif "label free sample" in label_set:
                label = "label free sample"
            elif "silac" in labels_str.lower():
                label = "SILAC"
            elif "ITRAQ" in labels_str:
                label = self._get_itraq_plex(label_set)
                self._add_default_itraq_mods(f2c, raw, label)
            else:
                raise ValueError(
                    f"Failed to find any supported labels. Supported labels are 'silac', "
                    f"'label free sample', 'ITRAQ', and tmt labels, found {labels}"
                )

            search_settings += tsv_line(
                URI,
                raw,
                f2c.file2mods[raw][0],
                f2c.file2mods[raw][1],
                acquisition_method,
                label,
                f2c.file2pctol[raw],
                f2c.file2pctolunit[raw],
                f2c.file2fragtol[raw],
                f2c.file2fragtolunit[raw],
                f2c.file2diss[raw],
                f2c.file2enzyme[raw],
            )

        with open(output_filename, "w+", encoding="utf-8") as f:
            f.write(search_settings)

    def _get_itraq_plex(self, label_set: set) -> str:
        """Determine iTRAQ plex from label set."""
        if (
            len(label_set) > 4
            or "ITRAQ113" in label_set
            or "ITRAQ118" in label_set
            or "ITRAQ119" in label_set
            or "ITRAQ121" in label_set
        ):
            return "itraq8plex"
        return "itraq4plex"

    def _add_default_tmt_mods(self, f2c: FileToColumnEntries, raw: str, label: str):
        """Add default TMT modifications if not present."""
        if "tmt" not in f2c.file2mods[raw][0].lower() and "tmt" not in f2c.file2mods[raw][1].lower():
            warning_message = (
                "The sdrf with TMT label doesn't contain TMT modification. Adding default variable modifications."
            )
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            tmt_var_mod = TMT_DEFAULT_MODS[label]
            if f2c.file2mods[raw][1]:
                VarMod = ",".join(f2c.file2mods[raw][1].split(",") + tmt_var_mod)
                f2c.file2mods[raw] = (f2c.file2mods[raw][0], VarMod)
            else:
                f2c.file2mods[raw] = (f2c.file2mods[raw][0], ",".join(tmt_var_mod))

    def _add_default_itraq_mods(self, f2c: FileToColumnEntries, raw: str, label: str):
        """Add default iTRAQ modifications if not present."""
        if "itraq" not in f2c.file2mods[raw][0].lower() and "itraq" not in f2c.file2mods[raw][1].lower():
            warning_message = (
                "The sdrf with ITRAQ label doesn't contain label modification. Adding default variable modifications."
            )
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            itraq_var_mod = ITRAQ_DEFAULT_MODS[label]
            if f2c.file2mods[raw][1]:
                VarMod = ",".join(f2c.file2mods[raw][1].split(",") + itraq_var_mod)
                f2c.file2mods[raw] = (f2c.file2mods[raw][0], VarMod)
            else:
                f2c.file2mods[raw] = (f2c.file2mods[raw][0], ",".join(itraq_var_mod))

    def _report_warnings(self, sdrf_file: str):
        """Report warnings and success message."""
        if len(self.warnings) != 0:
            for k, v in self.warnings.items():
                print('WARNING: "' + k + '" occurred ' + str(v) + " times.")
        print("SUCCESS (WARNINGS=" + str(len(self.warnings)) + "): " + sdrf_file)

    # Keep old method names for backwards compatibility
    def combine_factors_to_conditions(self, characteristics_cols, factor_cols, row):
        """Backwards compatible method."""
        return self._combine_factors_to_conditions(characteristics_cols, factor_cols, row)

    def removeRedundantCharacteristics(self, characteristics_cols, sdrf, factor_cols):
        """Backwards compatible method."""
        return self._remove_redundant_characteristics(characteristics_cols, sdrf, factor_cols)

    def reportWarnings(self, sdrf_file):
        """Backwards compatible method."""
        return self._report_warnings(sdrf_file)

    def writeTwoTableExperimentalDesign(self, *args, **kwargs):
        """Backwards compatible method."""
        return self._design_writer.write_two_table_format(*args, **kwargs)

    def writeOneTableExperimentalDesign(self, *args, **kwargs):
        """Backwards compatible method."""
        return self._design_writer.write_one_table_format(*args, **kwargs)

    def save_search_settings_to_file(self, output_filename, sdrf, f2c):
        """Backwards compatible method."""
        return self._save_search_settings_to_file(output_filename, sdrf, f2c)
