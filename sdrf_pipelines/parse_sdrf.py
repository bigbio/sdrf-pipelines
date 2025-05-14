#!/usr/bin/env python3

import csv
import logging
import os
import re
import sys

import click
import pandas as pd

from sdrf_pipelines import __version__
from sdrf_pipelines.maxquant.maxquant import Maxquant
from sdrf_pipelines.msstats.msstats import Msstats
from sdrf_pipelines.normalyzerde.normalyzerde import NormalyzerDE
from sdrf_pipelines.ols.ols import OlsClient
from sdrf_pipelines.openms.openms import OpenMS
from sdrf_pipelines.sdrf.schemas import SchemaRegistry, SchemaValidator
from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame, read_sdrf
from sdrf_pipelines.utils.exceptions import AppConfigException

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.version_option(
    version=__version__,
    package_name="sdrf_pipelines",
    message="%(package)s %(version)s",
)
@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """
    This tool validates SDRF files and can convert them for use in data analysis pipelines.
    """


@click.command("convert-openms", short_help="convert sdrf to openms file output")
@click.option("--sdrf", "-s", help="SDRF file")
@click.option(
    "--legacy/--modern",
    "-l/-m",
    default=False,
    help="legacy=Create artificial sample column not needed in OpenMS 2.6.",
)
@click.option(
    "--onetable/--twotables",
    "-t1/-t2",
    help="Create one-table or two-tables format.",
    default=False,
)
@click.option("--verbose/--quiet", "-v/-q", help="Output debug information.", default=False)
@click.option(
    "--conditionsfromcolumns",
    "-c",
    help="Create conditions from provided (e.g., factor) columns.",
)
@click.option(
    "--extension_convert",
    "-e",
    help=(
        "convert extensions of files from one type to other 'raw:mzML,mzml:MZML,d:d'. "
        "The original extensions are case insensitive"
    ),
)
@click.pass_context
def openms_from_sdrf(
    ctx,
    sdrf: str,
    onetable: bool,
    legacy: bool,
    verbose: bool,
    conditionsfromcolumns: str,
    extension_convert: str,
):
    if sdrf is None:
        help()
    try:
        OpenMS().openms_convert(sdrf, onetable, legacy, verbose, conditionsfromcolumns, extension_convert)
    except Exception as ex:
        msg = "Error: " + str(ex)
        raise ValueError(msg) from ex


@click.command(
    "convert-maxquant",
    short_help="convert sdrf to maxquant parameters file and generate an experimental design file",
)
@click.option("--sdrf", "-s", help="SDRF file", required=True)
@click.option("--fastafilepath", "-f", help="protein database file path", required=True)
@click.option("--mqconfdir", "-mcf", help="MaxQuant default configure path")
@click.option(
    "--matchbetweenruns",
    "-m",
    help="via matching between runs to boosts number of identifications",
    default="True",
)
@click.option(
    "--peptidefdr",
    "-pef",
    help="posterior error probability calculation based on target-decoy search",
    default=0.01,
)
@click.option(
    "--proteinfdr",
    "-prf",
    help="protein score = product of peptide PEPs (one for each sequence)",
    default=0.01,
)
@click.option(
    "--tempfolder",
    "-t",
    help="temporary folder: place on SSD (if possible) for faster search",
    default="",
)
@click.option("--raw_folder", "-r", help="spectrum raw data folder", required=True)
@click.option(
    "--numthreads",
    "-n",
    help="each thread needs at least 2 GB of RAM,number of threads should be ≤ number of logical cores "
    "available "
    "(otherwise, MaxQuant can crash)",
    default=1,
)
@click.option(
    "--output1",
    "-o1",
    help="parameters .xml file  output file path",
    default="./mqpar.xml",
)
@click.option(
    "--output2",
    "-o2",
    help="maxquant experimental design .txt file",
    default="./exp_design.xml",
)
@click.pass_context
def maxquant_from_sdrf(
    ctx,
    sdrf: str,
    fastafilepath: str,
    mqconfdir: str,
    matchbetweenruns: bool,
    peptidefdr,
    proteinfdr,
    tempfolder: str,
    raw_folder: str,
    numthreads: int,
    output1: str,
    output2: str,
):
    if sdrf is None:
        help()

    Maxquant().maxquant_convert(
        sdrf,
        fastafilepath,
        mqconfdir,
        matchbetweenruns,
        peptidefdr,
        proteinfdr,
        tempfolder,
        raw_folder,
        numthreads,
        output1,
    )
    Maxquant().maxquant_experiamental_design(sdrf, output2)


@click.command("validate-sdrf", short_help="Command to validate the sdrf file")
@click.option("--sdrf_file", "-s", help="SDRF file to be validated")
@click.option(
    "--template",
    "-t",
    help="select the template that will be use to validate the file (default: default)",
    default="default",
    required=False,
)
@click.option(
    "--use_ols_cache_only", help="Use ols cache for validation of the terms and not OLS internet service", is_flag=True
)
@click.pass_context
def validate_sdrf(
    ctx,
    sdrf_file: str,
    template: str,
    use_ols_cache_only: bool,
):
    """
    Command to validate the SDRF file. The validation is based on the template provided by the user.
    User can select the template to be used for validation. If no template is provided, the default template will
    be used. Additionally, the mass spectrometry fields and factor values can be validated separately. However, if
    the mass spectrometry validation or factor value validation is skipped, the user will be warned about it.

    @param sdrf_file: SDRF file to be validated
    @param template: template to be used for a validation
    @param use_ols_cache_only: flag to use the OLS cache for validation of the terms and not OLS internet service
    """

    if sdrf_file is None:
        msg = "The config file for the pipeline is missing, please provide one "
        logging.error(msg)
        raise AppConfigException(msg)

    if template is None:
        template = "default"

    registry = SchemaRegistry()  # Default registry, but users can create their own
    validator = SchemaValidator(registry)
    sdrf_df = SDRFDataFrame(read_sdrf(sdrf_file))

    errors = validator.validate(sdrf_df, template, use_ols_cache_only)
    errors_not_warnings = [error for error in errors if error.error_type == logging.ERROR]

    for error in errors:
        if error.error_type == logging.ERROR:
            click.secho(f"{error.message}", fg="red")
        elif error.error_type == logging.WARNING:
            click.secho(f"{error.message}", fg="yellow")
        else:
            click.secho(f"{error.message}", fg="green")

    if not errors:
        click.secho("Everything seems to be fine. Well done.", fg="green")
    else:
        click.secho("There were validation errors.", fg="red")

    sys.exit(bool(errors_not_warnings))


@click.command("split-sdrf", short_help="Command to split the sdrf file")
@click.option("--sdrf_file", "-s", help="SDRF file to be split", required=True)
@click.option(
    "--attribute",
    "-a",
    help="property to split, Multiple attributes are separated by commas",
    required=True,
)
@click.option("--prefix", "-p", help="file prefix to be added to the sdrf file name")
@click.pass_context
def split_sdrf(ctx, sdrf_file: str, attribute: str, prefix: str):
    pattern = re.compile(r"\]\.\d+\t")
    df = pd.read_csv(sdrf_file, sep="\t", skip_blank_lines=False)
    attributes = attribute.split(",")
    d = dict(tuple(df.groupby(attributes)))
    for key in d:
        dataframe = d[key]
        file_name = os.path.split(sdrf_file)[-1]
        path = os.path.split(sdrf_file)[0] + "/"
        if prefix is None:
            if len(file_name.split(".")) > 2:
                prefix = ".".join(file_name.split(".")[:-2])
            else:
                prefix = file_name.split(".")[0]
        if isinstance(key, tuple):
            new_file = prefix + "-" + "-".join(key).replace(" ", "_") + ".sdrf.tsv"
        else:
            new_file = prefix + "-" + key.replace(" ", "_") + ".sdrf.tsv"
        dataframe.to_csv(path + new_file, sep="\t", quoting=csv.QUOTE_NONE, index=False)

        # Handling duplicate column names
        with open(path + new_file, "r+", encoding="utf-8") as f:
            data = f.read()
        data = pattern.sub("]\t", data)
        with open(path + new_file, "w", encoding="utf-8") as f:
            f.write(data)


@click.command("convert-msstats", short_help="convert sdrf to msstats annotation file")
@click.option("--sdrf", "-s", help="SDRF file", required=True)
@click.option(
    "--conditionsfromcolumns",
    "-c",
    help="Create conditions from provided (e.g., factor) columns.",
)
@click.option("--outpath", "-o", help="annotation out file path", required=True)
@click.option(
    "--openswathtomsstats",
    "-swath",
    help="from openswathtomsstats output to msstats",
    default=False,
)
@click.option("--maxqtomsstats", "-mq", help="from maxquant output to msstats", default=False)
@click.pass_context
def msstats_from_sdrf(ctx, sdrf, conditionsfromcolumns, outpath, openswathtomsstats, maxqtomsstats):
    Msstats().convert_msstats_annotation(sdrf, conditionsfromcolumns, outpath, openswathtomsstats, maxqtomsstats)


@click.command("convert-normalyzerde", short_help="convert sdrf to NormalyzerDE design file")
@click.option("--sdrf", "-s", help="SDRF file", required=True)
@click.option(
    "--conditionsfromcolumns",
    "-c",
    help="Create conditions from provided (e.g., factor) columns.",
)
@click.option("--outpath", "-o", help="annotation out file path", required=True)
@click.option("--outpathcomparisons", "-oc", help="out file path for comparisons", default="")
@click.option(
    "--maxquant_exp_design_file",
    "-mq",
    help="Path to maxquant experimental design file for mapping MQ sample names",
    default="",
)
@click.pass_context
def normalyzerde_from_sdrf(
    ctx,
    sdrf,
    conditionsfromcolumns,
    outpath,
    outpathcomparisons,
    maxquant_exp_design_file,
):
    NormalyzerDE().convert_normalyzerde_design(
        sdrf,
        conditionsfromcolumns,
        outpath,
        outpathcomparisons,
        maxquant_exp_design_file,
    )


@click.command("build-index-ontology", short_help="Convert an ontology file to an index file")
@click.option("--ontology", "-in", help="ontology file")
@click.option("--index", "-out", help="Output file in parquet format")
@click.option("--ontology_name", "-name", help="ontology name")
@click.pass_context
def build_index_ontology(ctx, ontology: str, index: str, ontology_name: str | None = None):
    ols_client = OlsClient()

    if ontology.lower().endswith(".owl") and ontology_name is None:
        raise ValueError("Please provide the ontology name for the owl file")

    ols_client.build_ontology_index(ontology, index, ontology_name)


cli.add_command(validate_sdrf)
cli.add_command(openms_from_sdrf)
cli.add_command(maxquant_from_sdrf)
cli.add_command(split_sdrf)
cli.add_command(msstats_from_sdrf)
cli.add_command(normalyzerde_from_sdrf)
cli.add_command(build_index_ontology)


@click.command("validate-sdrf-simple", short_help="Simple command to validate the sdrf file.")
@click.argument("sdrf_file", type=click.Path(exists=True))
@click.option("--template", "-t", default="default", help="The template to validate against.")
@click.option(
    "--use_ols_cache_only", is_flag=True, help="Use only the OLS cache for validation. This option is deprecated."
)
def validate_sdrf_simple(sdrf_file: str, template: str, use_ols_cache_only: bool):
    """
    Simple command to validate an SDRF file.

    This command provides a simpler interface for validating SDRF files,
    without the additional options for skipping specific validations.
    """

    registry = SchemaRegistry()  # Default registry, but users can create their own
    validator = SchemaValidator(registry)
    sdrf_df = SDRFDataFrame(read_sdrf(sdrf_file))

    errors = validator.validate(sdrf_df, template, use_ols_cache_only)
    if errors:
        for error in errors:
            if error.error_type == logging.ERROR:
                click.secho(f"ERROR: {error.message}", fg="red")
            else:
                click.secho(f"WARNING: {error.message}", fg="yellow")
        errors_not_warnings = [error for error in errors if error.error_type != logging.ERROR]
        if len(errors_not_warnings):
            sys.exit(1)
    else:
        click.secho("SDRF file is valid!", fg="green")


cli.add_command(validate_sdrf_simple)


def main():
    try:
        cli()
    except SystemExit as e:
        if e.code != 0:
            raise


if __name__ == "__main__":
    main()
