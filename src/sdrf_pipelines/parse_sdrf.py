#!/usr/bin/env python3

import csv
import json
import logging
import os
import re
import sys
from typing import Optional

import click
import pandas as pd
import yaml

from sdrf_pipelines import __version__
from sdrf_pipelines.converters.maxquant.maxquant import Maxquant
from sdrf_pipelines.converters.msstats.msstats import Msstats
from sdrf_pipelines.converters.normalyzerde.normalyzerde import NormalyzerDE
from sdrf_pipelines.converters.openms.openms import OpenMS
from sdrf_pipelines.ols.ols import (
    OLS_AVAILABLE,
    ONTOLOGY_FILES,
    OlsClient,
    download_ontology_cache,
)
from sdrf_pipelines.sdrf.schemas import SchemaRegistry, SchemaValidator
from sdrf_pipelines.sdrf.sdrf import read_sdrf
from sdrf_pipelines.utils.exceptions import AppConfigException
from sdrf_pipelines.utils.utils import ValidationProof

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
@click.option(
    "--skip-ontology",
    help="Skip ontology term validation (useful when ontology dependencies are not installed)",
    is_flag=True,
)
@click.option(
    "--out",
    "-o",
    help="Output file to write the validation results to (default: stdout)",
    default=None,
    required=False,
)
@click.option(
    "--proof_out",
    "-po",
    help="Output file to write the validation proof",
    default=None,
    required=False,
)
@click.option(
    "--generate_proof",
    help="Generate cryptographic proof of validation",
    is_flag=True,
)
@click.option(
    "--proof_salt",
    help="Optional user-provided salt for proof generation",
    default=None,
)
@click.pass_context
def validate_sdrf(
    ctx,
    sdrf_file: str,
    template: str,
    use_ols_cache_only: bool,
    skip_ontology: bool,
    out: Optional[str] = None,
    proof_out: Optional[str] = None,
    generate_proof: bool = False,
    proof_salt: Optional[str] = None,
):
    """
    Command to validate the SDRF file. The validation is based on the template provided by the user.
    User can select the template to be used for validation. If no template is provided, the default template will
    be used. Additionally, the mass spectrometry fields and factor values can be validated separately. However, if
    the mass spectrometry validation or factor value validation is skipped, the user will be warned about it.

    @param sdrf_file: SDRF file to be validated
    @param template: template to be used for a validation
    @param use_ols_cache_only: flag to use the OLS cache for validation of the terms and not OLS internet service
    @param skip_ontology: flag to skip ontology term validation
    @param out: Output file to write the validation results to (default: stdout)
    """
    # Warn if ontology validation is requested but dependencies are not available
    if not skip_ontology and not OLS_AVAILABLE:
        click.secho(
            "Warning: Ontology validation is not available because OLS dependencies are not installed. "
            "Install them with: pip install sdrf-pipelines[ontology]. "
            "Continuing with structural validation only.",
            fg="yellow",
        )
        skip_ontology = True

    if sdrf_file is None:
        msg = "The config file for the pipeline is missing, please provide one "
        logging.error(msg)
        raise AppConfigException(msg)

    if template is None:
        template = "default"

    registry = SchemaRegistry()
    validator = SchemaValidator(registry)
    sdrf_df = read_sdrf(sdrf_file)
    validation_proof = ValidationProof()
    template_content = ""
    if generate_proof:
        try:
            if hasattr(registry, "raw_schema_data") and template in registry.raw_schema_data:
                template_content = yaml.dump(registry.raw_schema_data[template], sort_keys=True)
            else:
                schema_dir = os.path.join(os.path.dirname(__file__), "sdrf", "schemas")
                template_file = os.path.join(schema_dir, f"{template}.yaml")
                if os.path.exists(template_file):
                    with open(template_file, "r", encoding="utf-8") as f:
                        template_content = f.read()
        except Exception as e:
            logging.warning("Could not load template content for proof generation: %s", e)

    errors = validator.validate(sdrf_df, template, use_ols_cache_only, skip_ontology=skip_ontology)
    errors_not_warnings = [error for error in errors if error.error_type == logging.ERROR]
    error_list = []
    for error in errors:
        if error.error_type == logging.ERROR:
            error_list.append({"file": sdrf_file, "type": "ERROR", "message": error.message})
        elif error.error_type == logging.WARNING:
            error_list.append({"file": sdrf_file, "type": "WARNING", "message": error.message})
        else:
            error_list.append({"file": sdrf_file, "type": "INFO", "message": error.message})
            click.secho(f"{error.message}", fg="green")

    error_df = pd.DataFrame(error_list)
    error_df = error_df.drop_duplicates()
    if out is not None:
        error_df.to_csv(out, sep="\t", index=False)

    for _, row in error_df.iterrows():
        if row["type"] == "ERROR":
            click.secho(f"ERROR: {row['message']}", fg="red")
        elif row["type"] == "WARNING":
            click.secho(f"WARNING: {row['message']}", fg="yellow")
        else:
            click.secho(f"{row['message']}", fg="green")
    if not errors:
        click.secho("Everything seems to be fine. Well done.", fg="green")
    elif error_df[error_df["type"] == "WARNING"].shape[0] > 0 and not errors_not_warnings:
        click.secho("Most seems to be fine. There were only warnings.", fg="yellow")
    else:
        click.secho("There were validation errors.", fg="red")

    if generate_proof or proof_out:
        try:
            proof = validation_proof.generate_validation_proof(
                sdrf_df=sdrf_df, validator_version=__version__, template_content=template_content, user_salt=proof_salt
            )
            proof_output = json.dumps(proof, indent=2)
            if proof_out:
                with open(proof_out, "w", encoding="utf-8") as f:
                    f.write(proof_output)
                click.secho(f"Validation proof generated: {proof_out}", fg="blue")
            else:
                click.secho(f"SDRF Hash: {proof['sdrf_hash']}", fg="blue")
                click.secho(f"Template Hash: {proof['template_hash']}", fg="blue")
                click.secho(f"Validator Version: {proof['validator_version']}", fg="blue")
                click.secho(f"Timestamp: {proof['timestamp']}", fg="blue")
            click.secho(f"Proof hash: {proof['proof_hash']}", fg="blue")

        except Exception as e:
            click.secho(f"Warning: Could not generate validation proof: {e}", fg="yellow")

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
            new_file = prefix + "-" + "-".join(str(k) for k in key).replace(" ", "_") + ".sdrf.tsv"
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
@click.option(
    "--skip-ontology",
    is_flag=True,
    help="Skip ontology term validation (useful when ontology dependencies are not installed)",
)
def validate_sdrf_simple(sdrf_file: str, template: str, use_ols_cache_only: bool, skip_ontology: bool):
    """
    Simple command to validate an SDRF file.

    This command provides a simpler interface for validating SDRF files,
    without the additional options for skipping specific validations.
    """
    # Warn if ontology validation is requested but dependencies are not available
    if not skip_ontology and not OLS_AVAILABLE:
        click.secho(
            "Warning: Ontology validation is not available because OLS dependencies are not installed. "
            "Install them with: pip install sdrf-pipelines[ontology]. "
            "Continuing with structural validation only.",
            fg="yellow",
        )
        skip_ontology = True

    registry = SchemaRegistry()  # Default registry, but users can create their own
    validator = SchemaValidator(registry)
    sdrf_df = read_sdrf(sdrf_file)

    errors = validator.validate(sdrf_df, template, use_ols_cache_only, skip_ontology=skip_ontology)
    if errors:
        for error in errors:
            if error.error_type == logging.ERROR:
                click.secho(f"ERROR: {error.message}", fg="red")
            else:
                click.secho(f"WARNING: {error.message}", fg="yellow")
        actual_errors = [error for error in errors if error.error_type == logging.ERROR]
        if len(actual_errors):
            sys.exit(1)
    else:
        click.secho("SDRF file is valid!", fg="green")


@click.command("download-cache", short_help="Download ontology cache files from GitHub")
@click.option(
    "--ontology",
    "-o",
    help=(
        "Comma-separated list of specific ontologies to download (e.g., 'efo,cl'). "
        "If not specified, downloads all ontologies."
    ),
    default=None,
)
@click.option(
    "--cache-dir",
    "-d",
    help="Override the default cache directory location",
    default=None,
)
@click.option(
    "--show-info",
    "-i",
    is_flag=True,
    help="Show cache information (location, available ontologies, download URLs) without downloading",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force re-download even if files already exist in cache",
)
def download_cache(ontology, cache_dir, show_info, force):
    """
    Download ontology cache files from GitHub for offline validation.

    By default, ontology cache files are automatically downloaded on first use during validation.
    Use this command to pre-download all or specific ontology files.

    Examples:
        parse_sdrf download-cache                    # Download all ontologies
        parse_sdrf download-cache -o efo,cl          # Download only EFO and CL
        parse_sdrf download-cache --show-info        # Show cache information
        parse_sdrf download-cache -f                 # Force re-download all files
    """
    if not OLS_AVAILABLE:
        click.secho(
            "Error: Ontology dependencies not installed. Install with: pip install sdrf-pipelines[ontology]",
            fg="red",
        )
        sys.exit(1)

    if show_info:
        # Show cache information
        from sdrf_pipelines.ols.ols import ONTOLOGY_POOCH, __version__

        click.secho("=== Ontology Cache Information ===", fg="cyan", bold=True)
        click.echo(f"Cache directory: {ONTOLOGY_POOCH.path}")
        click.echo(f"Package version: {__version__}")
        click.echo(f"Base URL: {ONTOLOGY_POOCH.base_url}")
        click.echo(f"\nAvailable ontologies ({len(ONTOLOGY_FILES)}):")

        for ont_file in ONTOLOGY_FILES:
            ont_name = ont_file.replace(".parquet", "").upper()
            url = f"{ONTOLOGY_POOCH.base_url}{ont_file}"
            # Check if file exists in cache
            from pathlib import Path

            cache_path = Path(ONTOLOGY_POOCH.path) / ont_file
            status = "✓ cached" if cache_path.exists() else "✗ not cached"
            click.echo(f"  - {ont_name:12s} {status:15s} {url}")

        return

    # Parse ontology list if provided
    ontologies_to_download = None
    if ontology:
        ontologies_to_download = [ont.strip().lower() for ont in ontology.split(",")]
        click.echo(f"Downloading {len(ontologies_to_download)} ontologies: {', '.join(ontologies_to_download)}")
    else:
        click.echo(f"Downloading all {len(ONTOLOGY_FILES)} ontologies...")

    # Download files
    try:
        downloaded_files = download_ontology_cache(
            ontologies=ontologies_to_download,
            cache_dir=cache_dir,
            force=force,
        )
        click.secho(f"\n✓ Successfully downloaded {len(downloaded_files)} ontology files", fg="green", bold=True)

        if cache_dir:
            click.echo(f"Cache location: {cache_dir}")
        else:
            from sdrf_pipelines.ols.ols import ONTOLOGY_POOCH

            click.echo(f"Cache location: {ONTOLOGY_POOCH.path}")

        click.echo("\nYou can now use SDRF validation without internet connectivity.")

    except Exception as e:
        click.secho(f"\n✗ Download failed: {e}", fg="red", bold=True)
        sys.exit(1)


@click.command("list-templates", short_help="List all available SDRF templates with their versions")
@click.option(
    "--format",
    "-f",
    type=click.Choice(["table", "json", "yaml"], case_sensitive=False),
    default="table",
    help="Output format (table, json, or yaml)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Include detailed information (description, extends, layer)",
)
def list_templates(format: str, verbose: bool):
    """
    List all available SDRF templates with their versions.

    Shows template names, latest versions, and optionally detailed information
    like descriptions and inheritance relationships.

    Examples:
        parse_sdrf list-templates                    # Simple table view
        parse_sdrf list-templates -v                 # Detailed table view
        parse_sdrf list-templates -f json            # JSON output
        parse_sdrf list-templates -f yaml -v         # Detailed YAML output
    """
    registry = SchemaRegistry()

    if not hasattr(registry, "manifest") or not registry.manifest:
        click.secho("Error: Could not load templates manifest", fg="red")
        sys.exit(1)

    templates = registry.manifest.get("templates", {})

    if not templates:
        click.secho("No templates found", fg="yellow")
        return

    if format == "json":
        if verbose:
            output = templates
        else:
            output = {
                name: {"latest": info["latest"], "versions": info["versions"]} for name, info in templates.items()
            }
        click.echo(json.dumps(output, indent=2))

    elif format == "yaml":
        if verbose:
            output = templates
        else:
            output = {
                name: {"latest": info["latest"], "versions": info["versions"]} for name, info in templates.items()
            }
        click.echo(yaml.dump(output, default_flow_style=False, sort_keys=False))

    else:  # table format
        click.secho("\n=== Available SDRF Templates ===\n", fg="cyan", bold=True)

        # Sort templates by name
        sorted_templates = sorted(templates.items())

        for name, info in sorted_templates:
            latest = info.get("latest", "N/A")
            versions = ", ".join(info.get("versions", []))
            usable_alone = info.get("usable_alone", False)

            # Color coding: green for usable standalone, yellow for must combine
            name_color = "green" if usable_alone else "yellow"

            click.secho(f"{name}", fg=name_color, bold=True, nl=False)
            click.echo(f" (latest: {latest})")

            if verbose:
                click.echo(f"  Versions: {versions}")
                if info.get("extends"):
                    click.echo(f"  Extends: {info['extends']}")
                if info.get("layer"):
                    click.echo(f"  Layer: {info['layer']}")
                if info.get("description"):
                    # Wrap description at 80 characters
                    desc = info["description"]
                    click.echo(f"  Description: {desc}")
                click.echo(f"  Usable alone: {'Yes' if usable_alone else 'No (must combine with other templates)'}")
                click.echo()
            else:
                click.echo(f"  All versions: {versions}")
                click.echo()

        click.secho(f"Total templates: {len(templates)}", fg="cyan")
        click.echo("\nNote: Green = standalone templates, Yellow = must be combined with others")
        click.echo("Use --verbose/-v for detailed information")


cli.add_command(download_cache)
cli.add_command(validate_sdrf_simple)
cli.add_command(list_templates)


def main():
    try:
        cli()
    except SystemExit as e:
        if e.code != 0:
            raise


if __name__ == "__main__":
    main()
