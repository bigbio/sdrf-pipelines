#!/usr/bin/env python3

import click
import logging

from sdrf_pipelines.openms.openms import OpenMS
from sdrf_pipelines.sdrf.sdrf import SdrfDataFrame
from sdrf_pipelines.sdrf.sdrf_schema import MASS_SPECTROMETRY, ALL_TEMPLATES, DEFAULT_TEMPLATE
from sdrf_pipelines.utils.exceptions import AppConfigException

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
  """
  This is the main tool that give access to all commands to convert SDRF files into pipelines specific configuration files
  """


@click.command('convert-openms', short_help='convert sdrf to openms file output')
@click.option('--sdrf', '-s', help='SDRF file')
@click.option('--raw', '-r', help='Keep filenames in experimental design output as raw.')
@click.option('--legacy/--modern', "-l/-m", default=False,
              help='legacy=Create artifical sample column not needed in OpenMS 2.6.')
@click.option('--onetable/--twotables', "-t1/-t2", default=False, help='Create one-table or two-tables format.')
@click.option('--verbose/--quiet', "-v/-q", default=False, help='Output debug information.')
@click.pass_context
def openms_from_sdrf(ctx, sdrf: str, raw: bool, onetable: bool, legacy: bool, verbose: bool):
  if sdrf is None:
    help()
  OpenMS().openms_convert(sdrf, raw, onetable, legacy, verbose)


@click.command('validate-sdrf', short_help='Command to validate the sdrf file')
@click.option('--sdrf_file', '-s', help='SDRF file to be validated')
@click.option('--template', '-t', help='select the template that will be use to validate the file (default: default)',
              default='default', type=click.Choice(ALL_TEMPLATES, case_sensitive=False), required=False)
@click.option('--check_ms', help='check mass spectrometry fields in SDRF (e.g. postranslational modifications)',
              is_flag=True)
@click.pass_context
def validate_sdrf(ctx, sdrf_file: str, template: str, check_ms):
  if sdrf_file is None:
    msg = "The config file for the pipeline is missing, please provide one "
    logging.error(msg)
    raise AppConfigException(msg)
  if template is None:
    template = DEFAULT_TEMPLATE

  df = SdrfDataFrame.parse(sdrf_file)
  errors = df.validate(template)

  if check_ms:
    errors = errors + df.validate(MASS_SPECTROMETRY)

  for error in errors:
    print(error)

  # provide some info to the user, as no info is confusing
  print('Everything seems to fine. Well done.')


cli.add_command(validate_sdrf)
cli.add_command(openms_from_sdrf)


def main():
  cli()


if __name__ == "__main__":
  main()
