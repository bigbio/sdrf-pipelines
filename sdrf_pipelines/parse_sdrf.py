#!/usr/bin/env python3

import click

from sdrf_pipelines.openms.openms import OpenMS

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
  """This is the main tool that give access to all commands to convert SDRF files into pipelines specific configuration files"""


@click.command('convert-openms', short_help='convert sdrf to openms file output')
@click.option('--sdrf', '-s', help='SDRF file')
@click.option('--raw', '-r', help='Keep filenames in experimental design output as raw.')
@click.option('--legacy/--modern', "-l/-m", default=False, help='legacy=Create artifical sample column not needed in OpenMS 2.6.')
@click.option('--onetable/--twotables', "-t1/-t2", default=False, help='Create one-table or two-tables format.')
@click.option('--verbose/--quiet', "-v/-q", default=False, help='Output debug information.')
@click.pass_context
def openms_from_sdrf(ctx, sdrf: str, raw: bool, onetable : bool, legacy: bool, verbose: bool):
  if sdrf is None:
    help()
  OpenMS().openms_convert(sdrf, raw, onetable, legacy, verbose)

cli.add_command(openms_from_sdrf)

if __name__ == "__main__":
  cli()
