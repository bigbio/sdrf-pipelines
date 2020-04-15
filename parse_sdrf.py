#!/usr/bin/env python3

import click

from sdrf_pipelines.openms import OpenMS

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
  """This is the main tool that give access to all commands to convert SDRF files into pipelines specific configuration files"""


@click.command('convert-openms', short_help='convert sdrf to openms file output')
@click.option('--sdrf', '-s', help='SDRF file')
@click.pass_context
def openms_from_sdrf(ctx, sdrf: str):
  if sdrf is None:
    help()
  OpenMS().openms_convert(sdrf)


cli.add_command(openms_from_sdrf)

if __name__ == "__main__":
  cli()
