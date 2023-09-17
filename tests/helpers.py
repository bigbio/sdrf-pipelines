import difflib
import os

from click import BaseCommand
from click.testing import CliRunner
from click.testing import Result


def compare_files(file1: os.PathLike, file2: os.PathLike) -> list[str]:
    with open(file1, "r") as hosts0:
        with open(file2, "r") as hosts1:
            diff = difflib.unified_diff(
                hosts0.readlines(),
                hosts1.readlines(),
                fromfile=str(file1),
                tofile=str(file2),
            )
    out = list(diff)
    return out


def run_and_check_status_code(command: BaseCommand, args: list[str], status_code: int = 0) -> Result:
    runner = CliRunner()
    result = runner.invoke(command, args)

    assert result.exit_code == status_code, result.output
    return result
