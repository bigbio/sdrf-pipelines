#!/usr/bin/env python3
"""
Auto-generate CLI documentation from parse_sdrf --help output.

This script runs as a pre-commit hook to keep docs in sync with the CLI.
When parse_sdrf.py changes, this regenerates COMMANDS.md automatically.
"""

import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str]) -> str:
    """Run a command and return its output."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running command {' '.join(cmd)}: {e.stderr}", file=sys.stderr)
        return ""


def parse_options_to_table(options_lines: list[str]) -> str:
    """Parse Click options into a markdown table."""
    if not options_lines:
        return ""

    md = "| Option | Description |\n"
    md += "|--------|-------------|\n"

    current_option = ""
    current_desc: list[str] = []

    for line in options_lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Check if line starts a new option
        if stripped.startswith("-"):
            # Save previous option
            if current_option:
                desc = " ".join(current_desc).strip()
                md += f"| `{current_option}` | {desc} |\n"

            # Parse new option - two spaces separate option from description
            parts = stripped.split("  ", 1)
            current_option = parts[0].strip()
            current_desc = [parts[1].strip()] if len(parts) > 1 else []
        else:
            # Continuation of description
            current_desc.append(stripped)

    # Don't forget last option
    if current_option:
        desc = " ".join(current_desc).strip()
        md += f"| `{current_option}` | {desc} |\n"

    return md


def parse_help_to_section(command_name: str, title: str, help_text: str) -> str:
    """Parse Click help text into a markdown section."""
    lines = help_text.strip().split("\n")

    usage_line = ""
    description_lines = []
    options_lines = []
    in_options = False

    for line in lines:
        if line.startswith("Usage:"):
            usage_line = line.replace("Usage:", "").strip()
        elif line.strip() == "Options:":
            in_options = True
        elif in_options:
            options_lines.append(line)
        elif usage_line and not in_options and line.strip():
            description_lines.append(line.strip())

    # Build markdown section
    md = f"## {title}\n\n"

    if description_lines:
        md += "\n".join(description_lines) + "\n\n"

    md += "```bash\n"
    md += usage_line + "\n"
    md += "```\n\n"

    if options_lines:
        md += parse_options_to_table(options_lines)
        md += "\n"

    return md


def main():
    """Generate COMMANDS.md with all CLI documentation."""
    # Commands to document (command_name, display_title)
    commands = [
        ("validate-sdrf", "Validate SDRF"),
        ("convert-openms", "Convert to OpenMS"),
        ("convert-maxquant", "Convert to MaxQuant"),
        ("convert-msstats", "Convert to MSstats"),
        ("convert-normalyzerde", "Convert to NormalyzerDE"),
        ("split-sdrf", "Split SDRF"),
        ("download-cache", "Download Ontology Cache"),
    ]

    # Paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    output_file = project_root / "COMMANDS.md"

    print("Generating CLI documentation...")

    # Get main help
    main_help = run_command(["parse_sdrf", "--help"])
    if not main_help:
        print("Error: Could not get parse_sdrf --help output", file=sys.stderr)
        sys.exit(1)

    # Build COMMANDS.md
    md = """# CLI Commands Reference

> **Note**: This documentation is auto-generated from `parse_sdrf --help`.
> Do not edit manually - changes will be overwritten.

## Overview

```
"""
    md += main_help
    md += "```\n\n"

    # Generate each command section
    for cmd_name, title in commands:
        print(f"  Generating: {cmd_name}")
        help_text = run_command(["parse_sdrf", cmd_name, "--help"])

        if help_text:
            md += parse_help_to_section(cmd_name, title, help_text)

    # Write output (ensure trailing newline for end-of-file-fixer)
    output_file.write_text(md.rstrip() + "\n")

    print(f"\nGenerated: {output_file}")
    print("CLI documentation generated successfully!")


if __name__ == "__main__":
    main()
