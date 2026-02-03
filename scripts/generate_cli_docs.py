#!/usr/bin/env python3
"""
Auto-generate CLI documentation from parse_sdrf --help output.

This script runs as a pre-commit hook to keep docs in sync with the CLI.
When parse_sdrf.py changes, this regenerates the documentation automatically.
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


def parse_help_to_markdown(command_name: str, help_text: str) -> str:
    """Parse Click help text into markdown format."""
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
        elif line.strip() and not usage_line:
            # Description comes before usage in Click
            continue
        elif line.strip() and usage_line and not in_options:
            description_lines.append(line.strip())

    # Build markdown
    md = f"# {command_name}\n\n"

    if description_lines:
        md += "\n".join(description_lines) + "\n\n"

    md += "## Usage\n\n"
    md += f"```bash\n{usage_line}\n```\n\n"

    if options_lines:
        md += "## Options\n\n"
        md += "| Option | Description |\n"
        md += "|--------|-------------|\n"

        current_option = ""
        current_desc = []

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

                # Parse new option
                # Handle options like "-s, --sdrf_file PATH"
                parts = stripped.split("  ", 1)  # Two spaces separate option from desc
                if len(parts) >= 1:
                    current_option = parts[0].strip()
                    current_desc = [parts[1].strip()] if len(parts) > 1 else []
                else:
                    current_option = stripped
                    current_desc = []
            else:
                # Continuation of description
                current_desc.append(stripped)

        # Don't forget last option
        if current_option:
            desc = " ".join(current_desc).strip()
            md += f"| `{current_option}` | {desc} |\n"

        md += "\n"

    return md


def main():
    """Generate CLI documentation for all parse_sdrf commands."""
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
    docs_dir = project_root / "docs"
    cli_dir = docs_dir / "cli"
    commands_dir = cli_dir / "commands"

    # Ensure directories exist
    commands_dir.mkdir(parents=True, exist_ok=True)

    # Get main help
    print("Generating CLI documentation...")
    main_help = run_command(["parse_sdrf", "--help"])

    if not main_help:
        print("Error: Could not get parse_sdrf --help output", file=sys.stderr)
        sys.exit(1)

    # Generate individual command docs
    for cmd_name, title in commands:
        print(f"  Generating: {cmd_name}")
        help_text = run_command(["parse_sdrf", cmd_name, "--help"])

        if help_text:
            doc = parse_help_to_markdown(cmd_name, help_text)
            cmd_file = commands_dir / f"{cmd_name}.md"
            cmd_file.write_text(doc)

    # Generate main CLI reference
    cli_ref = """# CLI Reference

This documentation is auto-generated from `parse_sdrf --help`.

## Installation

```bash
pip install sdrf-pipelines
```

## Main Command

```
"""
    cli_ref += main_help
    cli_ref += """```

## Commands

"""

    for cmd_name, title in commands:
        cmd_file = commands_dir / f"{cmd_name}.md"
        if cmd_file.exists():
            # Include command doc (skip the h1 header)
            content = cmd_file.read_text()
            lines = content.split("\n")
            # Skip first line (# command-name) and add as h3
            cli_ref += f"### {title}\n\n"
            cli_ref += "\n".join(lines[2:]) + "\n"

    # Write main reference
    cli_ref_file = cli_dir / "reference.md"
    cli_ref_file.write_text(cli_ref)

    print(f"\nGenerated:")
    print(f"  - {cli_ref_file}")
    for cmd_name, _ in commands:
        print(f"  - {commands_dir / f'{cmd_name}.md'}")

    print("\nCLI documentation generated successfully!")


if __name__ == "__main__":
    main()
