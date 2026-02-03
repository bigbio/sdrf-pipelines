# Agent Instructions for sdrf-pipelines

## Package Manager

Use `uv` (not pip):
```bash
uv sync --group dev
uv run pytest
uv run pre-commit run --all-files
```

## Related Repositories

- **[sdrf-templates](https://github.com/bigbio/sdrf-templates)**: YAML schema definitions (submodule at `src/sdrf_pipelines/sdrf/sdrf-templates`). Template changes go there, not here.
- **[proteomics-metadata-standard](https://github.com/bigbio/proteomics-metadata-standard)**: SDRF-Proteomics specification and guidelines.

## Style

- Type hints required, checked by `mypy`
- Format with `ruff format`, lint with `ruff check`
- Run `pre-commit run --all-files` before committing

## Testing

- Use `ErrorCode` enums and `ValidationManifest` in tests, not string matching on error messages
- Skip slow ontology tests by default; use `--run-ontology` to include them

## Auto-generated Files

- `COMMANDS.md`: Auto-generated from CLI help. Don't edit manually.

## CLI

```bash
parse_sdrf --help
parse_sdrf validate-sdrf -s file.sdrf.tsv
```
