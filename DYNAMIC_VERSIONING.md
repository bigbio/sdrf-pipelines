# Dynamic Versioning for PR Branch Support

This document explains how the project uses dynamic versioning with **setuptools-scm** and a custom local scheme to support downloading ontology cache data from different git branches, including pull requests.

## Overview

The project uses git-based dynamic versioning to:
1. Automatically generate version strings from git state (tags, branches, commits)
2. **Encode branch names directly in the version string**
3. Parse that information to construct the correct GitHub raw URL for pooch downloads

## Version Formats

| Context | Version Example | Parsed Branch | GitHub URL Target |
|---------|----------------|---------------|-------------------|
| Release tag `v0.0.27` | `0.0.27` | `v0.0.27` | `v0.0.27` tag |
| Main branch | `0.0.28.dev5+g1a2b3c` | `main` | `main` branch |
| PR branch `feature/my-feature` | `0.0.28.dev5+feature_my_feature.g1a2b3c` | `feature/my-feature` | `feature/my-feature` branch |
| PR branch `fix/bug-123` | `0.0.28.dev5+fix_bug_123.g1a2b3c` | `fix/bug-123` | `fix/bug-123` branch |

## Setup

### pyproject.toml Configuration

```toml
[build-system]
requires = ["setuptools>=64", "setuptools-scm[toml]>=8"]
build-backend = "setuptools.build_meta"

[project]
name = "sdrf-pipelines"
dynamic = ["version"]

# Note: setuptools_scm configuration with custom local_scheme is in setup.py
# because callables cannot be passed via pyproject.toml
```

### Custom Local Scheme

The `setup.py` file contains a custom `local_scheme_node_and_branch` function that:
- Uses the branch name from the ScmVersion object (provided by setuptools-scm)
- Sanitizes branch names for PEP 440 compatibility
- Encodes branch name in the version string for non-main branches

### Installation

```bash
uv pip install setuptools setuptools-scm
```

## How It Works

### 1. Version Generation

When you install or build:
```bash
uv pip install -e .
```

setuptools-scm:
1. Runs `git describe` to get the base version from tags
2. Detects development commits since last tag
3. Calls custom `local_scheme_node_and_branch()` to add branch info
4. Generates version like `0.0.28.dev5+feature_xyz.g1a2b3c`

### 2. Branch Detection

The `_parse_version_to_branch()` function in `ols.py`:
```python
def _parse_version_to_branch(version: str) -> str:
    """Parse version string to determine git ref (tag or branch)"""
    # Extract branch from: +<branch>.<node>
    local_match = re.search(r'\+([a-zA-Z0-9_]+)(?:\.[a-zA-Z0-9]+)?', version)
    if local_match:
        branch_part = local_match.group(1)
        if branch_part.startswith('g') or branch_part in ('main', 'master'):
            return "main"
        # Restore slashes: feature_my_branch -> feature/my-branch
        return branch_part.replace('_', '/')
    # ... fallback logic
```

### 3. Pooch URL Construction

Automatically downloads from the correct branch:
```
https://raw.githubusercontent.com/bigbio/sdrf-pipelines/{branch}/data/ontologies/
```

## CI/CD Configuration

### GitHub Actions

```yaml
- name: Checkout with tags
  uses: actions/checkout@v4
  with:
    fetch-depth: 0  # Need full history for setuptools-scm

- name: Build package
  run: |
    uv build
```

### For PR Branches

**No special configuration needed!** It just works:

```yaml
- name: Install from PR branch
  run: |
    # Install will use version like: 0.0.28.dev5+feature_xyz.g1a2b3c
    # Pooch will automatically download from feature/xyz branch
    uv pip install -e .
```

## Local Development

```bash
# Switch to PR branch
git checkout feature/new-ontology-data

# Install in editable mode
uv pip install -e .

# Check version - will include branch name
python -c "import sdrf_pipelines; print(sdrf_pipelines.__version__)"
# Output: 0.0.28.dev5+feature_new_ontology_data.g1a2b3c

# Test pooch URL construction
python -c "from sdrf_pipelines.ols.ols import _parse_version_to_branch, __version__; print(_parse_version_to_branch(__version__))"
# Output: feature/new-ontology-data

# Pooch will automatically download from:
# https://raw.githubusercontent.com/bigbio/sdrf-pipelines/feature/new-ontology-data/data/ontologies/
```

## Testing

```bash
# Test version on different branches
git checkout main
python -c "import sdrf_pipelines; print(sdrf_pipelines.__version__)"
# -> 0.0.28.dev5+g1a2b3c (main)

git checkout feature/test
python -c "import sdrf_pipelines; print(sdrf_pipelines.__version__)"
# -> 0.0.28.dev5+feature_test.g1a2b3c (feature/test)

# Test URL parsing
python -c "
from sdrf_pipelines.ols.ols import _parse_version_to_branch
print(_parse_version_to_branch('0.0.27'))  # -> v0.0.27
print(_parse_version_to_branch('0.0.28.dev5+g1a2b3c'))  # -> main
print(_parse_version_to_branch('0.0.28.dev5+feature_xyz.g1a2b3c'))  # -> feature/xyz
"
```

## Advantages

1. **No manual work**: Branch names automatically encoded
2. **No environment variables**: Works out of the box
3. **No git tags needed**: Works on any branch
4. **PR-ready**: Test ontology data changes in PRs before merging
5. **Standards-compliant**: Follows PEP 440
6. **Deterministic**: Same git state = same version

## Branch Name Sanitization

Special characters are replaced with underscores for PEP 440 compatibility:
- `feature/my-branch` → `feature_my_branch`
- `fix/issue-123` → `fix_issue_123`
- `user/feature/test` → `user_feature_test`

The parser automatically restores slashes when constructing URLs.
