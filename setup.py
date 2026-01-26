"""Setup configuration for sdrf-pipelines with custom version scheme.

This setup.py is used to pass the custom local_scheme callable to setuptools-scm.
Callables cannot be passed via pyproject.toml, so we use setup.py for this purpose.

The custom local_scheme encodes the git branch name into the version string,
which is needed for downloading ontology cache data from the correct branch.
"""

import re

from setuptools import setup


def local_scheme_node_and_branch(version):
    """
    Custom local version scheme that includes branch name.

    This scheme encodes the branch name in the local version part for
    non-main branches, which allows the package to determine which
    GitHub branch to download cached ontology data from.

    Args:
        version: ScmVersion object from setuptools-scm containing:
            - node: the git commit hash
            - branch: the current git branch name
            - dirty: whether there are uncommitted changes
            - distance: number of commits since last tag

    Returns:
        For main/master branches: "+g<node>" (standard format)
        For feature branches: "+<sanitized_branch>.g<node>"
        Empty string for exact tag matches (distance == 0)
    """
    # For exact tag matches (release), no local version
    if version.exact:
        return ""

    # Get branch name from the version object
    branch = version.branch or "unknown"

    # Sanitize branch name for PEP 440 compliance
    sanitized_branch = re.sub(r"[^a-zA-Z0-9]", "_", branch)

    # Get the node (commit hash)
    node = version.node or ""

    # For main/master/HEAD, use standard format without branch name
    if sanitized_branch in ("main", "master", "HEAD"):
        return f"+g{node}" if node else ""

    # For feature branches, include the branch name
    if node:
        return f"+{sanitized_branch}.g{node}"
    else:
        return f"+{sanitized_branch}"


setup(
    use_scm_version={
        "write_to": "src/sdrf_pipelines/_version.py",
        "write_to_template": "__version__ = '{version}'",
        "local_scheme": local_scheme_node_and_branch,
    }
)
