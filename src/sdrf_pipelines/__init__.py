import re
import subprocess
from pathlib import Path


def _get_version_from_git():
    """Get version from git with branch encoding."""
    try:
        repo_root = Path(__file__).parent.parent.parent

        # Get base version from last tag
        tag = (
            subprocess.check_output(
                ["git", "describe", "--tags", "--abbrev=0"], stderr=subprocess.DEVNULL, text=True, cwd=repo_root
            )
            .strip()
            .lstrip("v")
        )

        # Get current branch
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], stderr=subprocess.DEVNULL, text=True, cwd=repo_root
        ).strip()

        # Get full git describe
        desc = subprocess.check_output(
            ["git", "describe", "--tags", "--long"], stderr=subprocess.DEVNULL, text=True, cwd=repo_root
        ).strip()

        parts = desc.split("-")
        if len(parts) >= 3:
            commits = parts[-2]
            commit_hash = parts[-1]

            # If no commits since tag, it's a release
            if commits == "0":
                return tag

            # Otherwise it's a dev version
            if branch in ("main", "master", "HEAD"):
                return f"{tag}.dev{commits}+{commit_hash}"
            else:
                # Sanitize branch name for PEP 440
                safe_branch = re.sub(r"[^a-zA-Z0-9]", "_", branch)
                return f"{tag}.dev{commits}+{safe_branch}.{commit_hash}"

        return tag
    except Exception:
        return None


# Try importlib.metadata first (for installed packages)
try:
    from importlib.metadata import version as _get_version

    __version__ = _get_version("sdrf-pipelines")
    # If it's 0.0.0, it means setuptools-scm failed, try git
    if __version__ == "0.0.0":
        git_version = _get_version_from_git()
        if git_version:
            __version__ = git_version
except Exception:
    # Fallback to git-based version
    __version__ = _get_version_from_git() or "dev"
