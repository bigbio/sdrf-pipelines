"""Custom setuptools-scm version scheme with branch name encoding."""
import subprocess
import re


def get_local_node_and_branch(version):
    """
    Custom local version scheme that includes branch name.
    
    Returns:
        For main/master: "+<node>"
        For other branches: "+<branch>.<node>"
    """
    # Get branch name
    try:
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        branch = "unknown"
    
    # Sanitize branch name for PEP 440
    branch = re.sub(r'[^a-zA-Z0-9]', '_', branch)
    
    # Get node (commit hash)
    node = version.node if hasattr(version, 'node') else ''
    
    # Skip branch prefix for main/master
    if branch in ('main', 'master', 'HEAD'):
        return f"+{node}" if node else ""
    
    # Include branch name for feature branches
    return f"+{branch}.{node}" if node else f"+{branch}"
