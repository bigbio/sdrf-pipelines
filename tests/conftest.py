import tempfile
from pathlib import Path

import pytest

from sdrf_pipelines.ols.ols import OLS_AVAILABLE


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "ontology: tests that require ontology dependencies")


def pytest_collection_modifyitems(config, items):
    """Skip ontology tests if dependencies are not available."""
    if OLS_AVAILABLE:
        # Ontology dependencies are installed, run all tests
        return

    skip_ontology = pytest.mark.skip(reason="OLS dependencies not installed")
    for item in items:
        if "ontology" in item.keywords:
            item.add_marker(skip_ontology)


@pytest.fixture(scope="function")
def on_tmpdir(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp_path:
        monkeypatch.chdir(tmp_path)
        yield Path(tmp_path)
