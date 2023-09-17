import tempfile
from pathlib import Path

import pytest


@pytest.fixture(scope="function")
def on_tmpdir(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp_path:
        monkeypatch.chdir(tmp_path)
        yield Path(tmp_path)
