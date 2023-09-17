# CONTRIBUTING

# Testing

```
python -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt -r requirements.txt
pytest tests
```

# Code formatting

We delegate code formatting to isort and black.
To pass the CI tests, the code must adhere to the code formatting standards set in `pyproject.toml` file.

You can install the required code formatters either with

```
pip install -r requirements-dev.txt
```

or

```
pip install isort black
```

As a convenience, we provide pre-commit hooks.
They can be installed with

```
pre-commit install --install-hooks
```
