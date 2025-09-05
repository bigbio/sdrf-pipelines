# CONTRIBUTING

## Setting up the development environment

```
python3.10 -m venv venv
source venv/bin/activate
pip install -e .
pip install -r requirements-dev.txt
```

## Running the tests

After setting up the development environment, it is as easy as running

```
pytest
```

## Static type checking

We use Python type hints and when added, they need to be checked with `mypy`

```
mypy sdrf_pipelines
```

## Code formatting

We delegate code formatting to isort and black.
To pass the CI tests, the code must adhere to the code formatting standards set in `pyproject.toml` file.

## `pre-commit` hooks

As a convenience, we provide pre-commit hooks.

They can be run manually with

```
pre-commit run --all-files
```

or they can be installed to run before each commit

```
pre-commit install --install-hooks
```
