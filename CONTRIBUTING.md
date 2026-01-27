# CONTRIBUTING

## Setting up the development environment

We use [uv](https://docs.astral.sh/uv/) as our package manager for fast, reliable dependency management.

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repository
git clone https://github.com/bigbio/sdrf-pipelines.git
cd sdrf-pipelines

# Create virtual environment and install all dependencies (including dev)
uv sync --group dev

# Activate the virtual environment (optional - you can also use `uv run` prefix)
source .venv/bin/activate  # Linux/macOS
# or
.venv\Scripts\activate     # Windows
```

## Running the tests

After setting up the development environment:

```bash
uv run pytest

# Or with coverage
uv run pytest --cov=sdrf_pipelines
```

## Static type checking

We use Python type hints and when added, they need to be checked with `mypy`:

```bash
uv run mypy src/sdrf_pipelines
```

## Code formatting and linting

We use [ruff](https://docs.astral.sh/ruff/) for both linting and code formatting. Ruff is an extremely fast Python linter and formatter written in Rust that replaces multiple tools (flake8, isort, black, etc.).

To pass the CI tests, the code must adhere to the code standards set in `pyproject.toml`.

```bash
# Check for linting issues
uv run ruff check .

# Auto-fix linting issues where possible
uv run ruff check --fix .

# Format code
uv run ruff format .

# Check formatting without making changes
uv run ruff format --check .
```

## `pre-commit` hooks

We use [pre-commit](https://pre-commit.com/) to automatically run code quality checks before each commit. The hooks run ruff (linting + formatting) and mypy (type checking).

**Install the hooks (recommended):**

```bash
uv run pre-commit install
```

This will automatically run checks on staged files before each commit.

**Run manually on all files:**

```bash
uv run pre-commit run --all-files
```

**Current pre-commit hooks:**
- `end-of-file-fixer` - Ensures files end with a newline
- `trailing-whitespace` - Removes trailing whitespace
- `detect-private-key` - Prevents committing private keys
- `ruff` - Linting with auto-fix
- `ruff-format` - Code formatting
- `mypy` - Static type checking

## Pull Request Guidelines

1. Fork the repository and create a feature branch
2. Ensure all pre-commit hooks pass: `uv run pre-commit run --all-files`
3. Ensure all tests pass: `uv run pytest`
4. Update documentation if needed
5. Submit a pull request with a clear description of changes
