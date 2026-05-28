# UV Packaging Refactor Design

## Overview

Refactor QSAutomate to use modern Python packaging with `uv` and pyproject.toml, replacing the legacy requirements.txt and justfile approach.

## Goals

- Modernize packaging using `uv` as the package manager
- Simplify installation and running commands
- Remove legacy/unused files
- Update documentation to reflect new workflow

## pyproject.toml Structure

```toml
[project]
name = "QSAutomate"
version = "0.1.0"
description = "Automate Algorithmic Trading Systems"
readme = "README.md"
requires-python = ">=3.12"
dependencies = [
    "prefect",
    "mlflow",
    "omega @ git+https://github.com/quant-science/omega.git",
    "qsresearch @ git+https://github.com/quant-science/QSResearch.git",
    "qsconnect @ git+https://github.com/quant-science/QSConnect.git",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

## README Updates

### Installation Section

- Replace `pip install -r requirements.txt` with `uv sync`
- Add uv installation instructions
- Remove "not designed to be installed like other libraries" note

### Running Servers Section

All commands use `uv run` prefix:
- `uv run mlflow server ...`
- `uv run prefect server start`
- `uv run python qsautomate/strategies/qsmomentum.py`

## Files to Remove

| Path | Reason |
|------|--------|
| `justfile` | Replaced by `uv run` commands |
| `requirements.txt` | Replaced by pyproject.toml |
| `qsautomate/strategies/multi_parameter.py` | Unused |
| `qsautomate/strategies/multi_strategy.py` | Unused |
| `.vscode/` | Per request |

## Implementation Order

1. Edit pyproject.toml
2. Edit README.md (installation + running sections)
3. Run `uv sync` to verify installation
4. Remove files via `git rm`
5. Verify imports work with `uv run python -c "from qsautomate.strategies import qsmomentum"`
