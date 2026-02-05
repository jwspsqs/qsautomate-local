# UV Packaging Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Modernize QSAutomate packaging to use `uv` with pyproject.toml, update documentation, and remove legacy files.

**Architecture:** Replace pip/requirements.txt workflow with uv sync. All commands run via `uv run` prefix. No virtual environment activation needed.

**Tech Stack:** uv, hatchling build backend, pyproject.toml (PEP 621)

---

### Task 1: Update pyproject.toml

**Files:**
- Modify: `pyproject.toml`

**Step 1: Update pyproject.toml with build-system and mlflow dependency**

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

**Step 2: Verify uv can parse the file**

Run: `uv pip compile pyproject.toml --quiet && echo "OK"`
Expected: OK (no errors)

**Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "feat: add build-system and mlflow to pyproject.toml"
```

---

### Task 2: Update README.md Installation Section

**Files:**
- Modify: `README.md` (lines 11-26)

**Step 1: Replace installation section**

Find the section starting with `# ⬆️ Installation` and replace through line 26 with:

```markdown
# ⬆️ Installation

## Install uv

If you don't have uv installed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Clone and install

```bash
git clone https://github.com/quant-science/QSAutomate.git
cd QSAutomate
uv sync
```

This creates a `.venv` directory, installs all dependencies, and installs QSAutomate in editable mode.
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: update installation instructions for uv"
```

---

### Task 3: Update README.md MLFlow Section

**Files:**
- Modify: `README.md` (lines 48-65)

**Step 1: Replace MLFlow section**

Find `## Running MLFlow` and replace through line 65 with:

```markdown
## Running MLFlow

MLFlow tracks experiments. Start it in a terminal:

```bash
uv run mlflow server \
  --port 8031 \
  --backend-store-uri ~/.qsresearch/mlflow/runs \
  --default-artifact-root ~/.qsresearch/mlflow/artifacts
```

Access MLFlow at: [http://127.0.0.1:8031](http://127.0.0.1:8031)
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: update MLFlow instructions to use uv run"
```

---

### Task 4: Update README.md Prefect Section

**Files:**
- Modify: `README.md` (lines 67-95)

**Step 1: Replace Prefect section**

Find `## Running Prefect` and replace through line 95 with:

```markdown
## Running Prefect

[Prefect](https://docs.prefect.io/v3/get-started) is an open-source orchestration engine. Start it in a new terminal:

```bash
uv run prefect server start
```

Access Prefect at: [http://127.0.0.1:4200](http://127.0.0.1:4200)

## Viewing and cancelling flows

List all flow runs:

```bash
uv run prefect flow-run ls
```

Cancel a flow run:

```bash
uv run prefect flow-run cancel <FLOW_RUN_ID>
```
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: update Prefect instructions to use uv run"
```

---

### Task 5: Update README.md Quick Start Section

**Files:**
- Modify: `README.md` (lines 36-44)

**Step 1: Replace Quick Start section**

Find `# 🏃 Quick start` and replace through line 44 with:

```markdown
# 🏃 Quick start

Run the momentum strategy:

```bash
uv run python qsautomate/strategies/qsmomentum.py
```
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: update quick start to use uv run"
```

---

### Task 6: Verify uv sync works

**Files:** None (verification only)

**Step 1: Run uv sync**

Run: `uv sync`
Expected: Successfully creates .venv and installs all dependencies

**Step 2: Verify imports work**

Run: `uv run python -c "from qsautomate.strategies import qsmomentum; print('OK')"`
Expected: OK

---

### Task 7: Remove legacy files

**Files:**
- Delete: `justfile`
- Delete: `requirements.txt`
- Delete: `qsautomate/strategies/multi_parameter.py`
- Delete: `qsautomate/strategies/multi_strategy.py`
- Delete: `.vscode/` (directory)

**Step 1: Remove files with git**

```bash
git rm justfile requirements.txt qsautomate/strategies/multi_parameter.py qsautomate/strategies/multi_strategy.py
git rm -r .vscode
```

**Step 2: Commit**

```bash
git commit -m "chore: remove legacy files (justfile, requirements.txt, multi_*.py, .vscode)"
```

---

### Task 8: Final verification

**Files:** None (verification only)

**Step 1: Clean install test**

```bash
rm -rf .venv
uv sync
```

Expected: Fresh install succeeds

**Step 2: Verify strategy imports**

Run: `uv run python -c "from qsautomate.strategies import qsmomentum; print('OK')"`
Expected: OK

**Step 3: Verify mlflow command works**

Run: `uv run mlflow --version`
Expected: Prints mlflow version

**Step 4: Verify prefect command works**

Run: `uv run prefect version`
Expected: Prints prefect version
