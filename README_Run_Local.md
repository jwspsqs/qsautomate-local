# Running QSAutomate Locally (WSL2, No Docker)

The project is configured for Docker Dev Containers by default. This document covers what is needed to run directly in WSL2.

---

## Why WSL2 over Docker

- `uv` already handles Python dependency isolation via `pyproject.toml` and `uv.lock` — the main reason to use Docker for Python is covered
- Zipline requires a Linux environment; WSL2 provides this without the Docker overhead
- IBKR/TWS runs natively on Windows and is reachable from WSL2 via `127.0.0.1`, avoiding the `host.docker.internal` workaround
- Eliminates the double-virtualisation overhead (WSL2 inside Docker inside WSL2) which affects Zipline backtest and DuckDB I/O performance

---

## One-Time Setup

### 1. System packages

```bash
sudo apt-get update
sudo apt-get install -y binutils libwebkit2gtk-4.0-dev sqlite3
```

### 2. Install `uv`

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 3. Install `just` (optional task runner)

```bash
pipx install rust-just
```

### 4. Set GitHub token

The private packages (`omega`, `qsresearch`, `qsconnect`) are installed from GitHub. `GITHUB_TOKEN` must be available before running `uv sync`. Either export it directly or ensure it is present in your `.env` file and exported:

```bash
export GITHUB_TOKEN=<your token>
```

### 5. Install dependencies

Run from the project root. `uv` will automatically download Python 3.11 and create the virtualenv:

```bash
uv sync --frozen
```

### 6. Create MLflow directory

```bash
mkdir -p ~/.qsresearch/mlflow
```

---

## Code Change Required

### IBKR host in `qsautomate/strategies/qsmomentum.py`

The Docker configuration injects `host.docker.internal` as the IBKR host. For WSL2, use `127.0.0.1` instead.

```python
# Docker (current default — comment out):
# host = "host.docker.internal"

# WSL2:
host = "127.0.0.1"
```

---

## VS Code

Connect to the project using the **WSL** remote extension (`ms-vscode-remote.remote-wsl`) instead of the Dev Containers extension.

Ports for MLflow (5000), Jupyter (8888), and Prefect (4200) are forwarded from WSL2 to Windows automatically — no additional configuration needed.

---

## Environment Variables

The `.env` file is loaded automatically at runtime via `load_dotenv()` in `qsmomentum.py`. No Docker `--env-file` flag or `containerEnv` injection is needed.

The Docker `containerEnv` block in `devcontainer.json` sets these — ensure equivalent values are present in `.env` for WSL2:

| Variable | Docker value | WSL2 value |
|---|---|---|
| `IBKR_HOST` | `host.docker.internal` | `127.0.0.1` |
| `TWS_PORT_LIVE` | `7497` | `7497` |
| `TWS_PORT_PAPER` | `7496` | `7496` |
| `GW_PORT_LIVE` | `4001` | `4001` |
| `GW_PORT_PAPER` | `4002` | `4002` |

---

## Running the Strategy

Once set up, run the full pipeline directly:

```bash
uv run python qsautomate/strategies/qsmomentum.py
```

---

## Summary Checklist

- [ ] `apt` packages installed (`binutils`, `libwebkit2gtk-4.0-dev`, `sqlite3`)
- [ ] `uv` installed
- [ ] `just` installed (optional)
- [ ] `GITHUB_TOKEN` available in environment
- [ ] `uv sync --frozen` completed successfully
- [ ] `~/.qsresearch/mlflow` directory created
- [ ] `host` changed to `127.0.0.1` in `qsmomentum.py`
- [ ] `.env` updated with WSL2 values (especially `IBKR_HOST`)
- [ ] VS Code connected via WSL remote extension
