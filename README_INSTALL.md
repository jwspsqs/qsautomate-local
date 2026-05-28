# Dev Container Installation Guide

## Prerequisites

- Docker Desktop installed and running
- VSCode with the [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension
- A GitHub Personal Access Token (PAT) with repo access
- An FMP API key

## Step 1: Clone the Repository

```bash
git clone https://github.com/quant-science/QSAutomate.git
cd QSAutomate
```

## Step 2: Configure Environment Variables

Rename `.env.example` to `.env` and fill in your credentials:

```
FMP_API_KEY=your_fmp_api_key
GITHUB_TOKEN=your_github_personal_access_token
```

The `GITHUB_TOKEN` is required to install private dependencies (Omega, QSConnect, QSResearch) during the container build.

## Step 3: Open the Dev Container

1. Open the `QSAutomate` folder in VSCode.
2. Press **Ctrl+Shift+P** and select **Dev Containers: Reopen in Container**.
3. The first build takes several minutes — dependencies are downloaded and installed inside the container.

When successful, the VSCode status bar shows **Dev Container: QSAutomate** in blue in the lower left corner.

## Step 4: Post-Build Configuration

### Set Capital Base

Two files inside `.venv` have `capital_base` hardcoded to `1_000_000`. Update both to match your actual account size after every container rebuild (these files are reset each time the container is rebuilt from scratch):

**File 1:**
`.venv/lib/python3.11/site-packages/qsresearch/strategies/factor/config.py` — line 79

**File 2:**
`.venv/lib/python3.11/site-packages/qsresearch/strategy_workflows/factor/base_config.py` — line 111

Change in both files:
```python
"capital_base": 1_000_000,  # original
"capital_base": 100_000,    # update to match your account size
```

### One-Time Prefect Configuration

Open a terminal inside the container (**Terminal → New Terminal**) and run:

```bash
uv run prefect config set PREFECT_RESULTS_PERSIST_BY_DEFAULT=true
```

This enables task result caching so completed tasks are skipped on reruns.

## Step 5: Configure the Strategy

Configuration is split across two files. When replicating a specific MLFlow run manually, update both.

### What each file controls

| Parameter | Controlled by |
|-----------|--------------|
| `start_date`, `end_date` | `qsmomentum.py` (overrides config.py) |
| `run_date` (data download end) | `qsmomentum.py` |
| `host`, `client_id` | `qsmomentum.py` |
| `capital_base` | `config.py` |
| `benchmark_symbol`, `window_length` | `config.py` |
| `rebalance_schedule`, `transaction_costs` | `config.py` |
| `algorithm`, `portfolio_strategy` | `config.py` |
| `preprocess` pipeline | `config.py` |
| MLFlow settings | `config.py` |

### `qsautomate/strategies/qsmomentum.py`

Update `run_date` to the most recent trading day (last Friday if running on a weekend). `backtest_start_date` and `data_start_date` are typically left as is unless replicating a specific MLFlow run:

```python
data_start_date = pd.Timestamp(2021, 1, 4)      # keep ~3 years before backtest_start_date
backtest_start_date = pd.Timestamp(2024, 1, 5)  # update if replicating a specific MLFlow run
run_date = pd.Timestamp(2026, 5, 22)            # update to most recent trading day
```

The `host` is already configured for dev containers — no change needed:

```python
host = "host.docker.internal"   # routes to TWS running on the host machine
```

### `.venv/.../qsresearch/strategies/factor/config.py`

Update `capital_base` and any other parameters to match the target MLFlow run. Key parameters to check:

```python
"capital_base": 100_000,
"portfolio_strategy": {
    "params": {
        "num_long_positions": 20,
        "long_threshold": 1.00,
    }
},
"transaction_costs": {
    "slippage": {"spread": 0.01},
    "commission": {"cost": 0.005, "min_trade_cost": 0},
},
```

> **Note:** This file lives inside `.venv` and is reset on every container rebuild. Re-apply changes after each rebuild.

## Step 6: Start Supporting Services

Open two additional terminals and start MLFlow and Prefect:

**Terminal 1 — MLFlow:**
```bash
uv run mlflow server \
  --port 8031 \
  --backend-store-uri ~/.qsresearch/mlflow/runs \
  --default-artifact-root ~/.qsresearch/mlflow/artifacts
```

**Terminal 2 — Prefect:**
```bash
uv run prefect server start
```

| Service | URL |
|---------|-----|
| MLFlow  | http://127.0.0.1:8031 |
| Prefect | http://127.0.0.1:4200 |

## Step 7: Run the Strategy

In a third terminal:

```bash
uv run python qsautomate/strategies/qsmomentum.py
```

---

## Dockerfile Notes

The original Dockerfile required three fixes to build on current Docker Desktop:

**1. Updated base image from `bullseye` to `bookworm`**
The original Debian 11 base image is outdated. Updated to Debian 12 (current stable).

**2. Removed expired Yarn apt repository**
The base image ships with a Yarn apt repository whose GPG key is expired, causing `apt-get update` to fail in both the Dockerfile and the `docker-outside-of-docker` feature. Fixed by removing it before any package operations:

```dockerfile
RUN find /etc/apt/sources.list.d/ -name "*yarn*" -delete && \
    find /etc/apt/trusted.gpg.d/ -name "*yarn*" -delete && \
    apt-get update && \
    ...
```

**3. Replaced `uv export + pip install` with `uv sync`**
The original pip-based install failed because hatchling requires `README.md` during the build, but only `pyproject.toml` and `uv.lock` are copied into the Docker context. Replaced with:

```dockerfile
RUN uv sync --no-dev --frozen --no-install-project
```

- `--frozen` — uses the lock file without updating it
- `--no-install-project` — skips installing `qsautomate` itself, which is available via the mounted workspace at runtime
