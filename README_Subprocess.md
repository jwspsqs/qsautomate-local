# QSAutomate Subprocess Scripts

Python scripts involved in running the qsmomentum strategy pipeline.

---

## Main Orchestrator

| Script | Role |
|--------|------|
| `qsautomate/strategies/qsmomentum.py` | Top-level Prefect `@flow` — runs the full pipeline end-to-end. Has `if __name__ == "__main__"` so it can be run directly via `uv run python qsautomate/strategies/qsmomentum.py`. |

---

## Pipeline Steps (each a Prefect `@task`)

| Script | Role |
|--------|------|
| `qsautomate/data/fmp.py` | Downloads prices & fundamentals from FMP API, builds DuckDB datalake |
| `qsautomate/data/bundle.py` | Ingests FMP data into Zipline bundle format |
| `qsautomate/backtest/zipline_runner.py` | Runs the Zipline backtest, returns positions/returns DataFrame |
| `qsautomate/trading/rebalance.py` | Connects to IBKR via Omega, executes trades to match backtest positions |

---

## Supporting Utilities

| Script | Role |
|--------|------|
| `qsautomate/utils/mlflow_utils.py` | MLflow experiment management helper (`ensure_experiment_active`) |
| `qsautomate/utils/omega_utils.py` | IBKR/Omega connection setup — used internally by `rebalance.py` |
| `qsautomate/config/prefect.py` | Retry, cache, and timeout configs for each task type |

---

## Execution Chain

```
qsmomentum.py (@flow)
 ├── download_prices_fmp() ─┐
 ├── download_fundamentals_fmp() ─┘  (run in parallel)
 ├── build_datalake_fmp()
 ├── build_zipline_bundle()
 ├── run_zipline_backtest()
 └── execute_trades()
```

---

## Task Detail

### `qsautomate/data/fmp.py`
- `download_prices_fmp()` — fetches historical OHLCV for NASDAQ/NYSE stocks (price > $5) plus benchmarks (SPY, QQQ, IWM)
- `download_fundamentals_fmp()` — fetches income statement, balance sheet, cash flow, and ratios
- `build_datalake_fmp()` — caches downloaded files into a DuckDB database

### `qsautomate/data/bundle.py`
- `build_zipline_bundle()` — registers and ingests FMP data from QSConnect into a Zipline-compatible bundle

### `qsautomate/backtest/zipline_runner.py`
- `run_zipline_backtest()` — executes the backtest using the `run_backtest` function from QSResearch; returns a performance DataFrame containing positions and returns

### `qsautomate/trading/rebalance.py`
- `execute_trades()` — compares target positions (from backtest) against current IBKR positions and places market orders to rebalance; supports custom `client_id` and `host` (defaults to `127.0.0.1` or `host.docker.internal` in Docker)

### `qsautomate/utils/mlflow_utils.py`
- `ensure_experiment_active()` — restores deleted MLflow experiments to active state

### `qsautomate/utils/omega_utils.py`
- `connect_to_ibkr()` — initialises IBController and Omega with watchdog monitoring; used by `rebalance.py`

### `qsautomate/config/prefect.py`
- Defines `DATA_TASK_CONFIG`, `BACKTEST_TASK_CONFIG`, `TRADING_TASK_CONFIG`, and `FLOW_CONFIG` with retry policies, caching, and timeouts
