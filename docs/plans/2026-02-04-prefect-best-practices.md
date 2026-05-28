# Prefect Best Practices Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor Prefect tasks and flows to use caching, retries with exponential backoff, and centralized configuration.

**Architecture:** Create shared configuration module, update all tasks to use `get_run_logger()` and spread configs, simplify flow to use direct calls instead of verbose submit/wait/result pattern.

**Tech Stack:** Prefect 3.x, Python 3.11

---

### Task 1: Create config module with shared Prefect configurations

**Files:**
- Create: `qsautomate/config/__init__.py`
- Create: `qsautomate/config/prefect.py`

**Step 1: Create config directory and __init__.py**

```bash
mkdir -p qsautomate/config
```

**Step 2: Create empty __init__.py**

Create `qsautomate/config/__init__.py`:
```python
```

**Step 3: Create prefect.py with shared configurations**

Create `qsautomate/config/prefect.py`:
```python
from datetime import timedelta

from prefect.cache_policies import INPUTS
from prefect.tasks import exponential_backoff

# Data tasks: cache by inputs, expire after 24 hours, retry with backoff
DATA_TASK_CONFIG = {
    "retries": 3,
    "retry_delay_seconds": exponential_backoff(backoff_factor=10),
    "retry_jitter_factor": 0.5,
    "cache_policy": INPUTS,
    "cache_expiration": timedelta(hours=24),
}

# Backtest tasks: retry but no caching (depends on bundle state)
BACKTEST_TASK_CONFIG = {
    "retries": 2,
    "retry_delay_seconds": exponential_backoff(backoff_factor=10),
    "retry_jitter_factor": 0.5,
}

# Trading tasks: aggressive retries, timeout for hung connections
TRADING_TASK_CONFIG = {
    "retries": 3,
    "retry_delay_seconds": exponential_backoff(backoff_factor=5),
    "retry_jitter_factor": 0.5,
    "timeout_seconds": 300,
}

# Flow-level config: one retry as last resort
FLOW_CONFIG = {
    "retries": 1,
    "retry_delay_seconds": 60,
}
```

**Step 4: Verify module imports correctly**

Run: `uv run python -c "from qsautomate.config.prefect import DATA_TASK_CONFIG; print('OK')"`
Expected: OK

**Step 5: Commit**

```bash
git add qsautomate/config/
git commit -m "feat: add shared Prefect task/flow configurations"
```

---

### Task 2: Update data/fmp.py with new task patterns

**Files:**
- Modify: `qsautomate/data/fmp.py`

**Step 1: Update imports and remove load_dotenv**

Replace the imports section at top of file:
```python
import datetime as dt

from prefect import task
from prefect import get_run_logger

from qsconnect import Client

from qsautomate.config.prefect import DATA_TASK_CONFIG
```

**Step 2: Update download_prices_fmp task**

Replace the task decorator and function:
```python
@task(
    name="download-prices-fmp",
    description="Download prices from FMP",
    tags=["data", "fmp", "prices"],
    **DATA_TASK_CONFIG,
)
def download_prices_fmp(start_date: dt.date, run_date: dt.date) -> None:
    logger = get_run_logger()
    logger.info(f"Downloading prices from FMP for {start_date} to {run_date}")
    client = Client()
    stock_list = client.stock_list("stock")

    # Filter stocks
    stock_list_filtered = stock_list[stock_list["type"] == "stock"]
    stock_list_filtered = stock_list_filtered[
        stock_list_filtered["exchangeShortName"].isin(["NASDAQ", "NYSE"])
    ]
    stock_list_filtered = stock_list_filtered[stock_list_filtered["price"] > 5]

    # Get benchmark ETFs
    benchmarks = ["SPY", "QQQ", "IWM"]

    # Download historical prices
    client.historical_prices(
        stock_list_filtered["symbol"].tolist() + benchmarks,
        start_date=start_date,
        end_date=run_date,
        cache=True,
        api_calls_per_minute=2900,
    )
```

**Step 3: Update download_fundamentals_fmp task**

Replace the task decorator and function:
```python
@task(
    name="download-fundamentals-fmp",
    description="Download fundamentals from FMP",
    tags=["data", "fmp", "fundamentals"],
    **DATA_TASK_CONFIG,
)
def download_fundamentals_fmp(
    start_date: dt.date, run_date: dt.date, api_buffer_seconds: int = 10
) -> None:
    logger = get_run_logger()
    logger.info(f"Downloading fundamentals from FMP for {start_date} to {run_date}")
    client = Client()

    statement_types = [
        "income-statement",
        "balance-sheet-statement",
        "cash-flow-statement",
        "ratios",
    ]

    # Download financial statements
    client.fetch_bulk_financial_statements(
        statement_type=statement_types,
        periods="all",
        start_year=start_date.year,
        end_year=run_date.year,
        api_buffer_seconds=api_buffer_seconds,
    )
```

**Step 4: Update build_datalake_fmp task**

Replace the task decorator and function:
```python
@task(
    name="build-datalake-fmp",
    description="Cache downloaded files from FMP into DuckDB",
    tags=["data", "fmp", "datalake"],
    **DATA_TASK_CONFIG,
)
def build_datalake_fmp() -> None:
    logger = get_run_logger()
    logger.info("Building datalake from cached FMP files")
    client = Client()

    # Store in a new database instance (destroys existing database)
    cached_files = client.detect_cached_files()
    client.load_cached_files_to_database(
        cached_files,
        fresh=True,
    )

    client.delete_cached_files(cached_files_df=cached_files)
```

**Step 5: Verify module imports correctly**

Run: `uv run python -c "from qsautomate.data.fmp import download_prices_fmp; print('OK')"`
Expected: OK

**Step 6: Commit**

```bash
git add qsautomate/data/fmp.py
git commit -m "feat: update fmp tasks with caching, retries, and get_run_logger"
```

---

### Task 3: Update data/bundle.py with new task patterns

**Files:**
- Modify: `qsautomate/data/bundle.py`

**Step 1: Update the entire file**

Replace contents of `qsautomate/data/bundle.py`:
```python
from zipline.data import bundles
from zipline.data.bundles import register

from prefect import task
from prefect import get_run_logger

from qsconnect import Client

from qsautomate.config.prefect import BACKTEST_TASK_CONFIG


@task(
    name="build-zipline-bundle",
    description="Build Zipline bundle",
    tags=["data", "bundle"],
    **BACKTEST_TASK_CONFIG,
)
def build_zipline_bundle(bundle_name: str) -> None:
    logger = get_run_logger()
    logger.info(f"Building Zipline bundle: {bundle_name}")

    # Connect to DB and stage data
    client = Client()
    client.connect_to_database(read_only=True)
    client.ingest_zipline_bundle_from_fmp_tables(bundle_name=bundle_name)

    # Avoid duplicate registration errors
    try:
        register(bundle_name)
    except KeyError:
        pass

    bundles.ingest(bundle_name)
    logger.info(f"Bundle {bundle_name} built successfully")
```

**Step 2: Verify module imports correctly**

Run: `uv run python -c "from qsautomate.data.bundle import build_zipline_bundle; print('OK')"`
Expected: OK

**Step 3: Commit**

```bash
git add qsautomate/data/bundle.py
git commit -m "feat: update bundle task with retries and get_run_logger"
```

---

### Task 4: Update backtest/zipline_runner.py with new task patterns

**Files:**
- Modify: `qsautomate/backtest/zipline_runner.py`

**Step 1: Update the entire file**

Replace contents of `qsautomate/backtest/zipline_runner.py`:
```python
from typing import Callable, Any

from prefect import task
from prefect import get_run_logger

from qsautomate.config.prefect import BACKTEST_TASK_CONFIG


@task(
    name="run-zipline-backtest",
    description="Run Zipline backtest",
    tags=["backtest", "zipline"],
    **BACKTEST_TASK_CONFIG,
)
def run_zipline_backtest(config: dict, backtest_fcn: Callable) -> Any:
    logger = get_run_logger()
    logger.info("Running Zipline backtest")
    result = backtest_fcn(config)
    logger.info("Backtest complete")
    return result
```

**Step 2: Verify module imports correctly**

Run: `uv run python -c "from qsautomate.backtest.zipline_runner import run_zipline_backtest; print('OK')"`
Expected: OK

**Step 3: Commit**

```bash
git add qsautomate/backtest/zipline_runner.py
git commit -m "feat: update backtest task with retries and get_run_logger"
```

---

### Task 5: Update trading/rebalance.py with new task patterns

**Files:**
- Modify: `qsautomate/trading/rebalance.py`

**Step 1: Update the entire file**

Replace contents of `qsautomate/trading/rebalance.py`:
```python
import pandas as pd

from prefect import task
from prefect import get_run_logger

import omega
from omega import MarketOrder, Stock, start_loop
from omega.utils.zipline_utils import omega_trades_from_zipline

from qsautomate.config.prefect import TRADING_TASK_CONFIG


@task(
    name="execute-trades",
    description="Execute trades via broker",
    tags=["trading", "rebalance"],
    **TRADING_TASK_CONFIG,
)
def execute_trades(
    bt_performance: pd.DataFrame,
    strategy_reference: str,
    client_id: int = 1,
    host: str = "127.0.0.1",
    **kwargs,
) -> None:
    logger = get_run_logger()
    logger.info(f"Executing trades for strategy: {strategy_reference}")

    start_loop()

    app = omega.Omega(client_id=client_id, host=host, **kwargs)

    # Get current account positions
    positions = app.positions_as_symbols()

    # Get current backtest positions
    bt_positions = [d["sid"].symbol for d in bt_performance.positions.iloc[-1]]

    # Calculate positions to liquidate
    divest = list[str](set(positions) - set(bt_positions))
    logger.info(f"Liquidating positions: {divest} (Client ID: {client_id})")
    if divest:
        for sym in divest:
            contract = Stock(sym, "SMART", "USD")
            app.order_target_percent(
                contract=contract,
                order_type=MarketOrder,
                target=0.0,
                order_ref=strategy_reference,
            )

    # Calculate and execute new trades
    trades = omega_trades_from_zipline(bt_performance, fail_on_day_mismatch=False)
    if trades:
        logger.info(f"Executing {len(trades)} trades (Client ID: {client_id})")
        for contract, order in trades:
            app.order_target_quantity(
                contract,
                MarketOrder,
                order.total_quantity,
                order_ref=strategy_reference,
            )

    app.disconnect()
    logger.info("Trade execution complete")
```

**Step 2: Verify module imports correctly**

Run: `uv run python -c "from qsautomate.trading.rebalance import execute_trades; print('OK')"`
Expected: OK

**Step 3: Commit**

```bash
git add qsautomate/trading/rebalance.py
git commit -m "feat: update trading task with retries, timeout, and get_run_logger"
```

---

### Task 6: Update strategies/qsmomentum.py flow

**Files:**
- Modify: `qsautomate/strategies/qsmomentum.py`

**Step 1: Update the entire file**

Replace contents of `qsautomate/strategies/qsmomentum.py`:
```python
import copy

import pandas as pd
from dotenv import load_dotenv
from typing import Callable

from prefect import flow
from prefect.futures import wait

from qsautomate.config.prefect import FLOW_CONFIG
from qsautomate.data.fmp import (
    download_prices_fmp,
    download_fundamentals_fmp,
    build_datalake_fmp,
)
from qsautomate.data.bundle import build_zipline_bundle
from qsautomate.backtest.zipline_runner import run_zipline_backtest
from qsautomate.trading.rebalance import execute_trades

from qsresearch.strategies.factor import run_backtest
from qsresearch.strategies.factor.config import CONFIG

# Load environment variables once at entry point
load_dotenv()


@flow(
    name="qsmomentum",
    description="Run QS Momentum strategy",
    **FLOW_CONFIG,
)
def main(
    start_date: pd.Timestamp,
    run_date: pd.Timestamp,
    config: dict,
    backtest_fcn: Callable,
    bundle_name: str,
    strategy_reference: str,
    client_id: int,
    host: str,
) -> None:
    # Download data in parallel
    prices_future = download_prices_fmp.submit(start_date, run_date)
    fundamentals_future = download_fundamentals_fmp.submit(start_date, run_date)
    wait([prices_future, fundamentals_future])

    # Sequential pipeline - direct calls, Prefect handles dependencies
    build_datalake_fmp()
    build_zipline_bundle(bundle_name)
    perf = run_zipline_backtest(config, backtest_fcn)
    execute_trades(perf, strategy_reference, client_id=client_id, host=host)


if __name__ == "__main__":
    # Set the start and end date
    start_date = pd.Timestamp(2024, 1, 5)
    run_date = pd.Timestamp.today().normalize()

    # Update the strategy config with the current dates
    strategy_config = copy.deepcopy(CONFIG)
    strategy_config["start_date"] = start_date
    strategy_config["end_date"] = run_date

    # Grab the backtest function and update the strategy reference
    backtest_fcn = run_backtest
    bundle_name = "historical_prices_fmp"
    strategy_reference = strategy_config.get("mlflow_experiment_name", "qsmomentum")
    client_id = 1

    # Uncomment if running Dev Containers
    # host = "host.docker.internal"
    host = "127.0.0.1"

    # Run the end to end strategy
    main(
        start_date=start_date,
        run_date=run_date,
        config=strategy_config,
        backtest_fcn=backtest_fcn,
        bundle_name=bundle_name,
        strategy_reference=strategy_reference,
        client_id=client_id,
        host=host,
    )
```

**Step 2: Verify module imports correctly**

Run: `uv run python -c "from qsautomate.strategies.qsmomentum import main; print('OK')"`
Expected: OK

**Step 3: Commit**

```bash
git add qsautomate/strategies/qsmomentum.py
git commit -m "feat: simplify flow with direct calls and shared config"
```

---

### Task 7: Update README with Prefect setup instructions

**Files:**
- Modify: `README.md`

**Step 1: Add Prefect configuration section after "Running Prefect" section**

Find the line `Access Prefect at:` and add after the "Viewing and cancelling flows" section:

```markdown
## Prefect Configuration

Enable result persistence for task caching:

```bash
uv run prefect config set PREFECT_RESULTS_PERSIST_BY_DEFAULT=true
```

This allows tasks to cache their results and skip re-execution when inputs haven't changed.
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add Prefect result persistence setup instructions"
```

---

### Task 8: Final verification

**Files:** None (verification only)

**Step 1: Verify all imports work**

Run: `uv run python -c "from qsautomate.strategies.qsmomentum import main; print('OK')"`
Expected: OK

**Step 2: Verify config module**

Run: `uv run python -c "from qsautomate.config.prefect import DATA_TASK_CONFIG, TRADING_TASK_CONFIG, FLOW_CONFIG; print('Configs loaded')"`
Expected: Configs loaded

**Step 3: Verify task decorators are valid**

Run: `uv run python -c "from qsautomate.data.fmp import download_prices_fmp; print(f'Retries: {download_prices_fmp.retries}')"`
Expected: Retries: 3
