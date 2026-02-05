# Prefect Best Practices Refactor Design

## Overview

Update the Prefect setup with best practices for task/flow structure, caching, retries, and maintainability.

## Decisions

| Topic | Decision |
|-------|----------|
| Caching | Cache by date range inputs, 24-hour expiration |
| Retries | Exponential backoff with jitter (3 retries, 10s base) |
| Flow structure | Direct calls with return values |
| Environment | Load `dotenv` once at flow entry point |
| Logging | Use Prefect's `get_run_logger()` |
| Timeouts | 5-minute timeout on trading task only |
| Configuration | Shared constants in `config/prefect.py` |

## Project Structure

```
qsautomate/
├── config/
│   ├── __init__.py
│   └── prefect.py         # Shared task/flow configurations
├── data/
│   ├── __init__.py
│   ├── fmp.py             # FMP download tasks
│   └── bundle.py          # Zipline bundle task
├── backtest/
│   ├── __init__.py
│   └── zipline_runner.py  # Backtest task
├── trading/
│   ├── __init__.py
│   └── rebalance.py       # Trading task
└── strategies/
    ├── __init__.py
    └── qsmomentum.py      # Flow + entry point
```

## Shared Configuration (`config/prefect.py`)

```python
from datetime import timedelta
from prefect.cache_policies import INPUTS
from prefect.tasks import exponential_backoff

DATA_TASK_CONFIG = {
    "retries": 3,
    "retry_delay_seconds": exponential_backoff(backoff_factor=10),
    "retry_jitter_factor": 0.5,
    "cache_policy": INPUTS,
    "cache_expiration": timedelta(hours=24),
}

BACKTEST_TASK_CONFIG = {
    "retries": 2,
    "retry_delay_seconds": exponential_backoff(backoff_factor=10),
    "retry_jitter_factor": 0.5,
}

TRADING_TASK_CONFIG = {
    "retries": 3,
    "retry_delay_seconds": exponential_backoff(backoff_factor=5),
    "retry_jitter_factor": 0.5,
    "timeout_seconds": 300,
}

FLOW_CONFIG = {
    "retries": 1,
    "retry_delay_seconds": 60,
}
```

## Task Pattern

All tasks follow this pattern:

```python
from prefect import task
from prefect import get_run_logger
from qsautomate.config.prefect import DATA_TASK_CONFIG

@task(
    name="task-name",
    description="Task description",
    tags=["category"],
    **DATA_TASK_CONFIG,
)
def task_function(args):
    logger = get_run_logger()
    logger.info("...")
    # implementation
```

**Changes from current:**
- Import and spread shared config with `**CONFIG`
- Replace `logging.getLogger(__name__)` with `get_run_logger()`
- Remove `load_dotenv()` from task modules

## Flow Pattern

```python
from dotenv import load_dotenv
from prefect import flow
from prefect.futures import wait
from qsautomate.config.prefect import FLOW_CONFIG

load_dotenv()  # Load once at entry point

@flow(name="flow-name", **FLOW_CONFIG)
def main_flow(args):
    # Parallel tasks use .submit() + wait()
    future_a = task_a.submit(args)
    future_b = task_b.submit(args)
    wait([future_a, future_b])

    # Sequential tasks use direct calls
    result_c = task_c()
    result_d = task_d(result_c)
```

## Files to Modify

| File | Change |
|------|--------|
| `config/__init__.py` | New (empty) |
| `config/prefect.py` | New - shared configs |
| `data/fmp.py` | Add configs, `get_run_logger()`, remove `load_dotenv()` |
| `data/bundle.py` | Add configs, `get_run_logger()`, remove `load_dotenv()` |
| `backtest/zipline_runner.py` | Add configs, `get_run_logger()` |
| `trading/rebalance.py` | Add configs, `get_run_logger()` |
| `strategies/qsmomentum.py` | Simplify flow, add `load_dotenv()`, direct calls |
| `README.md` | Add persistence setup instructions |

## Setup Requirement

Enable result persistence for caching:

```bash
prefect config set PREFECT_RESULTS_PERSIST_BY_DEFAULT=true
```

## References

- [Prefect Caching](https://docs.prefect.io/v3/concepts/caching)
- [Prefect Tasks](https://docs.prefect.io/v3/develop/write-tasks)
- [Prefect Flows](https://docs.prefect.io/v3/develop/write-flows)
