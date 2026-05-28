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
