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
