import logging
from typing import Callable, Any

from prefect import flow, task


@task(
    name="run-zipline-backtest",
    description="Run Zipline backtest",
    tags=["backtest", "zipline"],
)
def run_backtest(config: dict, backtest_fcn: Callable) -> Any:
    perf = backtest_fcn(config)
    return perf


@flow(
    name="zipline-backtest-flow",
    description="Flow to run Zipline backtest",
)
def main(config: dict, backtest_fcn: Callable) -> Any:
    """Main flow to run Zipline backtest.

    Args:
        config: Configuration dictionary for the backtest
        backtest_fcn: Function to run the backtest

    Returns:
        The performance results from the backtest
    """
    return run_backtest(config, backtest_fcn)


if __name__ == "__main__":
    from qsresearch.strategies.factor import run_backtest as run_backtest_fcn
    from qsresearch.strategies.factor.config import CONFIG

    main(
        config=CONFIG,
        backtest_fcn=run_backtest_fcn,
    )
