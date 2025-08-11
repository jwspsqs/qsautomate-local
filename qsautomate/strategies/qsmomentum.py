import pandas as pd

from prefect import flow

from typing import Callable

from qsautomate.data.fmp import download_prices_fmp, download_fundamentals_fmp
from qsautomate.data.bundle import build_zipline_bundle
from qsautomate.backtest.zipline_runner import run_zipline_backtest
from qsautomate.trading.rebalance import execute_trades

from qsresearch.strategies.factor import run_backtest
from qsresearch.strategies.factor.config import CONFIG


@flow(name="qsmomentum", description="Run QS Momentum strategy")
def main(
    start_date: pd.Timestamp,
    run_date: pd.Timestamp,
    config: dict,
    backtest_fcn: Callable,
    bundle_name: str,
    strategy_reference: str,
    client_id: int = 1,
) -> None:
    """
    Orchestrates the end-to-end execution of the QS Momentum strategy using Prefect flows.

    This flow performs the following steps:
        1. Downloads price and fundamental data concurrently.
        2. Builds the Zipline bundle after price data is available.
        3. Runs the backtest once both the bundle and fundamental data are ready.
        4. Executes trades based on the backtest results.

    Parameters
    ----------
    start_date : pd.Timestamp
        The start date for data downloads and backtesting.
    run_date : pd.Timestamp
        The end date (inclusive) for data downloads and backtesting.
    config : dict
        The configuration dictionary for the backtest.
    backtest_fcn : Callable
        The function to execute the backtest.
    bundle_name : str
        The name of the Zipline bundle to build and use for backtesting.
    strategy_reference : str
        A unique identifier for the strategy, used for trade execution.
    client_id : int, optional
        The client ID for trade execution (default is 1).

    Returns
    -------
    None
        This function orchestrates tasks and does not return a value.

    Notes
    -----
    - All data download, bundle build, backtest, and trade execution steps are orchestrated as Prefect tasks.
    - The flow ensures all steps are completed in the correct order and blocks until all tasks are finished.
    """

    # Initiate the download of price data and fundamental data concurrently
    prices_f = download_prices_fmp.submit(start_date, run_date)
    fundamentals_f = download_fundamentals_fmp.submit(start_date, run_date)

    # Explicitly wait for the price data download task to complete before proceeding
    prices_f.wait()
    prices_f.result()

    # Similarly, wait for the fundamental data download task to finish
    fundamentals_f.wait()
    fundamentals_f.result()

    # Submit a task to build the Zipline bundle dependent on prices being available
    bundle_f = build_zipline_bundle.submit(bundle_name=bundle_name, wait_for=[prices_f])

    # Wait for the bundle build process to finish
    bundle_f.wait()
    bundle_f.result()

    # Prepare a list of dependencies that must be satisfied before running any backtests
    ready = [bundle_f, fundamentals_f]

    # Submit a task to run the backtest dependent on the bundle being ready
    perf_f = run_zipline_backtest.submit(config, backtest_fcn, wait_for=ready)

    # Wait for the backtest to finish
    perf_f.wait()
    perf_f.result()

    # Execute trades for the backtest result
    trade_f = execute_trades.submit(
        perf_f, strategy_reference, client_id=client_id, wait_for=[perf_f]
    )

    trade_f.wait()
    trade_f.result()


if __name__ == "__main__":

    start_date = pd.Timestamp(2010, 1, 1)
    run_date = pd.Timestamp(2025, 6, 10)
    config = CONFIG
    backtest_fcn = run_backtest
    bundle_name = "historical_prices_fmp"
    strategy_reference = CONFIG.get("mlflow_experiment_name", "qsmomentum")
    client_id = 1

    main(
        start_date=start_date,
        run_date=run_date,
        config=config,
        backtest_fcn=backtest_fcn,
        bundle_name=bundle_name,
        strategy_reference=strategy_reference,
        client_id=client_id,
    )
