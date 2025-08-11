import pandas as pd
from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from qsautomate.data.fmp import download_prices_fmp, download_fundamentals_fmp
from qsautomate.data.bundle import build_zipline_bundle
from qsautomate.backtest.zipline_runner import run_zipline_backtest
from qsautomate.trading.rebalance import execute_trades

from qsresearch.strategies.factor import run_backtest
from qsresearch.strategies.factor.config import CONFIG as qsmomentum_config


@flow(
    name="qsmomentum",
    description="Run QS Momentum strategy",
    task_runner=ConcurrentTaskRunner(),
)
def main(
    start_date: pd.Timestamp,
    run_date: pd.Timestamp,
    bundle_name: str,
    backtests: list[dict],
) -> None:
    """
    Orchestrates the end-to-end execution of multiple quantitative strategies using Prefect flows.

    This flow performs the following steps:
        1. Submits concurrent tasks to download price and fundamental data.
        2. Builds the Zipline bundle after price data is available.
        3. Launches multiple backtests in parallel, each with its own configuration and backtest function,
           once both the bundle and fundamental data are ready.
        4. Executes trades for each backtest result, submitting trade execution tasks in parallel.
        5. Waits for all trade execution tasks to complete before exiting.

    Parameters
    ----------
    start_date : datetime.date
        The start date for data downloads and backtests.
    run_date : datetime.date
        The end date (inclusive) for data downloads and backtests.
    bundle_name : str
        The name of the Zipline bundle to build and use for backtesting.
    backtests : list of dict
        A list of backtest specifications. Each dictionary should contain:
            - "config": dict
                The configuration dictionary for the backtest.
            - "backtest_fcn": Callable or None
                The function to execute the backtest. If None, a default may be used.
            - "strategy_reference": str or None
                A unique identifier for the strategy, used for trade execution.
            - "client_id": int or None
                The client ID for trade execution.

    Returns
    -------
    None
        This function orchestrates tasks and does not return a value.

    Notes
    -----
    - All data download, bundle build, backtest, and trade execution steps are orchestrated as Prefect tasks.
    - Backtests and trade executions are fanned out and run concurrently for each strategy specification.
    - The flow ensures all trade executions are complete before exiting.
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

    # For each backtest configuration provided, submit a Prefect task to run the backtest in parallel.
    bt_futures = [
        run_zipline_backtest.submit(
            bt["config"], bt.get("backtest_fcn", run_backtest), wait_for=ready
        )
        for bt in backtests
    ]

    # --- 5. Execute trades for each backtest result in parallel ---
    # For each completed backtest, submit a trade execution task that waits for its corresponding backtest to finish.
    trade_futures = []
    for bt_future, strategy_reference, client_id in bt_futures:
        trade_f = execute_trades.submit(
            bt_future, strategy_reference, client_id=client_id, wait_for=[bt_future]
        )
        trade_futures.append(trade_f)

    # --- 6. Wait for all trade execution tasks to complete before exiting the flow ---
    # This ensures that all trades have been executed before the flow finishes.
    for f in trade_futures:
        f.wait()


if __name__ == "__main__":

    start_date = pd.Timestamp(2024, 1, 1)
    run_date = pd.Timestamp(2025, 7, 31)
    bundle_name = "historical_prices_fmp"

    # Update the backtest config to use the new start date
    qsmomentum_config["start_date"] = start_date
    qsmomentum_config["end_date"] = run_date

    qsmomentum_config_1 = qsmomentum_config.copy()
    qsmomentum_config_1["mlflow_experiment_name"] = "QS Momentum Strategy"

    qsmomentum_config_2 = qsmomentum_config.copy()
    qsmomentum_config_2["mlflow_experiment_name"] = "QS Mean Reversion Strategy"

    backtests = [
        {
            "config": qsmomentum_config_1,
            "backtest_fcn": run_backtest,
            "strategy_reference": "qsmomentum_strategy",  # qsmomentum_config.get("mlflow_experiment_name", "qsmomentum"),
            "client_id": 1000,
        },
        {
            "config": qsmomentum_config_2,
            "backtest_fcn": run_backtest,
            "strategy_reference": "qsmean_reversion_strategy",  # qsmomentum_config.get("mlflow_experiment_name", "qsmomentum"),
            "client_id": 1001,
        },
    ]

    main(
        start_date=start_date,
        run_date=run_date,
        bundle_name=bundle_name,
        backtests=backtests,
    )
