import pandas as pd
import copy
from prefect import flow

from typing import Callable

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

    # Initiate the download of price data and fundamental data concurrently
    prices_f = download_prices_fmp.submit(start_date, run_date)
    fundamentals_f = download_fundamentals_fmp.submit(start_date, run_date)

    # Explicitly wait for the price data download task to complete before proceeding
    prices_f.wait()
    prices_f.result()

    # Similarly, wait for the fundamental data download task to finish
    fundamentals_f.wait()
    fundamentals_f.result()

    build_datalake_f = build_datalake_fmp.submit(wait_for=[prices_f, fundamentals_f])

    # Submit a task to build the Zipline bundle dependent on prices being available
    bundle_f = build_zipline_bundle.submit(
        bundle_name=bundle_name, wait_for=[build_datalake_f]
    )

    # Wait for the bundle build process to finish
    bundle_f.wait()
    bundle_f.result()

    # Submit a task to run the backtest dependent on the bundle being ready
    perf_f = run_zipline_backtest.submit(config, backtest_fcn, wait_for=[bundle_f])

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

    start_date = pd.Timestamp(2024, 1, 1)
    run_date = pd.Timestamp.today().normalize()

    strategy_config = copy.deepcopy(CONFIG)
    strategy_config["start_date"] = start_date
    strategy_config["end_date"] = run_date

    backtest_fcn = run_backtest
    bundle_name = "historical_prices_fmp"
    strategy_reference = strategy_config.get("mlflow_experiment_name", "qsmomentum")
    client_id = 1

    main(
        start_date=start_date,
        run_date=run_date,
        config=strategy_config,
        backtest_fcn=backtest_fcn,
        bundle_name=bundle_name,
        strategy_reference=strategy_reference,
        client_id=client_id,
    )
