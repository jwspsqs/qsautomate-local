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
    data_start_date: pd.Timestamp,
    backtest_start_date: pd.Timestamp,
    run_date: pd.Timestamp,
    config: dict,
    backtest_fcn: Callable,
    bundle_name: str,
    strategy_reference: str,
    client_id: int,
    host: str,
) -> None:
    # Download data in parallel (uses data_start_date for full history)
    prices_future = download_prices_fmp.submit(data_start_date, run_date)
    fundamentals_future = download_fundamentals_fmp.submit(data_start_date, run_date)
    wait([prices_future, fundamentals_future])

    # Sequential pipeline - direct calls, Prefect handles dependencies
    build_datalake_fmp()
    build_zipline_bundle(bundle_name)
    perf = run_zipline_backtest(config, backtest_fcn)
    execute_trades(perf, strategy_reference, client_id=client_id, host=host)


if __name__ == "__main__":
    # Set dates: data_start_date must be far enough back to support window_length lookback
    # window_length is 756 trading days (3 years), so data needs to start ~3 years before backtest
    data_start_date = pd.Timestamp(2021, 1, 4)
    backtest_start_date = pd.Timestamp(2024, 1, 5)
    run_date = pd.Timestamp(2026, 5, 22)

    # Update the strategy config with the current dates
    strategy_config = copy.deepcopy(CONFIG)
    strategy_config["start_date"] = backtest_start_date
    strategy_config["end_date"] = run_date

    # Grab the backtest function and update the strategy reference
    backtest_fcn = run_backtest
    bundle_name = "historical_prices_fmp"
    strategy_reference = strategy_config.get("mlflow_experiment_name", "qsmomentum")
    client_id = 1

    # Uncomment if running Dev Containers
    host = "host.docker.internal"
    # host = "127.0.0.1"

    # Run the end to end strategy
    main(
        data_start_date=data_start_date,
        backtest_start_date=backtest_start_date,
        run_date=run_date,
        config=strategy_config,
        backtest_fcn=backtest_fcn,
        bundle_name=bundle_name,
        strategy_reference=strategy_reference,
        client_id=client_id,
        host=host,
    )
