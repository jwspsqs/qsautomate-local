import datetime as dt

from prefect import flow

from typing import Callable

from qsautomate.data.fmp import download_prices_fmp, download_fundamentals_fmp
from qsautomate.data.bundle import build_zipline_bundle
from qsautomate.backtest.zipline_runner import run_backtest
from qsautomate.trading.rebalance import execute_trades


@flow(name="qsmomentum", description="Run QS Momentum strategy")
def main(
    start_date: dt.date,
    run_date: dt.date,
    config: dict,
    backtest_fcn: Callable,
    bundle_name: str,
    strategy_reference: str,
    client_id: int = 1,
) -> None:

    download_prices_fmp(start_date, run_date)
    download_fundamentals_fmp(start_date, run_date)

    build_zipline_bundle(bundle_name=bundle_name)
    perf = run_backtest(config, backtest_fcn)
    execute_trades(perf, strategy_reference, client_id=client_id)


if __name__ == "__main__":

    from qsresearch.strategies.factor import run_backtest
    from qsresearch.strategies.factor.config import CONFIG

    start_date = dt.date(2010, 1, 1)
    run_date = dt.date(2025, 5, 13)
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
