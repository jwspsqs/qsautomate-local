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
