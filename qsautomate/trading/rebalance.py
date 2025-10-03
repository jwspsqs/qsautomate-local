import logging

from prefect import task
import pandas as pd

import omega
from omega import MarketOrder, Stock
from omega.utils.zipline_utils import omega_trades_from_zipline

logger = logging.getLogger(__name__)


@task(
    name="execute-trades",
    description="Execute trades",
    tags=["trading", "rebalance"],
)
def execute_trades(
    bt_performance: pd.DataFrame, strategy_reference: str, client_id: int = 1, **kwargs
) -> None:

    app = omega.Omega(client_id=client_id, **kwargs)

    # Get current account positions
    positions = app.positions_as_symbols()

    # Get current backtest positions
    bt_positions = [d["sid"].symbol for d in bt_performance.positions.iloc[-1]]

    # Calculate positions to liquidate
    divest = list(set(positions) - set(bt_positions))
    logger.info(f"liquidating positions: {divest} (Client ID: {client_id})")
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
    trades = omega_trades_from_zipline(bt_performance, fail_on_day_mismatch=True)
    if trades:
        logger.info(f"executing {len(trades)} trades (Client ID: {client_id})")
        for contract, order in trades:
            app.order_target_quantity(
                contract,
                MarketOrder,
                order.total_quantity,
                order_ref=strategy_reference,
            )
