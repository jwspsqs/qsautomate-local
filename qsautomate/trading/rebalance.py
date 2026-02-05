import pandas as pd

from prefect import task
from prefect import get_run_logger

import omega
from omega import MarketOrder, Stock, start_loop

from qsautomate.config.prefect import TRADING_TASK_CONFIG


@task(
    name="execute-trades",
    description="Rebalance broker positions to match backtest",
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
    logger.info(f"Rebalancing positions for strategy: {strategy_reference}")

    start_loop()
    app = omega.Omega(client_id=client_id, host=host, **kwargs)

    # Get target positions from backtest (final state)
    target_positions = {
        d["sid"].symbol: int(d["amount"])
        for d in bt_performance.positions.iloc[-1]
    }

    # Get current broker positions
    current_positions = {
        pos.contract.symbol: pos.position
        for pos in app.positions()
    }

    # Rebalance: for each symbol, order to target quantity
    all_symbols = set(target_positions.keys()) | set(current_positions.keys())

    for symbol in all_symbols:
        target_qty = target_positions.get(symbol, 0)
        current_qty = current_positions.get(symbol, 0)

        if target_qty != current_qty:
            contract = Stock(symbol, "SMART", "USD")
            logger.info(f"{symbol}: {current_qty} -> {target_qty}")
            app.order_target_quantity(
                contract,
                MarketOrder,
                target_qty,
                order_ref=strategy_reference,
            )

    app.disconnect()
    logger.info("Rebalancing complete")
