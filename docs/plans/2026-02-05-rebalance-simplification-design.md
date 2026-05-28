# Trading Rebalance Simplification Design

## Overview

Simplify `trading/rebalance.py` to fix a critical bug and reduce complexity by matching broker positions to backtest final state directly.

## Bug Discovery

The original code had a semantic mismatch:

| Function | Returns/Expects | Meaning |
|----------|-----------------|---------|
| `omega_trades_from_zipline()` | Order with `total_quantity` | **Incremental trade** (buy 30 shares today) |
| `order_target_quantity()` | `target` parameter | **Absolute position** (end up with 80 shares total) |

**Example of the bug:**
1. Current broker position: 50 AAPL
2. Zipline order: Buy 30 AAPL (to reach 80 total)
3. Code passes: `order_target_quantity(contract, MarketOrder, 30)`
4. Omega calculates: `30 - 50 = -20`
5. **Result: SELLS 20 shares instead of BUYING 30!**

## Solution

Instead of replaying individual orders, match the broker's positions to the backtest's final state directly.

### Before (buggy, 44 lines of logic)

```python
positions = app.positions_as_symbols()
bt_positions = [d["sid"].symbol for d in bt_performance.positions.iloc[-1]]

# Liquidate positions not in backtest
divest = list[str](set(positions) - set(bt_positions))
if divest:
    for sym in divest:
        app.order_target_percent(contract, MarketOrder, target=0.0, ...)

# Execute trades from zipline (BUG: passes order qty as target)
trades = omega_trades_from_zipline(bt_performance, fail_on_day_mismatch=False)
if trades:
    for contract, order in trades:
        app.order_target_quantity(contract, MarketOrder, order.total_quantity, ...)
```

### After (correct, 15 lines of logic)

```python
# Get target and current positions
target_positions = {d["sid"].symbol: int(d["amount"]) for d in bt_performance.positions.iloc[-1]}
current_positions = {pos.contract.symbol: pos.position for pos in app.positions()}

# Rebalance everything in one pass
all_symbols = set(target_positions.keys()) | set(current_positions.keys())
for symbol in all_symbols:
    target_qty = target_positions.get(symbol, 0)
    current_qty = current_positions.get(symbol, 0)
    if target_qty != current_qty:
        app.order_target_quantity(Stock(symbol, "SMART", "USD"), MarketOrder, target_qty, ...)
```

## Changes

### Removed
- `from omega.utils.zipline_utils import omega_trades_from_zipline`
- Separate liquidation loop
- `omega_trades_from_zipline()` call
- `divest` calculation

### Added
- Direct extraction of target positions from backtest
- Single unified rebalancing loop
- Only orders when position differs
- Clear logging of position changes

## Benefits

1. **Fixes critical bug** - Positions now correctly match backtest
2. **66% less code** - 15 lines vs 44 lines of logic
3. **Simpler mental model** - "Match positions" vs "liquidate then replay orders"
4. **Better logging** - Shows `AAPL: 50 -> 80` instead of just "executing trades"

## Final Implementation

```python
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
```
