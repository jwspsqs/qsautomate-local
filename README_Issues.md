# QSAutomate Known Issues

---

## Issue 1: Trade size exceeds `capital_base` due to backtest compounding

**Status:** Open
**Affected file:** `qsautomate/trading/rebalance.py`

### Description

When running the full pipeline via `qsautomate/strategies/qsmomentum.py`, the total value of trades executed against the broker can significantly exceed the `capital_base` configured in `qsresearch/strategies/factor/config.py`.

For example, with `capital_base = 100_000` the live trades placed may total ~$120K or more.

### Root Cause

`capital_base` only sets the **starting** portfolio value for the Zipline backtest simulation. It does not cap the portfolio's growth during the backtest period.

The backtest currently runs from `2024-01-05` to `2026-05-22` (~2.5 years). Over this period the momentum strategy compounds returns, and `bt_performance['portfolio_value'].iloc[-1]` reflects the grown portfolio value, not the original `capital_base`.

In `rebalance.py`, the target positions sent to IBKR are taken directly from the last row of the backtest:

```python
target_positions = {
    d["sid"].symbol: int(d["amount"])
    for d in bt_performance.positions.iloc[-1]
}
```

These share quantities represent the **end-of-backtest** portfolio state (e.g. ~$120K), not the original $100K starting capital. They are applied to the live account without any normalisation.

### Execution Chain

```
qsmomentum.py
 └── run_zipline_backtest()   # returns bt_performance with grown portfolio value
      └── execute_trades(bt_performance, ...)
           └── bt_performance.positions.iloc[-1]  # grown quantities sent to IBKR
```

### Proposed Fix

Scale the target share quantities back to `capital_base` using the final portfolio value before placing orders.

**1. Add `capital_base` parameter to `execute_trades` in `rebalance.py`:**

```python
def execute_trades(
    bt_performance: pd.DataFrame,
    strategy_reference: str,
    capital_base: float,          # new parameter
    client_id: int = 1,
    host: str = "127.0.0.1",
    **kwargs,
) -> None:
```

**2. Scale quantities inside `execute_trades`:**

```python
final_portfolio_value = bt_performance["portfolio_value"].iloc[-1]
scale = capital_base / final_portfolio_value

target_positions = {
    d["sid"].symbol: int(d["amount"] * scale)
    for d in bt_performance.positions.iloc[-1]
}
```

**3. Pass `capital_base` from `qsmomentum.py`:**

```python
execute_trades(
    perf,
    strategy_reference,
    capital_base=strategy_config["capital_base"],
    client_id=client_id,
    host=host,
)
```

### Notes

- The issue becomes more pronounced the longer the backtest window and the better the strategy performs.
- A workaround (not recommended) is to set `backtest_start_date` very close to `run_date` so the portfolio has little time to compound, but this undermines the backtest's purpose.
- Logs from a run showing the discrepancy can be used to confirm the actual `portfolio_value` at the final backtest date.
