"""Anomaly detection over monthly balances.

A balance is flagged when it falls outside a prediction interval the model fitted for
it. Nothing here decides a threshold and nothing here is consulted about what an
anomaly means: the interval comes from the fit, and the row is outside it or it is not.

`features` builds the design matrix from the mart's monthly grid, `models` holds the
two arms, `backtest` cuts the folds and scores them, `judge` turns predictions into
flags, and `store` is the table they land in. See docs/adr/0050 through 0056.
"""

__all__ = ["backtest", "features", "judge", "models", "store"]
