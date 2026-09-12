# ml

Anomaly detection over monthly balances.

Features are lagged balances at one, three, and twelve periods, month indicators, and a
cost-centre size band. The lag of twelve is what sets the history this layer needs: a
series has no usable row until its thirteenth period, and a range short enough that each
month appears once makes the month indicator a unique label per row rather than a
seasonal term. Three years is the working range, and the size band is computed from the
training periods of a fold rather than from the whole panel. See docs/adr/0055.

Two arms are compared: a regularised linear model and gradient boosting, backtested with
a time-series split rather than a random one, because financial series carry both trend
and seasonality. The split is cut on the accounting period by a splitter written here —
the data is a panel of many series over the same months, and scikit-learn's
`TimeSeriesSplit` cuts contiguous row positions, which on a panel cuts between series.
See docs/adr/0053.

The output is a prediction with an interval, and the interval is fitted rather than added
around a point prediction: both arms fit the 0.05, 0.50 and 0.95 quantiles, so the
interval widens where the series is dispersed and narrows where it is not. That is why
the linear arm is `QuantileRegressor` rather than `Ridge`, which fits a conditional mean
and produces no interval at all. A balance is flagged only when the actual value falls
outside that interval, and the residual and score are stored alongside the flag.
Anomalies are decided by a model, not by a fixed threshold. See docs/adr/0052.

The flags land in a schema of their own, written by a pipeline step that runs after the
mart has been published — this layer must not be able to stop the mart being published.
See docs/adr/0050. It trains on the mart rather than the landing layer, so it only ever
sees figures that passed all six gates. See docs/adr/0051.

`python -m ml.backtest` scores both arms over the mart and writes the comparison to
`data/ml/backtest.json`; `python -m ml.judge --periods <p> --run-id <id>` judges one
period without a pipeline run.

What CI gates here is behaviour — no leakage, nothing flagged from inside its interval, a
complete flag row, a reproducible rerun — plus one gate on effect: the anomalies the
generator planted must be caught. Coverage, interval width and pinball loss are recorded
for a person to read, not thresholded. See docs/adr/0054.
