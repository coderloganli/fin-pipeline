# ml

Anomaly detection over monthly balances.

Features are lagged balances at one, three, and twelve periods, month indicators, and a cost-centre size band. A ridge baseline is compared against gradient boosting, backtested with a time-series split rather than a random one, because financial series carry both trend and seasonality.

The output is a prediction with an interval. A balance is flagged only when the actual value falls outside that interval, and the residual and score are stored alongside the flag. Anomalies are decided by a model, not by a fixed threshold.
