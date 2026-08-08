-- Gold: derived per-ticker metrics for reporting. Rendered as an Airflow Jinja
-- template by stock_pipeline_dag.py, so {{ var.value.* }} resolves to Airflow Variables.
CREATE OR REPLACE TABLE `{{ var.value.gcp_project_id }}.stock_pipeline.stock_metrics_gold`
PARTITION BY date
CLUSTER BY ticker AS
SELECT
  ticker,
  date,
  close,
  volume,
  close - LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS daily_change,
  SAFE_DIVIDE(
    close - LAG(close) OVER (PARTITION BY ticker ORDER BY date),
    LAG(close) OVER (PARTITION BY ticker ORDER BY date)
  ) AS daily_return_pct,
  AVG(close) OVER (
    PARTITION BY ticker ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS moving_avg_7d,
  AVG(close) OVER (
    PARTITION BY ticker ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS moving_avg_30d
FROM `{{ var.value.gcp_project_id }}.stock_pipeline.stock_prices_silver`;
