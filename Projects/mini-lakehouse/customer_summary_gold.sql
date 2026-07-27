CREATE OR REPLACE TABLE lakehouse.customer_summary_gold
PARTITION BY event_date
CLUSTER BY customer_id AS
SELECT
  customer_id,
  DATE(event_ts) AS event_date,
  COUNT(*) AS total_events,
  SUM(purchase_amount) AS total_spent,
  COUNTIF(purchase_amount > 0) AS total_purchases
FROM lakehouse.web_log_silver
GROUP BY customer_id, event_date;