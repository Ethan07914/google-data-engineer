-- Load raw CSV into a staging table
LOAD DATA OVERWRITE lakehouse.web_log_raw
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://web_log_bucket/bronze/web_log.csv']
);

-- Silver: cleaned Iceberg table stored back in Cloud Storage
CREATE OR REPLACE TABLE lakehouse.web_log_silver (
  customer_id INT64,
  log_id INT64,
  event_ts TIMESTAMP,
  url STRING,
  purchase_amount NUMERIC
)
WITH CONNECTION `us-central1.gcs_connection`
OPTIONS (
  table_format = 'ICEBERG',
  storage_uri = 'gs://web_log_bucket/silver/web_log'
) AS
SELECT
  customer_id,
  log_id,
  TIMESTAMP(timestamp) AS event_ts,
  url,
  CAST(purchase_amount AS NUMERIC) AS purchase_amount
FROM lakehouse.web_log_raw
WHERE customer_id IS NOT NULL;