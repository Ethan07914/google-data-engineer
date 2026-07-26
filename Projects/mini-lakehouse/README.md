# Mini Lakehouse Project

**Course:** Build Data Lakes and Data Warehouses on Google Cloud (Modules 1–4)

A small, free-tier project to get hands-on with the concepts from that course: raw storage, Iceberg lakehouse tables, partitioning/clustering, governance, and BigQuery ML.

## Goal

Build a tiny medallion architecture (Bronze → Silver → Gold) for a synthetic e-commerce dataset, entirely within GCP's always-free tier.

## Cost & time

- Dataset stays under ~100MB, so storage and query costs stay inside BigQuery's always-free tier (10GB storage, 1TB queries/month) and Cloud Storage's always-free tier (5GB regional storage in `us-central1`, `us-east1` or `us-west1`).
- Estimated effort: a couple of evenings.
- **Skip AlloyDB / federated queries** — AlloyDB bills hourly with no always-free tier, so it's excluded from this project.

## Prerequisites

```bash
# Authenticate and set your project
gcloud auth login
gcloud config set project YOUR_PROJECT_ID

# Enable the APIs you'll need
gcloud services enable storage.googleapis.com bigquery.googleapis.com
```

## Step 1 — Bronze: generate and upload raw data

Generate a small synthetic dataset locally so there's no licensing/download concerns and size stays tiny.

```python
# generate_data.py
import csv
import random
from datetime import datetime, timedelta

random.seed(42)
start = datetime(2025, 1, 1)

with open("web_log.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["customer_id", "log_id", "timestamp", "url", "purchase_amount"])
    for i in range(5000):
        writer.writerow([
            random.randint(1, 500),
            i,
            (start + timedelta(minutes=random.randint(0, 200000))).isoformat(),
            random.choice(["/home", "/product/1", "/product/2", "/cart", "/checkout"]),
            round(random.uniform(0, 200), 2) if random.random() > 0.7 else 0,
        ])
```

```bash
python generate_data.py

# Create a bucket in a free-tier eligible region
gcloud storage buckets create gs://YOUR_BUCKET_NAME --location=us-central1

# Upload the raw (bronze) data
gcloud storage cp web_log.csv gs://YOUR_BUCKET_NAME/bronze/web_log.csv
```

## Step 2 — Silver: Iceberg table over Cloud Storage

Create a BigQuery connection to Cloud Storage, then a cleaned Iceberg table (mirrors the pattern from the Module 2/3 labs).

```bash
# Create a dataset
bq mk --dataset --location=us-central1 YOUR_PROJECT_ID:lakehouse

# Create a Cloud Resource connection for BigQuery to reach the bucket
bq mk --connection --location=us-central1 --connection_type=CLOUD_RESOURCE gcs_connection

# Grant the connection's service account access to the bucket
gcloud storage buckets add-iam-policy-binding gs://YOUR_BUCKET_NAME \
  --member="serviceAccount:$(bq show --connection --format=json us-central1.gcs_connection | jq -r '.cloudResource.serviceAccountId')" \
  --role="roles/storage.objectAdmin"
```

```sql
-- Load raw CSV into a staging table
LOAD DATA OVERWRITE lakehouse.web_log_raw
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://YOUR_BUCKET_NAME/bronze/web_log.csv']
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
  storage_uri = 'gs://YOUR_BUCKET_NAME/silver/web_log'
) AS
SELECT
  customer_id,
  log_id,
  TIMESTAMP(timestamp) AS event_ts,
  url,
  CAST(purchase_amount AS NUMERIC) AS purchase_amount
FROM lakehouse.web_log_raw
WHERE customer_id IS NOT NULL;
```

## Step 3 — Gold: aggregated native BigQuery table

Partition and cluster the gold table for efficient querying (Module 3 concepts).

```sql
CREATE OR REPLACE TABLE lakehouse.customer_summary_gold
PARTITION BY DATE(event_ts)
CLUSTER BY customer_id AS
SELECT
  customer_id,
  DATE(event_ts) AS event_date,
  COUNT(*) AS total_events,
  SUM(purchase_amount) AS total_spent,
  SUM(purchase_amount > 0) AS total_purchases
FROM lakehouse.web_log_silver
GROUP BY customer_id, event_date;
```

## Step 4 — Governance: row-level security

Practice the IAM/fine-grained security concepts from Module 4 with a simple row-level policy.

```sql
-- Only let analysts see rows for customers with total_spent > 0
CREATE ROW ACCESS POLICY paying_customers_only
ON lakehouse.customer_summary_gold
GRANT TO ("group:analysts@yourdomain.com")
FILTER USING (total_spent > 0);
```

## Step 5 (stretch) — BigQuery ML model

Mirrors the `customer_churn_predictor` example from the Module 4 notes.

```sql
CREATE OR REPLACE MODEL lakehouse.return_predictor
OPTIONS(model_type = 'LOGISTIC_REG') AS
SELECT
  customer_id,
  total_events,
  total_spent,
  (total_purchases > 1) AS will_return
FROM lakehouse.customer_summary_gold;

-- Evaluate
SELECT * FROM ML.EVALUATE(MODEL lakehouse.return_predictor);

-- Predict
SELECT * FROM ML.PREDICT(MODEL lakehouse.return_predictor,
  TABLE lakehouse.customer_summary_gold);
```

## Cleanup

To avoid any lingering charges once you're done exploring:

```bash
bq rm -r -f YOUR_PROJECT_ID:lakehouse
gcloud storage rm -r gs://YOUR_BUCKET_NAME
```
