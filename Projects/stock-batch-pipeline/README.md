# Daily Stock Market Batch Pipeline

**Course:** Build Batch Data Pipelines on Google Cloud (Modules 1–4)

A daily batch pipeline that pulls EOD stock prices from the [Tiingo](https://www.tiingo.com/) API,
validates and deduplicates them with Serverless for Apache Spark, and orchestrates the whole thing
with Cloud Composer — exercising batch-vs-streaming judgment, DLQ handling, deduplication, and
DAG orchestration/monitoring from the course.

## Architecture

```
Tiingo API
   │  (ingest_tiingo.py)
   ▼
Bronze  gs://BUCKET/bronze/{ticker}/{date}.json        — raw API response, one blob per ticker/run
   │  (validate_and_dedupe.py, Dataproc Serverless)
   ▼
Silver  BQ stock_pipeline.stock_prices_silver           — validated, deduplicated rows
   │        gs://BUCKET/dlq/{date}/                     — rows that failed validation
   │  (aggregate_gold.sql, BigQuery)
   ▼
Gold    BQ stock_pipeline.stock_metrics_gold             — daily returns, 7d/30d moving averages
```

Orchestrated end-to-end by `stock_pipeline_dag.py` on Cloud Composer:
`ingest_prices >> validate_and_load >> refresh_gold`, scheduled weekdays after US market close.

| Stage | Course concept |
|---|---|
| Batch, not streaming | Module 1 — EOD data updates once a day; no need for a streaming pipeline |
| `validate_and_dedupe.py` | Module 3 — validation + DLQ pattern, window-function deduplication |
| `stock_pipeline_dag.py` | Module 2/4 — `DataprocCreateBatchOperator`, `BigQueryInsertJobOperator`, task dependencies |
| Composer Variables, retries | Module 4 — separating config from logic, `default_args` retries |
| Cloud Monitoring alert (below) | Module 4 — alerting on DAG run failure |

## Cost & time

- **Cloud Composer is not in the always-free tier** — the environment bills continuously while it
  exists, unlike the mini-lakehouse project. Use the account with free credits, and **delete the
  environment** as soon as you're done (see Cleanup).
- Tiingo's free tier (500 req/hour) comfortably covers a 5-ticker daily pull.
- Dataproc Serverless and BigQuery usage here are small enough to stay marginal even outside
  free-tier credits.
- Estimated effort: an evening for setup, then it runs on its own.

## Prerequisites

```bash
# Authenticate and set your project (use the account with free credits)
gcloud auth login
gcloud config set project YOUR_PROJECT_ID

# Enable the APIs you'll need
gcloud services enable composer.googleapis.com dataproc.googleapis.com \
  bigquery.googleapis.com storage.googleapis.com
```

Get a free Tiingo API key from [tiingo.com](https://www.tiingo.com/) — no card required for the
free tier.

## Step 1 — Storage and BigQuery setup

```bash
export BUCKET=YOUR_BUCKET_NAME
gcloud storage buckets create gs://$BUCKET --location=us-central1

bq mk --dataset --location=us-central1 YOUR_PROJECT_ID:stock_pipeline
```

## Step 2 — Create the Composer environment

```bash
gcloud composer environments create stock-pipeline-env \
  --location us-central1 \
  --image-version composer-2-airflow-2
```

This takes ~20–25 minutes. While it's provisioning, add `requests` as a PyPI package the
environment will need for `ingest_tiingo.py`:

```bash
gcloud composer environments update stock-pipeline-env \
  --location us-central1 \
  --update-pypi-package requests
```

## Step 3 — Upload the DAG and scripts

```bash
# Find the environment's DAGs folder bucket
gcloud composer environments describe stock-pipeline-env \
  --location us-central1 --format="get(config.dagGcsPrefix)"

# Upload the DAG and ingest module together — Airflow imports ingest_tiingo.py
# as a plain module since it lives alongside the DAG file.
gcloud storage cp stock_pipeline_dag.py ingest_tiingo.py aggregate_gold.sql \
  gs://YOUR-ENV-BUCKET/dags/

# The Spark job runs from GCS directly, not the DAGs folder
gcloud storage cp validate_and_dedupe.py gs://$BUCKET/scripts/
```

## Step 4 — Set Airflow Variables

In the Airflow UI (Admin → Variables) or via CLI:

```bash
gcloud composer environments run stock-pipeline-env --location us-central1 \
  variables set -- gcp_project_id YOUR_PROJECT_ID

gcloud composer environments run stock-pipeline-env --location us-central1 \
  variables set -- stock_pipeline_bucket $BUCKET

gcloud composer environments run stock-pipeline-env --location us-central1 \
  variables set -- tiingo_api_key YOUR_TIINGO_KEY
```

`tiingo_api_key` is stored in the Airflow metadata DB this way — fine for a learning project. For
anything closer to production, back Airflow Variables with Secret Manager instead.

## Step 5 — Run and verify

Trigger the DAG manually from the Airflow UI (or `gcloud composer environments run ... dags trigger
-- daily_stock_pipeline`), then check:

```sql
SELECT * FROM `YOUR_PROJECT_ID.stock_pipeline.stock_metrics_gold`
ORDER BY date DESC, ticker;
```

**Optional — alert on failure (Module 4):** in Cloud Monitoring, create an alerting policy on the
`composer.googleapis.com/environment/dag_run/failed_count` metric filtered to `dag_id=daily_stock_pipeline`,
so a broken run gets flagged instead of silently sitting in the UI.

## Cleanup

Composer is the expensive part — delete it first:

```bash
gcloud composer environments delete stock-pipeline-env --location us-central1

bq rm -r -f YOUR_PROJECT_ID:stock_pipeline
gcloud storage rm -r gs://$BUCKET
```
