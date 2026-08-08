import os

import pendulum

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

from ingest_tiingo import WATCHLIST, fetch_prices, write_to_gcs

# --- CONFIGURATION ---
GCP_PROJECT_ID = Variable.get("gcp_project_id")
GCP_REGION = Variable.get("gcp_region", default_var="us-central1")
GCS_BUCKET = Variable.get("stock_pipeline_bucket")
TIINGO_API_KEY = Variable.get("tiingo_api_key")

BQ_DATASET = "stock_pipeline"
BQ_SILVER_TABLE_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET}.stock_prices_silver"
SPARK_VALIDATE_SCRIPT = f"gs://{GCS_BUCKET}/scripts/validate_and_dedupe.py"


def run_ingest(ds, **_context):
    for ticker in WATCHLIST:
        records = fetch_prices(ticker, TIINGO_API_KEY)
        write_to_gcs(GCS_BUCKET, ticker, ds, records)


with DAG(
    dag_id="daily_stock_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 22 * * 1-5",  # weekdays, after US market close (UTC)
    catchup=False,
    tags=["tiingo", "stocks", "spark", "bigquery"],
    template_searchpath=[os.path.dirname(__file__)],
    default_args={
        "owner": "Data Engineering",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:

    # Task 1: pull EOD prices from Tiingo into GCS bronze.
    ingest_prices = PythonOperator(
        task_id="ingest_prices",
        python_callable=run_ingest,
    )

    # Task 2: validate, dedupe and load into BigQuery silver via Serverless for Apache Spark.
    validate_and_load = DataprocCreateBatchOperator(
        task_id="validate_and_load",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        batch_id="validate-load-{{ ds_nodash }}-{{ ts_nodash }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": SPARK_VALIDATE_SCRIPT,
                "args": [
                    f"--input-path=gs://{GCS_BUCKET}/bronze/*/{{{{ ds }}}}.json",
                    f"--output-table={BQ_SILVER_TABLE_ID}",
                    f"--temp-bucket={GCS_BUCKET}",
                    f"--dlq-path=gs://{GCS_BUCKET}/dlq/{{{{ ds }}}}/",
                ],
            },
        },
    )

    # Task 3: refresh the gold aggregation table (query lives in aggregate_gold.sql).
    refresh_gold = BigQueryInsertJobOperator(
        task_id="refresh_gold",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": "aggregate_gold.sql",
                "useLegacySql": False,
            }
        },
    )

    ingest_prices >> validate_and_load >> refresh_gold
