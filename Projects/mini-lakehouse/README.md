# Mini Lakehouse Project

**Course:** Build Data Lakes and Data Warehouses on Google Cloud (Modules 1–4)

A small, free-tier project to get hands-on with the concepts from that course: raw storage, Iceberg lakehouse tables, partitioning/clustering, governance, and BigQuery ML. Runs entirely as Python scripts via the Google Cloud client libraries, so it can be run and debugged directly from an IDE — no CLI hopping required.

## Goal

Build a tiny medallion architecture (Bronze → Silver → Gold) for a synthetic e-commerce dataset, entirely within GCP's always-free tier.

## Cost & time

- Dataset stays under ~100MB, so storage and query costs stay inside BigQuery's always-free tier (10GB storage, 1TB queries/month) and Cloud Storage's always-free tier (5GB regional storage in `us-central1`, `us-east1` or `us-west1`).
- Estimated effort: a couple of evenings.
- **Skip AlloyDB / federated queries** — AlloyDB bills hourly with no always-free tier, so it's excluded from this project.

## Files

| File | Purpose |
|---|---|
| `config.py` | Project/bucket/dataset settings — edit this first |
| `generate_data.py` | Generates the synthetic bronze dataset (`web_log.csv`) |
| `setup_infra.py` | Creates the bucket, dataset, BigQuery connection, and IAM binding |
| `build_lakehouse.py` | Loads bronze data, creates the silver Iceberg table and gold table |
| `governance.py` | Creates a row access policy on the gold table |
| `ml_model.py` | Trains, evaluates, and runs predictions with a BigQuery ML model |
| `cleanup.py` | Deletes the dataset and bucket when you're done |

## Setup in PyCharm

1. Open this folder (`Projects/mini-lakehouse`) as a PyCharm project, or add it as a content root.
2. Create/select a Python interpreter (PyCharm: *Settings → Project → Python Interpreter → Add*), then install dependencies:

   ```bash
   pip install -r requirements.txt
   ```
3. Authenticate application-default credentials so the client libraries can reach GCP (run once, in PyCharm's terminal):

   ```bash
   gcloud auth application-default login
   gcloud services enable storage.googleapis.com bigquery.googleapis.com bigqueryconnection.googleapis.com --project YOUR_PROJECT_ID
   ```
4. Edit `config.py` with your project ID, a globally-unique bucket name, and (optionally) your analyst group email.

## Run order

1. **`generate_data.py`** — writes `web_log.csv` locally (Bronze).
2. **`setup_infra.py`** — creates the bucket + uploads the bronze CSV, creates the BigQuery dataset, creates a `CLOUD_RESOURCE` connection, and grants that connection's service account access to the bucket.
3. **`build_lakehouse.py`** — loads the CSV into a raw BigQuery table, then creates:
   - **Silver**: `web_log_silver`, an Iceberg table (`table_format = 'ICEBERG'`) stored back in Cloud Storage — the core lakehouse pattern from Module 2/3.
   - **Gold**: `customer_summary_gold`, a native BigQuery table partitioned by `event_date` and clustered by `customer_id` (Module 3 concepts).
4. **`governance.py`** — creates a row access policy (`paying_customers_only`) on the gold table, restricting rows to customers with `total_spent > 0` for the configured analyst group (Module 4 governance).
5. **`ml_model.py`** (stretch) — trains a logistic regression model predicting repeat purchases, mirroring the `customer_churn_predictor` example from the Module 4 notes, then evaluates and runs predictions.

## Cleanup

Run **`cleanup.py`** to delete the BigQuery dataset (including the model, tables, and policy) and the Cloud Storage bucket, so nothing keeps billing once you're done exploring.
