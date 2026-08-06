# Module 4: Orchestrate and monitor batch data pipelines

## Table of contents

- [Principles of orchestration](#principles-of-orchestration)
- [Cloud Scheduler](#cloud-scheduler)
- [Workflows](#workflows)
- [Cloud Composer](#cloud-composer)
  - [Anti-patterns](#anti-patterns)
  - [DAG Imports and Configuration](#dag-imports-and-configuration)
  - [DAG Definition](#dag-definition)
  - [Ingest and Validate with Dataflow](#ingest-and-validate-with-dataflow)
  - [Aggregate with Serverless for Apache Spark](#aggregate-with-serverless-for-apache-spark)
  - [Load Final Report with BigQuery](#load-final-report-with-bigquery)
  - [Defining Task Dependencies](#defining-task-dependencies)
  - [Environment setup](#environment-setup)
  - [Batch interval](#batch-interval)
  - [DAGs in Cloud Storage](#dags-in-cloud-storage)
  - [Production-grade systems](#production-grade-systems)
- [Unified observability](#unified-observability)
  - [Proactive monitoring](#proactive-monitoring)
  - [Cloud Monitoring Dashboards](#cloud-monitoring-dashboards)
    - [Centralized log analysis](#centralized-log-analysis)
    - [Proactive alerting](#proactive-alerting)
- [Alerts and troubleshooting](#alerts-and-troubleshooting)
  - [Alerts on system metrics](#alerts-on-system-metrics)
  - [Alerting on DAG Run Failures (Critical Alert)](#alerting-on-dag-run-failures-critical-alert)
  - [Alerting on Performance Degradation](#alerting-on-performance-degradation)
  - [Alerting on DAG Parsing Errors](#alerting-on-dag-parsing-errors)
  - [Alerts on log-based metrics](#alerts-on-log-based-metrics)
    - [Create the log-based metric](#create-the-log-based-metric)
    - [Create an alerting policy](#create-an-alerting-policy)
  - [Fixing orchestrated batch pipelines](#fixing-orchestrated-batch-pipelines)

## Principles of orchestration

- **Dependency Management**: Define the exact order of execution.
- **Scheduling**: Automatically start the entire workflow at a specific time or in response to an event.
- **Error Handling and Retries**: If a task fails due to a temporary issue, the system should automatically try running it again a few times before alerting the operator.
- **Monitoring and Alerting**: A centralized dashboard where you can see the status of all your workflows at a glance; if something fails permanently, the system should automatically send an alert.
- **Idempotency**: Running the same input multiple times produces the same result.

## Cloud Scheduler

- Schedule-driven, single-service orchestration.
- Allows you to trigger HTTP-based services at defined intervals using cron scheduling.
- Ideal for simple, recurring tasks.

## Workflows

- Service for orchestrating multiple HTTP-based services into a durable, stateful workflow.
- Allow for sophisticated logic to manage the execution order and handle step failures.
- Serverless and pay-per-use with built-in error handling.
- Suitable for chaining microservices and automating infrastructure tasks.
- **Primary use**: Service orchestration (chaining APIs), event-driven orchestration (low latency).
- **Programming model**: Declarative approach with YAML/JSON.
- **Latency**: Quick event-driven execution with low startup overhead.
- **Error handling/retries**: Built-in exception handling, automated HTTP call retries; retrying from a specific step is not possible.
- **Logs and UI**: Provides execution visualisation in the Google Cloud console; integrates with Cloud Logging.

## Cloud Composer

- Fully managed workflow orchestration service built on Apache Airflow.
- Designed for data-driven workloads (ELT and ETL pipelines).
- Create, schedule and monitor workflows that span across clouds and on-premise data centers.
- **Primary use case**: Batch data pipelines.
- **Programming model**: Python, full-fledged programming language.
- **Latency**: Relies on a scheduler and workers, which introduces a few seconds of latency between tasks, making it appropriate for non-time-sensitive tasks.
- **Error handling/retries**: Sophisticated task management, built-in scheduling, and features for unexpected behavior (sending email on failure); allows retry from a specific step.
- **Logs and UI**: Rich interface for debugging and managing data workflows, graph views, task-specific logs.

### Anti-patterns

- Airflow provides Google Cloud-specific operators that know exactly how to interact with each service — avoid the anti-pattern of shelling out with BashOperator.

**Run a job**

```python
# Traditional anti-pattern way
BashOperator(bash_command='gcloud')

# Best practice (operator provided)
DataflowTemplatedJobStartOperator()

DataprocSubmitJobOperator()

BigQueryInsertJobOperator()
```

### DAG Imports and Configuration

- DAGs begin with importing necessary components.
- Separate configuration from logic, don't hardcode them in.
- Store secrets in secret management.

```python
import pendulum

from airflow import DAG
from airflow.models.variable import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# --- CONFIGURATION ---
GCP_PROJECT_ID = Variable.get("gcp_project_id")
GCP_REGION = Variable.get("gcp_region", default_var="us-central1")
GCS_BUCKET = Variable.get("gcp_bucket")
# Dataflow Template details for raw transaction validation
DATAFLOW_TEMPLATE_PATH = f"gs://dataflow-templates-{GCP_REGION}/latest/GCS_Text_to_BigQuery"
DATAFLOW_PY_TRANSFORM = f"gs://{GCS_BUCKET}/dataflow/transforms/validate_transactions.py"
# Serverless Spark PySpark file for aggregation
SPARK_AGG_SCRIPT = f"gs://{GCS_BUCKET}/spark/aggregate_sales.py"
# BigQuery dataset and tables
BQ_DATASET = "cymbal_financials"
BQ_RAW_TABLE = "raw_transactions"
BQ_RAW_TABLE_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}"
BQ_AGG_TABLE = "daily_aggregated_sales"
BQ_FINAL_REPORT_TABLE = "daily_sales_report"
```

### DAG Definition

- Create the DAG object.
- Acts as a container for all the tasks in the workflow.
- **dag_id**: a unique identifier for the DAG across the entire workflow environment.
- **start_date**: the date the DAG is eligible to run.
- **schedule**: Defines when the DAG should run.
- **catchup=False**: If True Airflow would run the DAG for all missed schedules between the start_date and today.
- **default_args**: Parameters applied to every task in the DAG, reducing repetitive code; specify if a task should be retried.

```python
# --- DAG DEFINITION ---
with DAG(
    dag_id="cymbal_financial_daily_processing",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["cymbal_superstore", "dataflow", "spark"],
    default_args={
        "owner": "Cymbal Data Engineering",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:
```

### Ingest and Validate with Dataflow

- Runs a prebuilt Dataflow template.
- Reads CSV files from Cloud Storage, validates each record using a Python UDF, and loads valid data to BigQuery.
- Uses a Jinja template macro for inputFilePattern.

```python
# Task 1: Ingest and validate raw sales data from GCS to BigQuery using a Dataflow Template.
    ingest_raw_transactions = DataflowTemplatedJobStartOperator(
        task_id="ingest_raw_transactions",
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        template=DATAFLOW_TEMPLATE_PATH,
        parameters={
            "inputFilePattern": f"gs://{GCS_BUCKET}/sales/raw/{{{{ ds }}}}/transactions.csv",
            "JSONPath": f"gs://{GCS_BUCKET}/dataflow/schemas/transaction_schema.json",
            "outputTable": BQ_RAW_TABLE_ID,
            "bigQueryLoadingTemporaryDirectory": f"gs://{GCS_BUCKET}/temp/bq-load/",
            "pythonUserDefinedFunctions": DATAFLOW_PY_TRANSFORM,
            "userDefinedFunctionName": "validateAndTransform",
        },
    )
```

### Aggregate with Serverless for Apache Spark

- Perform complex aggregations on the raw data.

```python
# Task 2: Run a Serverless for Apache Spark job to perform complex aggregations on the raw data.
    aggregate_with_spark = DataprocSubmitJobOperator(
        task_id="aggregate_with_spark",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            "placement": {"cluster_name": "ephemeral-spark-cluster-{{ ds_nodash }}"},
            "reference": {"project_id": GCP_PROJECT_ID},
            "pyspark_job": {
                "main_python_file_uri": SPARK_AGG_SCRIPT,
                "args": [
                    f"--date={{{{ ds }}}}",
                    f"--source_table={BQ_RAW_TABLE_ID}",
                    f"--destination_table={GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_AGG_TABLE}"
                ],
            },
        },
         gcp_conn_id="google_cloud_default" # Explicitly define connection
    )
```

### Load Final Report with BigQuery

- Runs a MERGE statement.
- Upserts data into the final reporting table.

```python
# Task 3: Load the final, aggregated data into the main reporting table.
    load_to_reporting_table = BigQueryInsertJobOperator(
        task_id="load_to_reporting_table",
        gcp_conn_id="google_cloud_default",
        sql=f"""
            MERGE `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_FINAL_REPORT_TABLE}` T
            USING `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_AGG_TABLE}` S
            ON T.transaction_date = S.transaction_date AND T.product_id = S.product_id -- Or other unique key
            WHEN NOT MATCHED BY TARGET THEN
              INSERT (transaction_date, product_id, total_sales, total_quantity)
              VALUES(S.transaction_date, S.product_id, S.total_sales, S.total_quantity)
            WHEN MATCHED THEN
              UPDATE SET
                T.total_sales = S.total_sales,
                T.total_quantity = S.total_quantity
        """,
    )
```

### Defining Task Dependencies

- Brings the DAG to life.
- Defines the order of execution of the entire workflow.

```python
# --- TASK DEPENDENCIES ---
    ingest_raw_transactions >> aggregate_with_spark >> load_to_reporting_table
```

### Environment setup

- **Enable APIs**: Enable the APIs for Dataproc, Cloud Composer and BigQuery.
- **Orchestration**: Create a Cloud Composer environment.
- **Ensure your Composer service account has permission**: to read from the Cloud Storage bucket (Storage Object Viewer), write to the BigQuery table (BigQuery Data Editor and User), submit Serverless for Apache Spark jobs (Dataproc Editor, Dataproc Worker).
- Create a Cloud Storage bucket for input data and a BigQuery dataset for output data; set up a VPC network.

### Batch interval

- Separate the execution (the "what") from the scheduling (the "when").
- **Serverless for Apache Spark**: Use the DataprocCreateBatchOperator in your Cloud Composer DAG; the DAG's schedule_interval (@hourly or @daily, or a cron expression) tells Composer when to kick off your Spark job, while the operator tells Dataproc which job to run.
- **Dataflow**: Use the DataflowStartFlexTemplateOperator (modern, flexible way to run Dataflow jobs) or DataflowCreatePythonJobOperator (runs a batch pipeline written with the Apache Beam Python SDK).

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='hourly_spark_batch_pipeline',
    start_date=days_ago(1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['dataproc', 'spark', 'batch'],
) as dag:

    start_spark_batch_job = DataprocCreateBatchOperator(
        task_id='run_spark_transformation',
        project_id='your-gcp-project-id',
        region='your-gcp-region',
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://your-code-bucket/spark_job.py",
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "your-service-account@your-project.iam.gserviceaccount.com",
                },
            },
        },
    )
```

### DAGs in Cloud Storage

- To run a Cloud Composer job you must define your workflow in a DAG file and upload it to a specific Cloud Storage bucket.
- Every Composer environment has a DAGs folder.
- Cloud Storage bucket stores all job configuration files.
- You must specify the project ID and region, define the DAG's name, start time and batch schedule interval; a batch operator defines and submits the job based on the batch file URI, any arguments, and required network or environment configurations.
- **Job history**: The UI displays a schedule showing the dates and times the job was triggered, confirming it's running at the intended interval.
- **Viewing Logs**: You can view logs for a specific job run by clicking on its task ID in the Composer UI.

### Production-grade systems

**Require**:
- CI/CD automation: store code in a repo (GitHub), and set up a pipeline using Cloud Build to automatically test and deploy changes.
- Data quality checks: integrate data quality steps into your Spark job or Composer DAG to ensure the raw data is valid and the transformations produce expected results.
- Monitoring and alerting: configure detailed logging in Cloud Logging, and set up alerts in Cloud Monitoring to notify you if a pipeline fails or if performance degrades.

## Unified observability

- Alternative to a fragmented monitoring strategy.
- A fragmented monitoring strategy is a liability, as it is slow to view each pipeline's component logs in isolation.
- Having a single place or view where you can correlate signals from across your entire system is known as unified observability.
- This is done on Google Cloud through the tight integration of Cloud Monitoring and Cloud Logging.

### Proactive monitoring

- Allows you to detect performance regressions, spot data quality issues, and debug failures quickly.
- **Workflow Health**: DAG run duration, task success/failure rate, task retry count.
- **Data Health**: Data freshness/lag, processed record count, DLQ count.
- **Performance and Cost**: Resource utilization, BigQuery slot usage.

**Python code used in a DAG for structured logging**

```python
# This function could be part of a Dataflow DoFn or a Spark map function.
def process_transaction(element):
    # In a real application, the client is often initialized once per worker.
    from google.cloud import logging as gcp_logging
    log_client = gcp_logging.Client()
    logger = log_client.logger("financial_pipeline_logs")

    # The correlation_id would be passed in as a pipeline argument from
    # the Cloud Composer DAG, e.g., using context variables like dag_run.run_id
    correlation_id = "scheduled__2025-08-29T14:00:00+00:00"

    try:
        # Business logic to validate the transaction
        amount = element.get("amount")

        # CORRECTED: Check for None first to avoid a TypeError
        if amount is None or not isinstance(amount, (int, float)) or amount < 0:
            raise ValueError("Invalid or missing transaction amount.")  # Using ValueError is more specific here

        #... more validation...

    except Exception as e:
        # Log a structured payload for actionable, queryable insights
        error_payload = {
            "message": f"Transaction validation failed: {str(e)}",
            "severity": "ERROR",
            "transaction_id": element.get("transaction_id"),
            "pipeline_stage": "validation",
            "correlation_id": correlation_id
        }
        logger.log_struct(error_payload)
        # Optionally, you would then yield this element to a Dead Letter Queue.
        return  # Stop processing this record

    #... continue processing valid record...
```

### Cloud Monitoring Dashboards

- Rather than looking at metrics individually, assemble them into a dedicated custom dashboard in Cloud Monitoring.
- Instead of requiring you to stare at these dashboards, you can be alerted when something goes wrong, then explore in the dashboard.

#### Centralized log analysis

- Cloud Logging is a fully managed service that automatically ingests, stores and analyzes log data from every component of your pipelines.
- Writes structured logs, which you transform into a powerful queryable database.
- Logs should be written as JSON with nested fields making them more searchable.

#### Proactive alerting

- Create policies that automatically notify you when specific conditions are met.

## Alerts and troubleshooting

- Logs are managed by Cloud Logging; Cloud Monitoring handles the alerting functionality.
- Create alert policies on time series metrics.
- **System metric based alerting**: Ideal for monitoring the overall health of your pipeline infrastructure.
- **Log based alerting**: For more nuanced, business-logic-specific issues, you can create log-based alerts to monitor for data quality problems.

### Alerts on system metrics

- Alert on different kinds of failure modes.
- Monitor outright failures, performance degradation, and issues that prevent the DAG from starting.

### Alerting on DAG Run Failures (Critical Alert)

- Get notified immediately if a DAG run fails.
1. In Cloud Logging or Monitoring, in the left side menu navigate to Alerting and click Create Policy.
2. Select a metric.
3. Filter the data (dag_id=..., state='failed').
4. Configure the trigger: set the condition to fire if the metric is above a threshold.
5. Configure notifications: get notified in Slack or by email.

### Alerting on Performance Degradation

- A DAG completing but taking much longer than usual can be a sign of a serious problem.
1. Select a metric (DAG run duration, composer.googleapis.com/dag/run/duration).
2. Filter the data (dag_id='...').
3. Configure the trigger: set the condition to fire if the metric is above the threshold.

### Alerting on DAG Parsing Errors

- Syntax errors or import issues can prevent Composer from even loading your DAG file.
- This alert notifies you if it is broken.
1. Select a metric (composer.googleapis.com/dag_processing/unhealthy_dag_file_count).
2. Configure the trigger: set the condition to fire if the metric is above the threshold of 0 for 5 minutes.

### Alerts on log-based metrics

#### Create the log-based metric

1. Navigate to log-based metrics in Cloud Logging and click Create Metric.
2. Metric type: choose counter, since you want to count the number of errors.
3. Provide details: give the metric a name.
4. Filter by using a query.
5. Click Create Metric (Cloud Logging will now count every new log entry that matches this filter and expose it as a new custom metric).

#### Create an alerting policy

1. Navigate to Alerting and click Create Policy.
2. Select a metric.
3. Configure the trigger: set the condition to fire if the delta value (rate of change) of the metric is above a threshold for a given time period.
4. Configure notifications.

### Fixing orchestrated batch pipelines

1. Use the Airflow UI in Cloud Composer to visually inspect the DAG run and look for failed or stuck tasks.
2. View Airflow logs; override the logging level to DEBUG for more detailed messages, then set it back to INFO when done to manage costs.
