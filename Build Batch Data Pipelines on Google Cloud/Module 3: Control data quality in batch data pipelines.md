# Module 3: Control data quality in batch data pipelines

## Table of contents

- [Data Validation](#data-validation)
  - [Dead letter queue (DLQ)](#dead-letter-queue-dlq)
    - [Dataflow DLQ implementation](#dataflow-dlq-implementation)
    - [Serverless for Apache Spark DLQ implementation](#serverless-for-apache-spark-dlq-implementation)
- [Log and analyze errors](#log-and-analyze-errors)
  - [Serverless for Apache Spark templates for error logs](#serverless-for-apache-spark-templates-for-error-logs)
    - [Steps](#steps)
    - [Key PySpark functions](#key-pyspark-functions)
- [Schema evolution for batch pipelines](#schema-evolution-for-batch-pipelines)
  - [Breaking changes](#breaking-changes)
  - [Schema-on-write vs schema-on-read](#schema-on-write-vs-schema-on-read)
- [Data integrity and duplication](#data-integrity-and-duplication)
  - [Handling duplicates in Dataflow](#handling-duplicates-in-dataflow)
    - [Two stage aggregation](#two-stage-aggregation)
  - [Window function](#window-function)
  - [Semantic Deduplication](#semantic-deduplication)
- [Deduplication with Serverless for Apache Spark](#deduplication-with-serverless-for-apache-spark)
- [Deduplication with Dataflow](#deduplication-with-dataflow)
  - [Post processing deduplication](#post-processing-deduplication)
- [Lab: Validate Data Quality for a Batch Data Pipeline using Serverless for Apache Spark](#lab-validate-data-quality-for-a-batch-data-pipeline-using-serverless-for-apache-spark)

## Data Validation

- Check data meets predefined rules and standards (data type checks and format constraints).
- Data cleansing is the process of correcting or removing inconsistencies, inaccurate or malformed data.
- **Completeness**: All expected data values are present, with no missing or null fields.
- **Conformity**: Checks if a single data point follows a rule, a predefined format, type or pattern.
- **Consistency**: Check if a data point makes sense when compared to other related data points, across different locations and over time.

### Dead letter queue (DLQ)

- Handle bad data without stopping your entire pipeline.
- When a record fails a validation check you route it to a separate location rather than crashing the entire job.
- In Dataflow, you implement a DLQ using a ParDo.
- Use a DoFn class to write a ParDo that contains the validation logic.
- ParDo functions can have multiple outputs, send invalid records to a special tagged side output (beam.pvalue.TaggedOutput).
- In Serverless for Apache Spark, utilise the DataFrame API or Spark SQL; avoid UDFs for complex validation as SQL is much more optimized.

#### Dataflow DLQ implementation

- There exists pre-built Dataflow templates with DLQ functionality.
- Handles parsing errors (if a row in the file doesn't match the expected schema).
- Also handles conversion errors (if a value can't be converted to the target BigQuery data type).

#### Serverless for Apache Spark DLQ implementation

```python
# --- Inside a new Spark job ---
from data_quality_templates import apply_standard_validations

# Call the template to get the clean data and the DLQ
clean_df, dlq_df = apply_standard_validations(
    source_df=raw_transactions_df, 
    not_null_cols=["transaction_id", "customer_id"],
    valid_country_codes=["DE", "FR", "US", "CA"]
)

# Now, write the resulting DataFrames to their destinations
clean_df.write.format("bigquery").save("project.dataset.main_table")
dlq_df.write.format("json").save("gs://my-bucket/dlq/")
```

## Log and analyze errors

- Identify trends in errors and triage issues.
- Use logs to understand the nature and volume of data errors.
- Also use error tables (BigQuery) that aggregate data quality issues.

### Serverless for Apache Spark templates for error logs

- Streams all driver and executor logs from the Spark application into Cloud Logging.
- The template detects a bad record and uses a logger to write a structured message in its output.

#### Steps

1. Load raw data into staging area like Cloud Storage bucket or Iceberg table (Spark job reads data into a DataFrame).
2. Apply custom business logic using Apache Spark (check for nulls, remove whitespace and ensure data type correctness).
3. Split records into valid or invalid, enrich invalid records with an error description and separate from main data batch.
4. Write valid records to a trusted table for downstream analysis.
5. Invalid data is isolated and stored in a separate table.

#### Key PySpark functions

```python
# Conditional function used to apply a rule when()
when(isnull(col("created_at"),...))

# Removes leading and trailing whitespace
trim()

# New string from multiple values (can be used to add a validation error message as a new column)
concat()

# Used to split data (invalid and valid)
filter()
```

## Schema evolution for batch pipelines

- **Additive**: New column, nullable column. Does not break the pipeline and is the simplest case to handle.
- **Breaking**: Deleting columns, changing data type. Requires more advanced patterns to avoid pipeline failures and downstream impact.
- In the Job Builder UI (Dataflow), two parameters are linked to schema evolution: write disposition (append or overwrite) and schema update options (allow new fields).
- With Serverless for Apache Spark, you enable schema merging by providing a specific parameter on the command line (--merge-schema=true).

### Breaking changes

- When renaming a column or altering a data type, implement a Facade View Pattern.
- Deploy a new pipeline to write to a new destination table.
- Create a unifying view that sits in front of both the old and new tables (utilises a UNION ALL statement).
- Downstream dashboards and reports point at the stable facade instead of the underlying tables.

### Schema-on-write vs schema-on-read

- **Schema-on-write**: schema is strictly enforced when the pipeline tries to write the data; this is how BigQuery operates.
- **Schema-on-read**: data is stored in its raw format (JSON or CSV files), structure is flexible, and schema is only applied when the pipeline reads the data.
- With schema-on-write, pipelines clean, structure and validate data before loading into storage; enforcing structure at write time benefits query performance and data quality since only data meeting the predefined structure is accepted, the trade-off is that it's more rigid.
- Schema-on-write is the data warehouse approach and requires more thought around schema drift; schema-on-read is the lake approach.
- With schema-on-read there is minimal upfront transformation, just load the raw data; this maximises flexibility with the drawback of slower query performance, since data must be validated on every read.

## Data integrity and duplication

- **Exact duplicates**: Caused by technical glitches that cause the same data to be sent more than once; inflates metrics like count and average.
- **Key-based duplicates**: Caused by business logic or architectural issues (multiple source systems updating the same entity, creating multiple states); corrupts analytics, making it impossible to get an accurate view of an entity's current state.
- **Semantic duplicates**: Caused by data modelling identity resolution failures from manual data entry, system migrations or merging datasets. Breaks the single customer view, leading to inaccurate customer lifetime value (LTV) calculations.

### Handling duplicates in Dataflow

```python
# Key based deduplication
beam.Distinct()
```

- GroupByKey works by shuffling all values for a given key to a single worker (becomes a problem if a key has billions of records).
- GroupByKey is preferred when you truly need all elements for a key, like joining two datasets.
- CombinePerKey is preferred for associative and commutative aggregations like finding the max value.
- Instead, use two-stage aggregation (beam.CombinePerKey with a custom beam.CombineFn), which distributes the heaviest load across the entire cluster.

#### Two stage aggregation

- Local combine (pre-shuffle): on each worker, the CombineFn reduces all records for a key down to a single best record for that worker.
- Global combine (post-shuffle): the shuffle now moves only these few pre-combined records; the final worker simply merges the candidates to find the globally best record.

**Deduplication**

```python
# The CombineFn is the robust, production-grade pattern for stateful reduction.
import apache_beam as beam

class GetLatestRecordFn(beam.CombineFn):
    """A CombineFn to find the record with the latest timestamp. This is inherently skew-resistant."""

    def create_accumulator(self):
        # The initial state is a placeholder for (record, timestamp)
        return (None, float('-inf'))

    def add_input(self, accumulator, new_record):
        # Compare the new record's timestamp with the latest one seen so far.
        current_record, max_timestamp = accumulator
        new_timestamp = new_record['event_timestamp']
        if new_timestamp > max_timestamp:
            return (new_record, new_timestamp)
        return accumulator

    def merge_accumulators(self, accumulators):
        # Merge the pre-combined results from different workers.
        return max(accumulators, key=lambda acc: acc[1])

    def extract_output(self, final_accumulator):
        # The final output is just the record itself.
        return final_accumulator[0]

# --- In the pipeline ---
latest_records = (p
                 | 'ReadData' >> beam.io.Read(...)
                 | 'KeyByTransactionId' >> beam.WithKeys(lambda r: r['transaction_id'])
                 # CombinePerKey is the scalable alternative to GroupByKey for this use case.
                 | 'SelectLatestRecord' >> beam.CombinePerKey(GetLatestRecordFn())
                 # The output of CombinePerKey is (key, value), so we extract the value.
                 | 'ExtractValue' >> beam.Values()
                )
```

### Window function

- Use for deduplication in Spark.
- **Provides**:
  - Absolute determinism: guarantees the exact same record is chosen every time for a job.
  - Clarity and readability: another developer can immediately understand the logic.
  - Scalable performance: requires a data shuffle, however modern Spark engines are highly optimized for this.
  - Adaptive Query Execution: automatically detects data skew by breaking large partitions into smaller chunks.

```python
# The window function is the production-standard for deterministic deduplication in Spark.
from pyspark.sql import Window
from pyspark.sql.functions import row_number

# 1. Define the window specification.
# Partition by the unique key and order by timestamp to find the latest record.
window_spec = Window.partitionBy("transaction_id").orderBy(df.event_timestamp.desc())

# 2. Add the rank column, then filter to keep only the latest record (rank=1).
# The original schema is preserved without a complex aggregation.
clean_df = (df.withColumn("rank", row_number().over(window_spec))
              .filter("rank = 1")
              .drop("rank")) # Drop the temporary rank column
```

### Semantic Deduplication

- Use a BigQuery ML model to identify similar customer records (ML.GENERATE_TEXT_EMBEDDING and ML.DISTANCE to find pairs that are semantically similar).
- **Govern with Dataplex**: Manage the BigQuery ML job as a Dataplex Data Quality Task. Allows you to schedule scans, track data quality scores over time and provide a governance layer for auditors.
- Automate notifications with Cloud Monitoring: a log-based alert watches for the successful-completion log of a Dataplex task; when the alert fires, it signifies that a new list of candidate duplicates is ready for review.
- Trigger the handoff with Pub/Sub and Cloud Functions: make the monitoring alert publish a notification to a Pub/Sub topic. A lightweight Cloud Function or Cloud Run service subscribes to this topic, and when a message is received, the function is triggered to execute the handoff logic.
- This handoff logic could be reading the candidate duplicate pairs from the BigQuery results table, calling an API to create a review ticket task (in Jira or Salesforce).

## Deduplication with Serverless for Apache Spark

- Window function isolates duplicate records without requiring expensive joins or shuffles.

```python
# Create a separate box for each unique combination
Window.partitionBy()

# Iterates over each box (or window) and assigns a sequential number to every row
row_number().over(window_spec)

# Keep only the row with rank of 1
.filter(col("rownum") == 1)
```

## Deduplication with Dataflow

- Use a deduplicate transform to remove duplicates before data ever reaches its destination.
- Discards any element it has already seen before.
- First derive a unique key (primary or composite primary key).
- Then apply the transform Deduplicate.by(key_function).
- Windowing does not require you to specify a time duration for batch pipelines; the transform just operates over the entire input PCollection.

```python
import apache_beam as beam
from apache_beam.transforms.deduplicate import Deduplicate

# Example PCollection with duplicate records
with beam.Pipeline() as p:
    raw_data = p | 'Create' >> beam.Create([
        {'id': 1, 'value': 'A'},
        {'id': 2, 'value': 'B'},
        {'id': 1, 'value': 'A'}, # Duplicate record
        {'id': 3, 'value': 'C'},
        {'id': 2, 'value': 'B'}  # Another duplicate
    ])
    
    # Deduplicate based on the 'id' field
    deduplicated_data = raw_data | 'Deduplicate' >> Deduplicate.by(lambda row: row['id'])

    # Log the results
    deduplicated_data | 'Log' >> beam.Map(print)

```

### Post processing deduplication

- Offloads computationally expensive deduplication to BigQuery, which is better optimized for this workload.
- Load data into a BigQuery staging table.
- Periodically run a scheduled query (Dataform or Cloud Composer) on the staging table.
- Deduplicate with SQL, copy only unique records from your staging table into the production table using a MERGE statement.

```sql
MERGE INTO `<project>.<dataset>.final_table` T
USING (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC) AS rn
  FROM `<project>.<dataset>.staging_table`
) S
ON T.id = S.id
WHEN NOT MATCHED BY TARGET AND S.rn = 1 THEN
  INSERT (id, value, timestamp) VALUES(S.id, S.value, S.timestamp)
```

## Lab: Validate Data Quality for a Batch Data Pipeline using Serverless for Apache Spark

**Fetch the first 10 records**
```bash
gsutil cat gs://qwiklabs-gcp-04-b73c52f3e78a-main-bucket/source/customer_contacts_1000.csv | head -n 11
```

**Create a new BigQuery dataset
```bash
bq mk customer_data_clean
```

**Create a python file for pyspark code**
```bash
nano customer_dq.py
```

**Email validation code**
```python
# Import necessary libraries

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# This script expects 1 command-line argument:
# 1. The destination BigQuery table path in format 'dataset.table'

if len(sys.argv) != 2:
    print("Usage: customer_dq.py <bq_dataset_table>")
    sys.exit(-1)

# Assign command-line argument to variable
bq_dataset_table = sys.argv[1]

# Lab variables are substituted here when the lab runs
bq_project = "qwiklabs-gcp-04-b73c52f3e78a"
gcs_source_path = f"gs://{bq_project}-main-bucket/source/customer_contacts_1000.csv"
gcs_dlq_path = f"gs://{bq_project}-dlq-bucket/errors/"

# Initialize a new Spark Session
spark = SparkSession.builder.appName("Customer DQ Check").getOrCreate()

# Step 1: Read the source CSV data from the GCS bucket
df = spark.read.option("header", "true").option("inferSchema", "true").csv(gcs_source_path)

# Step 2: Define the Data Quality rules
# Rule 1: The 'id' column must not be null.
dq_rule_id = col("id").isNotNull()

# Rule 2: The 'email' column must not be null and must match a valid email format regex.
email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
dq_rule_email = col("email").isNotNull().__and__(col("email").rlike(email_regex))

# Step 3: Apply rules and split the DataFrame into clean and error records
df_with_dq = df.withColumn("dq_passed", when(dq_rule_id.__and__(dq_rule_email), True).otherwise(False))
clean_df = df_with_dq.filter(col("dq_passed") == True).drop("dq_passed")
error_df = df_with_dq.filter(col("dq_passed") == False).drop("dq_passed")

# Step 4: Write the clean records to the specified BigQuery table
# The BigQuery connector requires a temporary GCS bucket NAME.
temp_gcs_bucket_name = f"{bq_project}-main-bucket"

clean_df.write \
    .format("bigquery") \
    .option("table", bq_dataset_table) \
    .option("temporaryGcsBucket", temp_gcs_bucket_name) \
    .option("project", bq_project) \
    .mode("overwrite") \
    .save()

# Step 5: Write the error records to the DLQ bucket in GCS as a single CSV file
error_df.repartition(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(gcs_dlq_path)

# Stop the Spark session
spark.stop()
```

**Upload PySpark script into a Cloud Storage bucket**
```bash
# The command below uploads the script to a 'scripts' folder in the main data bucket
gcloud storage cp customer_dq.py gs://qwiklabs-gcp-04-b73c52f3e78a-main-bucket/scripts/
```

**Create shortcuts to provisioned terraform**
```bash

# The name for the final table in BigQuery
export BQ_TABLE="valid_customers"

# The BigQuery table path in 'dataset.table' format
export BQ_DATASET_TABLE="customer_data_clean.${BQ_TABLE}"

# The path to the 1000-record source CSV file
export GCS_SOURCE_PATH="gs://qwiklabs-gcp-04-b73c52f3e78a-main-bucket/source/customer_contacts_1000.csv"

# The GCS path where error records will be written
export GCS_DLQ_PATH="gs://qwiklabs-gcp-04-b73c52f3e78a-dlq-bucket/errors/"

# The GCS path to the PySpark script you just uploaded
export PYSPARK_SCRIPT_PATH="gs://qwiklabs-gcp-04-b73c52f3e78a-main-bucket/scripts/customer_dq.py"

# The full URI of the custom subnet created by Terraform
export SUBNET_URI="projects/qwiklabs-gcp-04-b73c52f3e78a/regions/us-east4/subnetworks/spark-subnet"
```

- --subnet flag tells the job to run within the secure customer spark-subnet created by terraform (security best practice).
- --deps-bucket flag specifies a GCS bucket for the staging job dependencies.
- -- double dash separates the gcloud command flags from the arguments that will be passed into the pyspark scripts.

**Submit the job**
```bash
gcloud dataproc batches submit pyspark $PYSPARK_SCRIPT_PATH \
 --version=2.1 \
 --batch="customer-dq-job-$(date +%s)" \
 --region=us-east4 \
 --subnet=$SUBNET_URI \
 --deps-bucket=gs://qwiklabs-gcp-04-b73c52f3e78a-main-bucket \
 -- \
 $BQ_DATASET_TABLE`
```

- Clean records get written to a BigQuery table 
- Invalid records get routed to a CSV file in a cloud storage bucket for review (DLQ)

**View clean records**
```bash
bq query \
  --use_legacy_sql=false \
  'SELECT count(*) as total_clean_records FROM `customer_data_clean.valid_customers`;'
```

**View first 10 invalid records**
```bash
gcloud storage cat gs://qwiklabs-gcp-04-b73c52f3e78a-dlq-bucket/errors/*.csv | head -n 11
```