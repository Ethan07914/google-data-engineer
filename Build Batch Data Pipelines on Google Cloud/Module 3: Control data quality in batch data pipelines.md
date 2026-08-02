# Module 3: Control data quality in batch data pipelines

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

