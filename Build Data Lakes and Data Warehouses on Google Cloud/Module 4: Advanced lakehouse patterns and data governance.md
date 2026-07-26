# Module 4: Advanced lakehouse patterns and data governance

## Data Governance

- Covers who can access data, how sensitive data is protected, and where its metadata/lineage is tracked.
- Spans metadata management, access control (IAM), fine-grained security, and PII discovery/protection.

## Metadata

- Data about who created the data, when, what it contains, relationship to other data, who owns it, and its security sensitivity.

## Cloud Storage security

- Access controlled at the bucket level.
- Restrict to engineers and service accounts responsible for data ingestion.

## BigQuery

- Granular IAM control at dataset and table level.
- **Analysts:** read-only access to curated sales data.
- **Data scientists:** create and modify tables in sandbox datasets.

## Lake-house

- Extends BigQuery's fine-grained security to Cloud Storage data.

## Fine-grained security

- **Column-level security:** restrict access to specific columns of a table.
- **Row-level security:** filters which rows a user can access.
- For lake-house tables dynamic masking can also be applied.

## Sensitive Data Discovery

- Discovery, classification and protection.
- Discover fields with PII, classify them into security levels and mask personal data if not needed.
- You can set up a discovery to find sensitive data columns in tables.
- The discovery will scan through your tables and find columns likely to contain PII.

## ML on a lakehouse

- BigQuery ML allows you to create predictive models with just SQL

### Feature Engineering

- Create features or signals to help the model more accurately predict.

### Create the model

```sql
CREATE OR REPLACE MODEL cymbal_ecommerce.customer_churn_predictor
OPTIONS(model_type='LOGISTIC_REG') AS
SELECT
customer_id,
recency,
frequency,
monetary_value,
(total_purchases > 1) AS will_return -- This is our label
FROM
cymbal_ecommerce.customer_purchase_summary;
```

### Model evaluation

```sql
ML.EVALUATE
```

- Use the ML.EVALUATE function on the model.
- Provides metrics like accuracy, precision and recall.
- Helps to identify how well the model is performing.

### Prediction

```sql
ML.PREDICT
```
- Use ML.PREDICT function on the model to get the predictions and prediction probabilities.

## Agent platform for advanced ML

- Agent platform when more comprehensive models required and more flexibility is necessary.
- End-to-end platform for ML.
- Agent platform integrates directly with BigQuery.
- Provides a suite of MLOps tools to automate and monitor the entire ML lifecycle.
- Models can be deployed to the Model Registry on Gemini Enterprise Agent Platform.
- From the registry a model can be deployed to an endpoint which provides an API to connect to.

## Medallion architecture

### Bronze

- Raw data
- Could be clickstream data streamed in real-time via Pub/Sub which lands in a Cloud Storage Bucket.
- Bronze layer is normally immutable.
- Historic-record of what was received.

### Silver

- Cleaned, validated and enriched.
- Standardizing date and time formats.
- Joining data from different sources.
- Often stored in open format like Parquet or as a Lake-house table in cloud storage.

### Gold

- Business level data
- Refined, aggregated and optimized.
- Stored in native BigQuery tables.

## Migration Steps

1. **Establish the foundation:** setup project with IAM permissions, create cloud storage buckets, and setup knowledge catalog to manage metadata.
2. **Start with a high impact use case:** instead of migrating everything at once pick one business problem and solve it.
3. **Migrate the data:** Use BigQuery data transfer service, a Dataflow pipeline, or other tools depending on type of data and scale.
4. **Build new pipelines and reports:** populate bronze, silver, gold zones, and build dashboards from the new gold tables.
5. **Decommission and iterate:** decommission the section of the old system then pick the next business area to migrate.

## Cost management and optimization

- **Choose the right storage class:** not all data needs to be accessed at the same frequency, cheaper storage like nearline or cold line can be used.
- **Optimize BigQuery queries:** use features like partitioning and clustering.
- **Use BigQuery's flat-rate pricing:** Purchase dedicated processing capacity at a fixed monthly cost for predictable workflows.
- **Set up budgets and alerts:** get notified when costs approach limits.