# Module 2: Building a data lakehouse with Cloud Storage, open formats, and BigQuery

## Cloud Storage

- Used to store vast amounts of data in different formats

## Apache Iceberg

- Type of open table format.
- Adds a metadata and structured layer on top of Cloud Storage files.
- Doesn't move the data, just organizes it — acts as an index and catalog for data files.
- You can even run UPDATE, DELETE and MERGE statements from BigQuery on Iceberg tables
### Provides

- **Schema evolution:** allows data schemas to evolve with business needs, e.g. adding and renaming columns.
- **Hidden partitioning:** manages how data is partitioned for efficient queries.
- **Time travel:** allows querying of past versions of the data, reproduces previous states.
- **Atomic transactions:** reliable, concurrent operations on data, ensures multiple processes can read and write to the same tables without data corruption.

## BigQuery as a Data Warehouse

- Fully managed, serverless data warehouse for fast SQL analytics at scale.
- Stores structured, native data in its own optimized columnar storage.
- Separates storage and compute so each can scale independently.

## BigQuery in Lake-house architecture

- Query data from Cloud Storage with Apache Iceberg metadata layer through a familiar SQL interface in BigQuery.
- No duplication of data is needed.

## AlloyDB

- Fully managed PostgreSQL database
- Handles very high transaction volumes
- Low latency for read and write
- Ensures strong data consistency

## Federated Queries

- Query data in external system without moving or copying data into BigQuery.

## Lab: Federated query with BigQuery

### Federated Query

```sql
WITH log AS (
  SELECT customer_id, log_id, timestamp, url FROM EXTERNAL_QUERY("qwiklabs-gcp-02-b9429eb36f51.us-central1.AlloyDB-weblog", "SELECT customer_id, CAST(log_id AS VARCHAR(200)) AS log_id, timestamp, url FROM web_log LIMIT 100"))
SELECT  log.customer_id
, log.timestamp
, log.url
, C.*
FROM customers.customer_details AS C
INNER JOIN log
ON C.id = log.customer_id
ORDER BY C.id
LIMIT 100;
```

