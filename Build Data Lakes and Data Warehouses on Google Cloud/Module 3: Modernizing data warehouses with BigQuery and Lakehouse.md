# Module 3: Modernizing data warehouses with BigQuery and Lakehouse

## Table of contents

- [BigQuery](#bigquery)
  - [Slots and shuffle](#slots-and-shuffle)
- [Partitioning and clustering](#partitioning-and-clustering)
- [Lakehouse in BigQuery](#lakehouse-in-bigquery)
- [Lab: Querying external data and Iceberg tables](#lab-querying-external-data-and-iceberg-tables)
  - [Task 2. Create and load the Iceberg table in Cloud Storage with BigQuery](#task-2-create-and-load-the-iceberg-table-in-cloud-storage-with-bigquery)

## BigQuery

- Fully managed, serverless data warehouse.
- Separates storage from compute.
- Uses distributed compute engine (Dremel) to make queries run incredibly fast.
- Thousands of compute resources scan the tables simultaneously.
- Storage and compute can scale independently.
- Only pay for what you use.

### Slots and shuffle

- A slot is a virtual worker (small unit of computational power); thousands of slots are assigned to each scan a small amount of data (massively parallel processing).
- Shuffle gathers and reorganizes data between stages of a query.

## Partitioning and clustering

- **Partitioning:** separates data into sections (year, month or day); tables can be partitioned on a date or integer column.
- **Clustering:** user-defined column sort order, sorts data into storage blocks based on their values.

## Lakehouse in BigQuery

- BigQuery can act as the query engine over both native storage and open-format tables (e.g. Iceberg) in Cloud Storage, unifying warehouse and lake workloads.
- BigQuery tables (`table_format = 'ICEBERG'`) store data in Iceberg format in Cloud Storage while still being managed and queried through BigQuery.
- This lets teams modernize a warehouse into a lakehouse without moving data out of BigQuery-managed storage.

## Lab: Querying external data and Iceberg tables

### Task 2. Create and load the Iceberg table in Cloud Storage with BigQuery

```sql
CREATE TABLE cymbal_lake.iceberg_web_log
WITH CONNECTION `projects/qwiklabs-gcp-02-00feec23fc07/locations/us-central1/connections/gcs-bucket-qwiklabs-gcp-02-00feec23fc07_eds`
OPTIONS (
 table_format = 'ICEBERG',
 storage_uri = 'gs://gcs-bucket-qwiklabs-gcp-02-00feec23fc07') AS
SELECT * FROM `cymbal_lake.web_log`;
```