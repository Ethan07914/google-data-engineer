# Module 1: Introduction to modern data engineering on Google Cloud

## Table of contents

- [Data Lakes](#data-lakes)
- [Data Warehouses](#data-warehouses)
- [Data Lake-house](#data-lake-house)
- [Choosing between warehouse, lake and lake-house](#choosing-between-warehouse-lake-and-lake-house)
  - [Data Warehouse when](#data-warehouse-when)
  - [Data Lake when](#data-lake-when)
  - [Data Lake-house when](#data-lake-house-when)

## Data Lakes

- Stores large amount of data in raw format
- Any kind of data (structured, semi-structured and unstructured)
- Structured (Databases), Semi-Structured (JSON), Unstructured (Video)
- Store everything now, figure out how to use it later
- Store all data types
- **Advantages:** Extremely scalable, cost-effective, fast to ingest, support AI and ML
- **Disadvantages:** Without governance can become disorganized, requires management overhead, time-consuming for analysis, raw formats increase security risk.

## Data Warehouses

- Stores structured, processed data optimized for analysis and reporting
- Data is cleaned, transformed and schema-defined before loading (schema on write)
- Optimized for fast SQL queries and interactive BI
- **Advantages:** High performance, strong consistency, easy for analysts to query, mature tooling
- **Disadvantages:** Less flexible for unstructured/semi-structured data, upfront transformation cost, can be more expensive to store large raw volumes

## Data Lake-house

- Combines flexibility and low cost of data lake with speed and accuracy of a warehouse.
- Introduces a metadata and governance layer on top of open format files in low cost object storage.
- Breaks down data silos by storing different formats in the same place so they can be analyzed together.
- **Features:** support most formats, schema on read or schema on write, flexible cost, unified governance, ACID transaction support.
- Data lives in open formats like parquet, ORC and avro which is queryable by BigQuery.
- **Advantages:** Reduced data redundancy, unified governance, broken down silos, improved flexibility.

## Choosing between warehouse, lake and lake-house

### Data Warehouse when

- Primary need is speed
- Interactive BI on structured data

### Data Lake when

- Massive volumes of raw data
- Low cost is important
- AI/ML research and development is being considered

### Data Lake-house when

- Support for both AI/ML and BI is needed