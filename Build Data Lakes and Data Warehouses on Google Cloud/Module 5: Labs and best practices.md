# Module 5: Labs and best practices

## Lab: Getting Started with BigQuery ML

### Create model

- Alias what you're trying to predict as `label`.

```sql
#standardSQL
CREATE OR REPLACE MODEL `bqml_lab.sample_model`
OPTIONS(model_type='logistic_reg') AS
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  IFNULL(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  IFNULL(geoNetwork.country, "") AS country,
  IFNULL(totals.pageviews, 0) AS pageviews
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20160801' AND '20170631'
LIMIT 100000;
```

### Evaluate the model

```sql
#standardSQL
SELECT
  *
FROM
  ml.EVALUATE(MODEL `bqml_lab.sample_model`, (
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  IFNULL(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  IFNULL(geoNetwork.country, "") AS country,
  IFNULL(totals.pageviews, 0) AS pageviews
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20170701' AND '20170801'));
```

#### Result

Outputs training metrics:

| precision | recall | accuracy | f1_score | log_loss | roc_auc |
| --- | --- | --- | --- | --- | --- |
| 0.42708333333333331 | 0.076350093109869649 | 0.98518179862306365 | 0.12954186413902052 | 0.047963077954367654 | 0.982922077922078 |

### Use the model

#### By country

```sql
#standardSQL
SELECT
  country,
  SUM(predicted_label) as total_predicted_purchases
FROM
  ml.PREDICT(MODEL `bqml_lab.sample_model`, (
SELECT
  IFNULL(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  IFNULL(totals.pageviews, 0) AS pageviews,
  IFNULL(geoNetwork.country, "") AS country
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20170701' AND '20170801'))
GROUP BY country
ORDER BY total_predicted_purchases DESC
LIMIT 10;
```

#### By user

```sql
#standardSQL
SELECT
  fullVisitorId,
  SUM(predicted_label) as total_predicted_purchases
FROM
  ml.PREDICT(MODEL `bqml_lab.sample_model`, (
SELECT
  IFNULL(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  IFNULL(totals.pageviews, 0) AS pageviews,
  IFNULL(geoNetwork.country, "") AS country,
  fullVisitorId
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20170701' AND '20170801'))
GROUP BY fullVisitorId
ORDER BY total_predicted_purchases DESC
LIMIT 10;
```

## Lab: Vector search with BigQuery

- BigQuery Vector Search allows you to find the most similar items in your dataset.
- It compares the mathematical representations of their features (embeddings).
- Steps: create an ML model to generate embeddings, create a vector index, then use the `VECTOR_SEARCH` function against the embeddings you created.

### Create an AI model

```sql
CREATE OR REPLACE MODEL `bqml_lab.embedding_model`
  REMOTE WITH CONNECTION DEFAULT
  OPTIONS (ENDPOINT = 'text-embedding-005');
```

### Create table to store embeddings

```sql
CREATE OR REPLACE TABLE `bqml_lab.embeddings` AS
SELECT * FROM ML.GENERATE_EMBEDDING(
  MODEL `bqml_lab.embedding_model`,
  (
    SELECT
      title,
      url,
      abstract AS content
    FROM
      `bqml_lab.patent_data`
    LIMIT 200000
  ))
WHERE LENGTH(ml_generate_embedding_status) = 0;
```

### Create vector index

```sql
CREATE OR REPLACE VECTOR INDEX my_index
ON `bqml_lab.embeddings`(ml_generate_embedding_result)
OPTIONS(index_type = 'IVF',
  distance_type = 'COSINE',
  ivf_options = '{"num_lists":500}');
```

- Index is ready when `coverage_percentage` > 0 and `last_refresh_time` is not null.

```sql
SELECT
  table_name,
  index_name,
  index_status,
  coverage_percentage,
  last_refresh_time,
  disable_reason
FROM `PROJECT_ID.bqml_lab.INFORMATION_SCHEMA.VECTOR_INDEXES`;
```

### Perform text similarity test

- Gives you the top-k most similar phrases.

```sql
SELECT query.query, base.title, base.content
FROM VECTOR_SEARCH(
  TABLE `bqml_lab.embeddings`, 'ml_generate_embedding_result',
  (
    SELECT ml_generate_embedding_result, content AS query
    FROM ML.GENERATE_EMBEDDING(
      MODEL `bqml_lab.embedding_model`,
      (SELECT 'improving online shopper search results' AS content))
  ),
  top_k => 5, options => '{"fraction_lists_to_search": 0.01}');
```