import datetime

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, current_timestamp, lit, expr



# --- Configuration ---

# Define your GCS paths for the Iceberg and BigQuery jars

ICEBERG_SPARK_RUNTIME_JAR = "gs://<your-path>/iceberg/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"

ICEBERG_BIGQUERY_CATALOG_JAR = "gs://spark-lib/bigquery/iceberg-bigquery-catalog-1.6.1-1.0.1-beta.jar"



# Define your catalog properties

CATALOG_NAME = "bqms"

CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog"

CATALOG_BIGQUERY_IMPL = "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog"

WAREHOUSE_PATH = "gs://<your-bucket>/warehouse"

GCP_PROJECT = "<your-project-name>"

GCP_LOCATION = "<your-region>"



# Input and output table names

INPUT_ORDERS_TABLE = f"{CATALOG_NAME}.ecommerce.orders"

INPUT_ORDER_ITEMS_TABLE = f"{CATALOG_NAME}.ecommerce.order_items"

OUTPUT_TABLE = f"{CATALOG_NAME}.ecommerce.top_20_items_per_city_recent"



# --- SparkSession Initialization ---

# Create a SparkSession with the necessary Iceberg and BigQuery configurations.

# This block is essential when running as a standalone Python script via spark-submit.

spark = (

 SparkSession.builder.appName("Top20ItemsPerCityLastHourSQL")

 .config("spark.jars", f"{ICEBERG_SPARK_RUNTIME_JAR},{ICEBERG_BIGQUERY_CATALOG_JAR}")

 .config(f"spark.sql.catalog.{CATALOG_NAME}", CATALOG_IMPL)

 .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", CATALOG_BIGQUERY_IMPL)

 .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE_PATH)

 .config(f"spark.sql.catalog.{CATALOG_NAME}.gcp_project", GCP_PROJECT)

 .config(f"spark.sql.catalog.{CATALOG_NAME}.gcp_location", GCP_LOCATION)

 .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

 .getOrCreate()

)



spark.sparkContext.setLogLevel("WARN")



# --- Register Base Tables as Temporary Views for Spark SQL ---

spark.sql(f"""

  CREATE TEMPORARY VIEW orders_view AS

  SELECT

      id AS order_id,

      delivery_city,

      created_at

  FROM

      {INPUT_ORDERS_TABLE}

 """)



spark.sql(f"""

  CREATE TEMPORARY VIEW order_items_view AS

  SELECT

      order_id,

      id as item_id,

      1 as quantity -- Assuming quantity is always 1 as per original script

  FROM

      {INPUT_ORDER_ITEMS_TABLE}

 """)



# --- Find Top 20 Items per City using Spark SQL - Broken Down Steps ---



# Step 1: Join orders_view and order_items_view and filter for recent orders

spark.sql(f"""

  CREATE TEMPORARY VIEW joined_recent_orders_items_view AS

  SELECT

      o.order_id,

      o.delivery_city,

      o.created_at,

      oi.item_id,

      oi.quantity

  FROM

      orders_view o

  JOIN

      order_items_view oi ON o.order_id = oi.order_id

  WHERE

      o.created_at >= (TIMESTAMP '2025-06-23 10:55:00' - INTERVAL 1 HOUR)

 """)




# Step 2: Aggregate item sales per city

spark.sql(f"""

  CREATE TEMPORARY VIEW item_sales_per_city_view AS

  SELECT

      delivery_city,

      item_id,

      SUM(quantity) AS total_quantity_sold

  FROM

      joined_recent_orders_items_view

  GROUP BY

      delivery_city,

      item_id

 """)




# Step 3: Rank items within each city

spark.sql(f"""

  CREATE TEMPORARY VIEW ranked_items_view AS

  SELECT

      delivery_city,

      item_id,

      total_quantity_sold,

      ROW_NUMBER() OVER (PARTITION BY delivery_city ORDER BY total_quantity_sold DESC) as rank

  FROM

      item_sales_per_city_view

 """)




# Step 4: Filter for top 20 ranked items and get final DataFrame

top_items_per_city_df = spark.sql(f"""

  SELECT

      delivery_city,

      item_id,

      total_quantity_sold

  FROM

      ranked_items_view

  WHERE

      rank <= 20

 """)



# --- Write Results to a New Iceberg Table ---

top_items_per_city_df.writeTo(OUTPUT_TABLE) \

 .using("iceberg") \

 .createOrReplace()



# Stop the SparkSession (essential for standalone scripts)

spark.stop()