import datetime

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, current_timestamp, lit



# --- Configuration ---

# Define your GCS paths for the Iceberg and BigQuery jars

ICEBERG_SPARK_RUNTIME_JAR = "gs://<your-bucket-name>/iceberg/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"

ICEBERG_BIGQUERY_CATALOG_JAR = "gs://spark-lib/bigquery/iceberg-bigquery-catalog-1.6.1-1.0.1-beta.jar"



# Define your catalog properties

CATALOG_NAME = "bqms"

CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog"

CATALOG_BIGQUERY_IMPL = "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog"

WAREHOUSE_PATH = "gs://dw-387711160261-bucket/warehouse"

GCP_PROJECT = "<your-project-ID>"

GCP_LOCATION = "<your-region>"



# Input and output table names

INPUT_EVENTS_TABLE = f"{CATALOG_NAME}.ecommerce.random_events_sampled"



# --- SparkSession Initialization ---

# Create a SparkSession with the necessary Iceberg and BigQuery configurations.

spark = (

  SparkSession.builder.appName("DemonstrateSchemaEvolution")

  .config("spark.jars", f"{ICEBERG_SPARK_RUNTIME_JAR},{ICEBERG_BIGQUERY_CATALOG_JAR}")

  .config(f"spark.sql.catalog.{CATALOG_NAME}", CATALOG_IMPL)

  .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", CATALOG_BIGQUERY_IMPL)

  .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE_PATH)

  .config(f"spark.sql.catalog.{CATALOG_NAME}.gcp_project", GCP_PROJECT)

  .config(f"spark.sql.catalog.{CATALOG_NAME}.gcp_location", GCP_LOCATION)

  # Ensure Iceberg extensions are enabled for Spark SQL

  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

  .getOrCreate()

)



spark.sparkContext.setLogLevel("WARN")



try:



 spark.sql(f"""

      ALTER TABLE {INPUT_EVENTS_TABLE}

      ADD COLUMN OS STRING COMMENT 'Operating System derived from browser agent string'

  """).show()



 spark.sql(f"""UPDATE {INPUT_EVENTS_TABLE}

  SET OS = CASE

      WHEN browser ILIKE '%iPhone%' OR browser ILIKE '%iPad%' OR browser ILIKE '%iOS%' THEN 'iOS'

      WHEN browser ILIKE '%Android%' THEN 'Android'

      WHEN browser ILIKE '%Mac OS X%' OR browser ILIKE '%Macintosh%' THEN 'Mac'

      WHEN browser ILIKE '%Windows NT%' THEN 'Windows'

      ELSE 'Other/Unknown' -- Handle cases that don't match or are not relevant

  END

  WHERE OS IS NULL; -- Only update rows where OS is NULL for initial data population

  """).show()



except Exception as e:

 print(f"An error occurred: {e}")

 raise e

finally:

 # Stop the SparkSession

 spark.stop()