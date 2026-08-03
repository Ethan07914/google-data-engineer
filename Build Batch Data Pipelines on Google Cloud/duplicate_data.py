from pyspark.sql import SparkSession

from pyspark.sql.functions import col, row_number, lit

from pyspark.sql.window import Window

# --- Configuration ---

# Define your GCS paths for the Iceberg and BigQuery jars

ICEBERG_SPARK_RUNTIME_JAR = "gs://<your-bucket-name>/iceberg/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"

ICEBERG_BIGQUERY_CATALOG_JAR = "gs://spark-lib/bigquery/iceberg-bigquery-catalog-1.6.1-1.0.1-beta.jar"

# Define your catalog properties

CATALOG_NAME = "bqms"

CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog"

CATALOG_BIGQUERY_IMPL = "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog"

WAREHOUSE_PATH = "gs://<your-bucket-name>/warehouse"

GCP_PROJECT = "<your-project-name>"

GCP_LOCATION = "<your-region>"

# Input and output table names

INPUT_EVENTS_TABLE = f"{CATALOG_NAME}.ecommerce.random_events_sampled"

DUPLICATE_EVENTS_TABLE = f"{CATALOG_NAME}.ecommerce.random_events_duplicated"

DE_EVENTS_TABLE = f"{CATALOG_NAME}.ecommerce.random_events_de_duplicated"

def remove_duplicate_events(df):

 """

 Removes duplicate rows from the specified DataFrame.

 Args:

     df (pyspark.sql.DataFrame): The input DataFrame.

 Returns:

     pyspark.sql.DataFrame: The DataFrame without duplicate rows.

 """

 try:

     # Create a window specification to partition by all columns and order by a dummy column

     window_spec = Window.partitionBy(*[col(c) for c in df.columns]).orderBy(lit(0))

     # Assign a row number to each row within each partition

     df_with_rownum = df.withColumn("rownum", row_number().over(window_spec))

     # Filter out rows where row number is not 1 (i.e., keep only the first occurrence of each duplicate)

     df_without_duplicates = df_with_rownum.filter(col("rownum") == 1).drop("rownum")

     return df_without_duplicates

 except Exception as e:

     print(f"Error removing duplicates: {e}")

     raise e

# --- SparkSession Initialization ---

# Create a SparkSession with the necessary Iceberg and BigQuery configurations.

spark = (

 SparkSession.builder.appName("RandomlySampledEvents")

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

 # --- Read Data ---

 print(f"Reading data from {INPUT_EVENTS_TABLE}")

 source_df = spark.table(INPUT_EVENTS_TABLE)

 # --- Duplicate Data (x2) ---

 print(f"Duplicating data (x2) and writing to {DUPLICATE_EVENTS_TABLE}")

 duplicated_df = source_df.union(source_df)

 duplicated_df.writeTo(DUPLICATE_EVENTS_TABLE).using("iceberg").createOrReplace()

 # --- Read Data from Duplicated table ---

 print(f"Reading data from {DUPLICATE_EVENTS_TABLE}")

 duplicated_rep_df = spark.table(DUPLICATE_EVENTS_TABLE)

 # --- Removing duplicates from duplicated table ---

 print(f"Removing duplicates from {DUPLICATE_EVENTS_TABLE} AND written to {DE_EVENTS_TABLE}")

 deduplicated_original_df = remove_duplicate_events(duplicated_rep_df)

 deduplicated_original_df.writeTo(DE_EVENTS_TABLE).using("iceberg").createOrReplace()

 print(f"Processing finished successfully")

except Exception as e:

 print(f"An error occurred: {e}")

 raise e

finally:

 # Stop the SparkSession

 spark.stop()