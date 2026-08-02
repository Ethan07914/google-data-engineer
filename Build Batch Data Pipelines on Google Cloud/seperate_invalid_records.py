from pyspark.sql import SparkSession

from pyspark.sql.functions import col, when, isnull, trim, length, lit, concat # Added concat here

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

from datetime import datetime

# --- Configuration ---

# Define your GCS paths for the Iceberg and BigQuery jars

ICEBERG_SPARK_RUNTIME_JAR = "gs://<your-bucket-name>/iceberg/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"

ICEBERG_BIGQUERY_CATALOG_JAR = "gs://spark-lib/bigquery/iceberg-bigquery-catalog-1.6.1-1.0.1-beta.jar"

# Add GCS Connector for reliable GCS access

# This is a common version; adjust the path/version if needed for your environment.

GCS_CONNECTOR_JAR = "gs://spark-lib/bigquery/gcs-connector-hadoop3-2.2.19.jar"

# --- Configuration ---

# Replace with your actual project ID, GCS bucket, and BigQuery catalog/warehouse details

GCP_PROJECT_ID = "<your-project-ID>"

GCP_LOCATION = "<your-region>"

ICEBERG_CATALOG_NAME = "bigquery_catalog" # A name for your Spark catalog

ICEBERG_WAREHOUSE_PATH = "gs://<path-to-warehouse>/warehouse"

# Define your catalog properties

CATALOG_BIGQUERY_IMPL = "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog"

CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog"

# Input and output table names

ICEBERG_TABLE_NAME = "events"

BIGQUERY_DATASET = "ecommerce" # The BigQuery dataset where your Iceberg table metadata is stored

INVALID_EVENT_DATA_TABLE="invalid_event_data"

# Define the schema for the valid records DataFrame

# This helps in ensuring the output schema is consistent after validation

valid_records_schema = StructType([

 StructField("id", IntegerType(), False),  # REQUIRED -> False for non-nullable

 StructField("user_id", IntegerType(), True),

 StructField("sequence_number", IntegerType(), True),

 StructField("session_id", StringType(), True),

 StructField("created_at", TimestampType(), True),

 StructField("ip_address", StringType(), True),

 StructField("city", StringType(), True),

 StructField("state", StringType(), True),

 StructField("postal_code", StringType(), True),

 StructField("browser", StringType(), True),

 StructField("traffic_source", StringType(), True),

 StructField("uri", StringType(), True),

 StructField("event_type", StringType(), True)

])

def main():

 # Initialize SparkSession with necessary configurations

 spark = (

  SparkSession.builder

  .appName("IcebergDataValidation")

  # Include all necessary jars, including the GCS connector

  .config("spark.jars", f"{ICEBERG_SPARK_RUNTIME_JAR},{ICEBERG_BIGQUERY_CATALOG_JAR},{GCS_CONNECTOR_JAR}")

  # Configure the Iceberg catalog to use BigQuery for metadata

  .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}", CATALOG_IMPL)

  .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.catalog-impl", CATALOG_BIGQUERY_IMPL)

  .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.gcp_project", GCP_PROJECT_ID)

  .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.gcp_location", GCP_LOCATION)

  .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.warehouse", ICEBERG_WAREHOUSE_PATH)

  # Enable Iceberg Spark extensions for DDL/DML operations

  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

  .getOrCreate()

 )

 # Set Spark log level to WARN to reduce verbosity

 spark.sparkContext.setLogLevel("WARN")

 # --- 1. Generate Synthetic Data for Testing (to ensure invalid records) ---

 # This section replaces the direct read from the Iceberg table for demonstration purposes.

 # It includes records that will intentionally fail the validation rules.

 invalid_data = [

  # Invalid: city empty

  (3, 111, 3, "session_i", datetime(2023, 1, 1, 10, 10, 0), "192.168.1.3", "", "IL", "60601", "Safari", "referral", "/checkout", "purchase"),

  # Invalid: city empty

  (8, 112, 3, "session_gi", datetime(2023, 1, 1, 10, 10, 0), "192.168.1.3", "", "IL", "60601", "Safari", "referral", "/checkout", "purchase"),

  # Invalid: USER_ID_INVALID (negative)

  (3, -5, 3, "session_ghi", datetime(2023, 1, 1, 10, 10, 0), "192.168.1.3", "Chicago", "IL", "60601", "Safari", "referral", "/checkout", "purchase"),

  # Invalid: SESSION_ID_EMPTY

  (4, 104, 4, "", datetime(2023, 1, 1, 10, 15, 0), "192.168.1.4", "Houston", "TX", "77001", "Edge", "direct", "/about", "page_view"),

  # Invalid: CREATED_AT_NULL

  (5, 105, 5, "session_jkl", None, "192.168.1.5", "Phoenix", "AZ", "85001", "Chrome", "social", "/contact", "click"),

  # Valid record with some nulls where allowed

  (6, None, 6, "session_mno", datetime(2023, 1, 1, 10, 25, 0), "192.168.1.6", "Philadelphia", None, None, "Chrome", "email", "/promo", "impression"),

  # Invalid: USER_ID_INVALID (zero) and SESSION_ID_EMPTY

  (7, 0, 7, "   ", datetime(2023, 1, 1, 10, 30, 0), "192.168.1.7", "San Antonio", "TX", "78201", "Firefox", "organic", "/blog", "page_view")

 ]

 invalid_data_df = spark.createDataFrame(invalid_data, schema=valid_records_schema)

 # --- 1. Read from Iceberg Table ---

 # Construct the fully qualified table name (catalog.dataset.table)

 full_table_name = f"{ICEBERG_CATALOG_NAME}.{BIGQUERY_DATASET}.{ICEBERG_TABLE_NAME}"

 print(f"Attempting to write wrong data to Iceberg table: {full_table_name}")

 (

 invalid_data_df.writeTo(full_table_name)

 .using("iceberg")

 .createOrReplace()

 )

 print(f"Attempting to read from Iceberg table: {full_table_name}")

 try:

 df = spark.table(full_table_name).limit(10)

 print("Successfully read from Iceberg table.")

 df.printSchema()

 df.show(5)

 except Exception as e:

 print(f"Error reading from Iceberg table: {e}")

 spark.stop()

 return

 # --- 2. Implement Data Validation Rules and Cleansing Logic ---

 # Initialize a new column 'validation_errors' to capture all issues

 df_with_validation = df.withColumn("validation_errors", lit(""))

 # Rule 1: 'id' is REQUIRED and should not be null

 df_with_validation = df_with_validation.withColumn(

  "validation_errors",

  when(isnull(col("id")), concat(col("validation_errors"), lit("ID_NULL;"))) # Changed to use concat function

  .otherwise(col("validation_errors"))

 )

 # Rule 2: 'user_id' should be a positive integer if not null

 df_with_validation = df_with_validation.withColumn(

  "validation_errors",

  when((col("user_id").isNotNull()) & (col("user_id") <= 0), concat(col("validation_errors"), lit("USER_ID_INVALID;"))) # Changed to use concat function

  .otherwise(col("validation_errors"))

 )

 # Cleansing: If 'user_id' is null, set it to -1 as an example cleansing step

 df_with_validation = df_with_validation.withColumn(

  "user_id",

  when(isnull(col("user_id")), lit(-1)).otherwise(col("user_id"))

 )

 # Rule 3: 'created_at' should be a valid timestamp (i.e., not null)

 df_with_validation = df_with_validation.withColumn(

  "validation_errors",

  when(isnull(col("created_at")), concat(col("validation_errors"), lit("CREATED_AT_NULL;"))) # Changed to use concat function

  .otherwise(col("validation_errors"))

 )

 # Rule 4: 'session_id' should not be an empty string if not null

 df_with_validation = df_with_validation.withColumn(

  "validation_errors",

  when((col("session_id").isNotNull()) & (trim(col("session_id")) == ""), concat(col("validation_errors"), lit("SESSION_ID_EMPTY;"))) # Changed to use concat function

  .otherwise(col("validation_errors"))

 )

 # Rule 4: 'session_id' should not be an empty string if not null

 df_with_validation = df_with_validation.withColumn(

  "validation_errors",

  when((col("city").isNotNull()) & (trim(col("city")) == ""), concat(col("validation_errors"), lit("CITY_EMPTY;")))

  .otherwise(col("validation_errors"))

 )

 # Cleansing: Trim whitespace from all relevant string columns

 string_cols = ["session_id", "ip_address", "city", "state", "postal_code", "browser", "traffic_source", "uri", "event_type"]

 for c in string_cols:

 df_with_validation = df_with_validation.withColumn(c, trim(col(c)))

 # Determine overall validity: a record is valid if 'validation_errors' is empty

 df_with_validation = df_with_validation.withColumn(

  "is_valid",

  when(length(trim(col("validation_errors"))) == 0, True).otherwise(False)

 )

 # --- 3. Separate Invalid Records ---

 # Filter for valid records and drop the validation-specific columns

 valid_records_df = df_with_validation.filter(col("is_valid") == True).drop("is_valid", "validation_errors")

 # Filter for invalid records and drop the 'is_valid' column

 invalid_records_df = df_with_validation.filter(col("is_valid") == False).drop("is_valid")

 print("\n--- Valid Records ---")

 valid_records_df.printSchema()

 valid_records_df.show(truncate=False)

 print(f"Number of valid records: {valid_records_df.count()}")

 print("\n--- Invalid Records ---")

 invalid_records_df.printSchema()

 invalid_records_df.show(truncate=False)

 print(f"Number of invalid records: {invalid_records_df.count()}")

 output_table = f"{ICEBERG_CATALOG_NAME}.{BIGQUERY_DATASET}.{INVALID_EVENT_DATA_TABLE}"

 print(f"Writing invalid records to: {output_table}")

 (

 invalid_records_df.writeTo(output_table)

 .using("iceberg")

 .createOrReplace()

 )

 spark.stop()

if name == "__main__":

 main()

