"""Serverless for Apache Spark batch job (Dataproc): validates raw Tiingo price
records, deduplicates by (ticker, date), and writes clean rows to BigQuery with
bad rows routed to a GCS dead letter queue."""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, concat, isnull, lit, monotonically_increasing_id, row_number, when


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("TiingoValidateAndDedupe").getOrCreate()


def validate(df):
    df = df.withColumn("validation_errors", lit(""))

    df = df.withColumn(
        "validation_errors",
        when(isnull(col("close")) | (col("close") <= 0), concat(col("validation_errors"), lit("INVALID_CLOSE;")))
        .otherwise(col("validation_errors")),
    )
    df = df.withColumn(
        "validation_errors",
        when(isnull(col("volume")) | (col("volume") < 0), concat(col("validation_errors"), lit("INVALID_VOLUME;")))
        .otherwise(col("validation_errors")),
    )
    df = df.withColumn(
        "validation_errors",
        when(isnull(col("date")) | isnull(col("ticker")), concat(col("validation_errors"), lit("MISSING_KEY;")))
        .otherwise(col("validation_errors")),
    )

    return df.withColumn("is_valid", when(col("validation_errors") == "", True).otherwise(False))


def dedupe(df):
    # The bronze lookback window (see ingest_tiingo.py) can land the same
    # (ticker, date) more than once across reruns; duplicate rows carry
    # identical data, so any deterministic tiebreak is fine.
    window_spec = Window.partitionBy("ticker", "date").orderBy(monotonically_increasing_id().desc())
    return (
        df.withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") == 1)
        .drop("rank")
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True, help="gs://bucket/bronze/*/{ds}.json")
    parser.add_argument("--output-table", required=True, help="project.dataset.table")
    parser.add_argument("--temp-bucket", required=True, help="scratch bucket for the BigQuery Spark connector")
    parser.add_argument("--dlq-path", required=True, help="gs://bucket/dlq/{ds}/")
    args = parser.parse_args()

    spark = build_spark_session()

    raw_df = spark.read.json(args.input_path)
    validated_df = validate(raw_df)

    clean_df = validated_df.filter(col("is_valid")).drop("is_valid", "validation_errors")
    dlq_df = validated_df.filter(~col("is_valid")).drop("is_valid")

    deduped_df = dedupe(clean_df)

    (
        deduped_df.write.format("bigquery")
        .option("temporaryGcsBucket", args.temp_bucket)
        .mode("append")
        .save(args.output_table)
    )

    if dlq_df.count() > 0:
        dlq_df.write.mode("append").json(args.dlq_path)

    spark.stop()


if __name__ == "__main__":
    main()
