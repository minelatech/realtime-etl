"""PySpark Structured Streaming job: Kafka → clean → aggregate → sink.

Run with spark-submit (see README). The per-record cleaning reuses the exact rules
from :mod:`etl.transforms`; windowed aggregation is done in Spark SQL for scale.

    spark-submit \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
      etl/streaming.py
"""
from __future__ import annotations

from etl.config import config
from schemas.event import spark_schema


def build_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("realtime-etl")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def clean_stream(df):
    """Parse the Kafka value JSON, validate and enrich. Returns (clean_df, dlq_df)."""
    from pyspark.sql import functions as F

    parsed = (
        df.selectExpr("CAST(value AS STRING) AS raw")
        .withColumn("json", F.from_json(F.col("raw"), spark_schema()))
    )

    # DLQ: rows that failed JSON parsing or basic validation
    valid_cond = (
        F.col("json").isNotNull()
        & F.col("json.event_id").isNotNull()
        & (F.col("json.amount") >= 0)
        & F.col("json.type").isin(*list(_event_types()))
    )
    dlq_df = parsed.where(~valid_cond).select("raw", F.lit("invalid").alias("reason"))

    clean_df = (
        parsed.where(valid_cond)
        .select("json.*")
        .withColumn("ts", F.to_timestamp("ts"))
        .withColumn("event_date", F.to_date("ts"))
        .withColumn("is_high_value", F.col("amount") >= F.lit(config.high_value_threshold))
        .dropDuplicates(["event_id"])          # idempotent ingestion
        .withWatermark("ts", "10 minutes")
    )
    return clean_df, dlq_df


def aggregate_stream(clean_df):
    from pyspark.sql import functions as F

    return (
        clean_df.groupBy(
            F.window("ts", f"{config.window_minutes} minutes"),
            F.col("type"),
            F.col("region"),
        )
        .agg(F.count("*").alias("count"), F.sum("amount").alias("total_amount"))
        .select("window.start", "window.end", "type", "region", "count", "total_amount")
    )


def _event_types():
    from schemas.event import EVENT_TYPES

    return EVENT_TYPES


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    clean_df, dlq_df = clean_stream(raw)
    agg_df = aggregate_stream(clean_df)

    # sink 1: raw clean events → partitioned parquet
    q1 = (
        clean_df.writeStream.format("parquet")
        .option("path", f"{config.warehouse_uri}/events")
        .option("checkpointLocation", f"{config.checkpoint_dir}/events")
        .partitionBy("event_date")
        .outputMode("append")
        .start()
    )

    # sink 2: windowed aggregates → console (swap for a DB sink in production)
    q2 = (
        agg_df.writeStream.format("console")
        .option("checkpointLocation", f"{config.checkpoint_dir}/agg")
        .outputMode("update")
        .start()
    )

    # sink 3: dead-letter queue
    q3 = (
        dlq_df.writeStream.format("json")
        .option("path", f"{config.warehouse_uri}/dlq")
        .option("checkpointLocation", f"{config.checkpoint_dir}/dlq")
        .outputMode("append")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
