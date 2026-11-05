# Databricks notebook source
"""Full replacement Bronze ingestion; execute on Databricks serverless."""

import logging
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, LongType, StringType


SOURCE_PATH = "/Volumes/ai_dev_class_demo/ai_dev_schema/ai_dev_raw/player_master.parquet"
TARGET_TABLE = "ai_dev_class_demo.bronze_layer.player_master"

# Ordered source contract from docs/player_master_data_dictionary.md.
# Decimal precision is inferred from Parquet, not fixed by the dictionary.
EXPECTED_COLUMNS = [
    ("player_id", LongType),
    ("external_player_key", StringType),
    ("first_name", StringType),
    ("last_name", StringType),
    ("gender", StringType),
    ("birth_date", StringType),
    ("dominant_hand", StringType),
    ("home_region_id", LongType),
    ("country_code", StringType),
    ("registration_date", StringType),
    ("player_status", StringType),
    ("rating", DecimalType),
    ("rating_value", DecimalType),
    ("confidence_score", DecimalType),
    ("volatility_score", DecimalType),
    ("global_percentile", DecimalType),
    ("match_count_used", LongType),
    ("rating_date", StringType),
    ("rating_batch_id", LongType),
    ("snapshot_month", StringType),
]

logger = logging.getLogger("player_master_bronze")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
logger.propagate = False


def validate_schema(schema):
    expected_names = [name for name, _ in EXPECTED_COLUMNS]
    if schema.fieldNames() != expected_names:
        raise ValueError(f"Expected ordered columns {expected_names}; got {schema.fieldNames()}")
    for field, (_, expected_type) in zip(schema.fields, EXPECTED_COLUMNS):
        if not isinstance(field.dataType, expected_type):
            raise ValueError(f"Unexpected type for {field.name}: {field.dataType}")
        if isinstance(field.dataType, DecimalType):
            expected_scale = 2 if field.name == "global_percentile" else 3
            if field.dataType.scale != expected_scale:
                raise ValueError(f"Unexpected decimal scale for {field.name}: {field.dataType}")


def main():
    started = time.monotonic()
    spark = SparkSession.builder.getOrCreate()
    logger.info("Starting source=%s target=%s mode=overwrite", SOURCE_PATH, TARGET_TABLE)
    try:
        source = spark.read.parquet(SOURCE_PATH)
        logger.info("Source schema=%s", source.schema.simpleString())
        validate_schema(source.schema)
        source_count = source.count()
        logger.info("Source row count=%d", source_count)

        # Only create the missing schema; never drop or replace a namespace.
        spark.sql("CREATE SCHEMA IF NOT EXISTS ai_dev_class_demo.bronze_layer")
        if spark.catalog.tableExists(TARGET_TABLE):
            detail = spark.sql(f"DESCRIBE DETAIL {TARGET_TABLE}").first()
            if detail["format"].lower() != "delta":
                raise ValueError("Existing destination must be a Delta table")
            existing_schema = spark.table(TARGET_TABLE).schema
            if [(f.name, f.dataType) for f in existing_schema] != [
                (f.name, f.dataType) for f in source.schema
            ]:
                raise ValueError("Existing destination schema differs; review before replacement")

        # No casts, cleanup, deduplication, added fields, or automatic schema evolution.
        source.write.format("delta").mode("overwrite").option(
            "mergeSchema", "false"
        ).option("overwriteSchema", "false").saveAsTable(TARGET_TABLE)

        output = spark.table(TARGET_TABLE)
        output_count = output.count()
        logger.info("Destination row count=%d", output_count)
        detail = spark.sql(f"DESCRIBE DETAIL {TARGET_TABLE}").first()
        if detail["format"].lower() != "delta":
            raise ValueError("Destination format is not Delta")
        if [(f.name, f.dataType) for f in output.schema] != [
            (f.name, f.dataType) for f in source.schema
        ]:
            raise ValueError("Destination schema does not match source")
        if source_count != output_count:
            raise ValueError("Source and destination row counts differ")
        if source.exceptAll(output).limit(1).count() or output.exceptAll(source).limit(1).count():
            raise ValueError("Source and destination rows differ")
        logger.info("SUCCESS row_count=%d elapsed_seconds=%.2f", output_count, time.monotonic() - started)
    except Exception:
        logger.exception("FAILED elapsed_seconds=%.2f", time.monotonic() - started)
        raise


main()
