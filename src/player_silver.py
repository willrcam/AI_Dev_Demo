# Databricks notebook source
"""Validate complete Bronze inputs, then publish the specified Silver snapshot."""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from functools import reduce

from pyspark.sql import SparkSession, functions as F


PLAYER_TABLE = "ai_dev_class_demo.bronze_layer.player_master"
REGION_TABLE = "ai_dev_class_demo.bronze_layer.regions"
SILVER_SCHEMA = "ai_dev_class_demo.silver_layer"
OUTPUT_TABLE = f"{SILVER_SCHEMA}.player_silver"
ISSUE_SCHEMA = (
    "run_timestamp TIMESTAMP, source_table STRING, record_id STRING, "
    "issue_code STRING, issue_description STRING"
)

# Output order and types are exactly those in the Silver build instructions.
OUTPUT_COLUMNS = [
    ("player_id", "p", "bigint"),
    ("external_player_key", "p", "string"),
    ("first_name", "p", "string"),
    ("last_name", "p", "string"),
    ("gender", "p", "string"),
    ("birth_date", "p", "date"),
    ("dominant_hand", "p", "string"),
    ("home_region_id", "p", "bigint"),
    ("country_code", "r", "string"),
    ("region_type", "r", "string"),
    ("region_name", "r", "string"),
    ("state_province_code", "r", "string"),
    ("population", "r", "bigint"),
    ("latitude", "r", "decimal(18,6)"),
    ("longitude", "r", "decimal(18,6)"),
    ("registration_date", "p", "date"),
    ("player_status", "p", "string"),
    ("rating", "p", "decimal(18,3)"),
    ("rating_value", "p", "decimal(18,3)"),
    ("confidence_score", "p", "decimal(18,3)"),
    ("volatility_score", "p", "decimal(18,3)"),
    ("global_percentile", "p", "decimal(18,2)"),
    ("match_count_used", "p", "bigint"),
    ("rating_date", "p", "date"),
    ("rating_batch_id", "p", "bigint"),
    ("snapshot_month", "p", "date"),
]


@F.udf("boolean")
def numeric_value_changed(original, converted):
    """Compare exactly, including numeric strings; never compare through float."""
    if original is None:
        return False
    if converted is None:
        return True
    try:
        before, after = Decimal(original), Decimal(converted)
        return not before.is_finite() or not after.is_finite() or before != after
    except (InvalidOperation, ValueError):
        return True


def converted(name, data_type):
    return F.expr(f"try_cast(`{name}` AS {data_type})")


def main():
    spark = SparkSession.builder.getOrCreate()
    run_timestamp = datetime.now(timezone.utc)
    players = spark.table(PLAYER_TABLE)
    regions = spark.table(REGION_TABLE)
    player_count, region_count = players.count(), regions.count()
    print(f"Bronze players={player_count}, Bronze regions={region_count}")
    findings = {"player_master": [], "regions": []}

    def issue(frame, source, key, condition, code, description):
        findings[source].append(frame.where(condition).select(
            F.lit(run_timestamp).cast("timestamp").alias("run_timestamp"),
            F.lit(source).alias("source_table"),
            F.col(key).cast("string").alias("record_id"),
            F.lit(code).alias("issue_code"),
            F.lit(description).alias("issue_description"),
        ))

    # Required values and duplicate keys: keep one finding per affected record.
    for frame, source, key, required, strings in [
        (players, "player_master", "player_id",
         ["player_id", "external_player_key", "first_name", "last_name",
          "birth_date", "registration_date", "player_status", "snapshot_month"],
         ["external_player_key", "first_name", "last_name", "player_status"]),
        (regions, "regions", "id", ["id", "country_code", "region_name"],
         ["country_code", "region_name"]),
    ]:
        for name in required:
            issue(frame, source, key, F.col(name).isNull(),
                  "NULL_KEY" if name == key else "NULL_REQUIRED", f"{name} is required")
        for name in strings:
            issue(frame, source, key, F.col(name).rlike(r"^\s*$"),
                  "BLANK_REQUIRED", f"{name} is blank or whitespace-only")
        duplicate_keys = frame.groupBy(key).count().where(
            F.col(key).isNotNull() & (F.col("count") > 1)
        ).select(key)
        duplicates = frame.join(duplicate_keys, key, "left_semi")
        issue(duplicates, source, key, F.lit(True), "DUPLICATE_KEY", f"{key} is not unique")

    # Validate every numeric/date conversion before publishing. Helper expressions
    # are never added to the output. Optional null values remain allowed.
    for source, frame, alias, key in [
        ("player_master", players, "p", "player_id"),
        ("regions", regions, "r", "id"),
    ]:
        casts = {name: dtype for name, owner, dtype in OUTPUT_COLUMNS
                 if owner == alias and dtype != "string"}
        if source == "regions":
            casts["id"] = "bigint"
        for name, dtype in casts.items():
            raw, cast_value = F.col(name), converted(name, dtype)
            if dtype == "date":
                invalid = raw.isNotNull() & (
                    cast_value.isNull() | ~raw.cast("string").rlike(r"^\d{4}-\d{2}-\d{2}$")
                )
                code = "INVALID_DATE"
            else:
                invalid = numeric_value_changed(raw.cast("string"), cast_value.cast("string"))
                code = "INVALID_NUMERIC_CAST"
            issue(frame, source, key, invalid, code,
                  f"{name} cannot convert to {dtype} without invalid data or meaningful precision loss")

    issue(players, "player_master", "player_id",
          converted("birth_date", "date") > converted("registration_date", "date"),
          "DATE_ORDER", "birth_date is after registration_date")
    issue(players, "player_master", "player_id", F.col("match_count_used") < 0,
          "NEGATIVE_COUNT", "match_count_used is negative")
    percentile = converted("global_percentile", "decimal(38,18)")
    issue(players, "player_master", "player_id", (percentile < 0) | (percentile > 100),
          "OUT_OF_RANGE", "global_percentile is outside [0, 100]")
    issue(players, "player_master", "player_id",
          ~F.col("rating").eqNullSafe(F.col("rating_value")),
          "RATING_MISMATCH", "rating and rating_value must both be null or numerically equal")
    issue(regions, "regions", "id", F.col("population") < 0,
          "NEGATIVE_COUNT", "population is negative")
    for name, bound in [("latitude", 90), ("longitude", 180)]:
        value = F.col(name)
        issue(regions, "regions", "id", (value < -bound) | (value > bound),
              "OUT_OF_RANGE", f"{name} is outside [-{bound}, {bound}]")

    # Distinct reference pairs prevent duplicate identical regions from multiplying
    # player findings. Duplicate regions are independently logged and block output.
    reference = regions.select("id", "country_code").distinct().alias("r")
    relationships = players.alias("p").join(
        reference, F.col("p.home_region_id") == F.col("r.id"), "left"
    )
    issue(relationships, "player_master", "p.player_id",
          F.col("p.home_region_id").isNotNull() & F.col("r.id").isNull(),
          "ORPHAN_REGION", "Non-null home_region_id does not match regions.id")
    issue(relationships, "player_master", "p.player_id",
          (F.col("r.id").isNotNull() &
           ~F.col("p.country_code").eqNullSafe(F.col("r.country_code"))) |
          (F.col("p.home_region_id").isNull() & F.col("p.country_code").isNotNull()),
          "COUNTRY_MISMATCH", "Player country_code is inconsistent with its home region")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
    issue_count = 0
    for source in ["player_master", "regions"]:
        table = f"{SILVER_SCHEMA}.{source}_dq_issues"
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({ISSUE_SCHEMA}) USING DELTA")
        issues = reduce(lambda left, right: left.unionByName(right), findings[source])
        count = issues.count()
        issue_count += count
        if count:
            issues.write.format("delta").mode("append").saveAsTable(table)
            issues.groupBy("source_table", "issue_code").count().orderBy("issue_code").show(
                truncate=False
            )
    if issue_count:
        print(f"Bronze players={player_count}, Bronze regions={region_count}, "
              f"issues={issue_count}, Silver not overwritten")
        raise ValueError(f"Validation failed with {issue_count} issues; inspect Silver DQ issue tables")

    joined = players.alias("p").join(
        regions.alias("r"), F.col("p.home_region_id") == F.col("r.id"), "left"
    )
    output = joined.select(*[
        F.col(f"{owner}.{name}").cast(dtype).alias(name)
        for name, owner, dtype in OUTPUT_COLUMNS
    ])
    expected_schema = [(name, dtype) for name, _, dtype in OUTPUT_COLUMNS]
    if [(field.name, field.dataType.simpleString()) for field in output.schema] != expected_schema:
        raise ValueError("Silver output does not have the exact required schema")
    if output.count() != player_count or output.select("player_id").distinct().count() != player_count:
        raise ValueError("Silver candidate count or player-key uniqueness check failed; not overwritten")

    output.write.format("delta").mode("overwrite").option("mergeSchema", "false").option(
        "overwriteSchema", "false"
    ).saveAsTable(OUTPUT_TABLE)
    published = spark.table(OUTPUT_TABLE)
    silver_count = published.count()
    if [(field.name, field.dataType.simpleString()) for field in published.schema] != expected_schema:
        raise ValueError("Published Silver schema verification failed")
    if silver_count != player_count or published.select("player_id").distinct().count() != player_count:
        raise ValueError("Published Silver count or key verification failed")
    print(f"SUCCESS Bronze players={player_count}, Bronze regions={region_count}, "
          f"issues={issue_count}, Silver rows={silver_count}")


main()
