# Regions Bronze job

The separate `regions_bronze` job runs `src/regions_bronze.py` as a serverless
notebook task. It has no cluster settings, extra dependencies, or schedule.
The existing player job is unchanged. The root bundle already includes
`resources/*.yml`, so no root configuration change is needed.

## Source and destination

- Source: `/Volumes/ai_dev_class_demo/ai_dev_schema/ai_dev_raw/regions.parquet`
- Destination: managed Delta table `ai_dev_class_demo.bronze_layer.regions`
- Write mode: overwrite, with schema evolution disabled.

The ordered source contract from `regions_data_dictionary.md` is:

| Column | Spark type | Nullable in contract |
| --- | --- | --- |
| id | BIGINT | No |
| country_code | STRING | No |
| region_type | STRING | Yes |
| region_name | STRING | No |
| state_province_code | STRING | Yes |
| population | BIGINT | Yes |
| latitude | DECIMAL(p,6) | Yes |
| longitude | DECIMAL(p,6) | Yes |

Decimal precision is inferred from the source. No casts, added columns,
deduplication, or business-quality corrections are applied. Unexpected types
fail for review; the player job's string exception does not apply to regions.

## Preconditions and reruns

Assume a complete lab snapshot matching the player release. Monthly incremental
files are not accumulated or merged. Keep the source stable during execution;
verification reads it multiple times. Actual volume schema and counts are
established at runtime, not assumed from local sample files.

The catalog and input volume/file must already exist. The runner needs USE
CATALOG, USE SCHEMA, and READ VOLUME for the source; USE SCHEMA and CREATE TABLE
for the destination, and SELECT/MODIFY on an existing table. If the destination
schema is absent, CREATE SCHEMA on the catalog is also required. The code uses
`CREATE SCHEMA IF NOT EXISTS ai_dev_class_demo.bronze_layer`, without dropping
or replacing a namespace. Serverless Jobs must be available in the workspace.

A successful rerun replaces the destination with the source rows instead of
appending. An unchanged input yields the same rows, including duplicate
multiplicity, with a new Delta transaction. An existing destination must be
Delta and match the source column names, order, and types. Only one run of this
job may execute at a time; avoid other writers to the table during verification.

## Checks and logs

Before writing, check the eight column names/order, types, and decimal scale 6,
then count source rows. Required-field nulls and business-quality issues are
preserved for Silver checks rather than rejected or silently corrected.

After writing, verify Delta format, schema, row counts, and bidirectional
`exceptAll` equality. Failures raise an exception and fail the task. A post-write
failure does not automatically roll back the committed overwrite.

Task output logs the source/destination paths, mode, observed schema, source and
output counts, elapsed time, and success or exception details. No player or
region records are printed, and no audit columns or audit tables are added.

## Validate and deploy

Run Spark processing in Databricks. From the repository root:

```powershell
git diff --check
databricks bundle validate -t dev
```

Bundle validation checks configuration; it does not execute ingestion or
establish data access. After deployment approval, use:

```powershell
databricks bundle deploy -t dev
```

The bundle deploy includes both job definitions. Review the deployment changes;
the player definition should remain unchanged. Authentication determines the
workspace. No workspace URL or credentials are added to the bundle.

After execution approval, especially when replacing an existing destination:

```powershell
databricks bundle run -t dev regions_bronze
```

The `dev` target writes the exact destination above, not an isolated copy.
Inspect task logs and verify in Databricks SQL:

```sql
DESCRIBE TABLE ai_dev_class_demo.bronze_layer.regions;
DESCRIBE DETAIL ai_dev_class_demo.bronze_layer.regions;
SELECT COUNT(*) AS row_count FROM ai_dev_class_demo.bronze_layer.regions;
DESCRIBE HISTORY ai_dev_class_demo.bronze_layer.regions LIMIT 5;
```

Repeat with unchanged input to confirm equal counts and successful row equality
checks without duplicate accumulation. Cross-table foreign-key checks belong to
the Silver build using matching player and regions release states.
