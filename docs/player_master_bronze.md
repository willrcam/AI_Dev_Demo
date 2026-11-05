# Player master Bronze job

The `player_master_bronze` bundle job runs `src/player_master_bronze.py` as a
serverless notebook task. No cluster configuration or additional packages are
required. Run Spark processing in Databricks, not locally.

## Input and output

- Input: `/Volumes/ai_dev_class_demo/ai_dev_schema/ai_dev_raw/player_master.parquet`
- Output: managed Delta table `ai_dev_class_demo.bronze_layer.player_master`
- Mode: full-table overwrite. An unchanged input produces the same rows on rerun,
  without accumulating duplicates. Delta records a new transaction on each write.
- Contract: the 20 ordered columns in `player_master_data_dictionary.md`.
  Preserve date strings, observed decimal precision, nulls, and duplicate rows.
  No additional columns or business transformations are applied.

Approved source-contract exception: the uploaded lab Parquet was observed to
store `global_percentile` as STRING instead of the dictionary's DECIMAL(scale 2).
The job accepts either type and logs a WARNING when it encounters STRING,
preserving all values without casting. Silver must validate and convert these
strings before publishing its required numeric column. The source dictionary is
unchanged. All other source type checks remain enforced, including scale 2 when
`global_percentile` is a decimal. This exception does not enable destination
schema evolution: an existing decimal destination still requires review before
it can be replaced with a string column.

Assume the supplied file is a complete lab snapshot. Monthly incremental files
are not accumulated or merged by this job. Confirm this with the instructor
before execution if the source release is uncertain. Keep the source file stable
throughout execution: validation reads it more than once.

## Prerequisites

Use a workspace with serverless Jobs support and a current Databricks CLI.
The catalog `ai_dev_class_demo` and the source volume/file must already exist.
The job identity needs USE CATALOG, USE SCHEMA and READ VOLUME for the input;
USE SCHEMA and CREATE TABLE for the output, plus SELECT and MODIFY on an existing
destination table. If `bronze_layer` is absent, CREATE SCHEMA on the catalog is
needed. The code uses CREATE SCHEMA IF NOT EXISTS; it never drops a schema.
The deploying identity also needs permission to deploy workspace files and jobs.

## Validate, deploy, and run

Review the overwrite operation and obtain approval before running against an
existing destination. Deploying the bundle and executing the job are separate
actions. From the repository root in PowerShell:

```powershell
databricks auth login --host https://<your-workspace-host>
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev player_master_bronze
```

Authentication selects the workspace; no credentials or workspace URL are stored
in the bundle. The `dev` target uses the exact requested job and table names;
it does not create an isolated development copy of the table. Only one run of
this job may execute at a time; other writers must also avoid the destination
during ingestion and verification. No automatic schedule is configured.

Bundle validation checks configuration, not data access or Spark execution.
Open the job run's task output to inspect logs. The script logs paths, mode,
observed schema, source count, destination count, elapsed time, and exceptions;
it does not print player records. No separate audit table is created.

Before writing, schema checks reject missing, extra, reordered, or incorrectly
typed fields and incorrect decimal scales. An existing destination must be Delta
and match the source column names, order, and types. Schema evolution is disabled.
Required-field nulls and business-quality issues are preserved for later layers.

After writing, checks verify Delta format, schema, row counts, and bidirectional
EXCEPT ALL equality (including duplicate multiplicity). A failed check fails the
job. Post-write failures do not roll back the already committed Delta write.

Verify in Databricks SQL:

```sql
DESCRIBE TABLE ai_dev_class_demo.bronze_layer.player_master;
DESCRIBE DETAIL ai_dev_class_demo.bronze_layer.player_master;
SELECT COUNT(*) AS row_count
FROM ai_dev_class_demo.bronze_layer.player_master;
DESCRIBE HISTORY ai_dev_class_demo.bronze_layer.player_master LIMIT 5;
```

Run the job again with unchanged input. Both runs should pass row equality checks
and report the same count. Approximately 5,000 players is README context, not a
hard-coded acceptance threshold. Actual counts are established at runtime.
