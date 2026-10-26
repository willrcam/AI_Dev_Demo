# Player Master Dataset Data Dictionary

## Status and Scope

This document contains the `player_master` specifications extracted from
[Student-Facing Analytics Dataset Data Dictionary](student_facing_dataset_data_dictionary.md).
Student dataset schema version: `1.6`.

The input file is `player_master.parquet`. It represents player state and the
latest eligible public rating as of a release snapshot. Legacy
`players.parquet` and `player_rating_history.parquet` outputs are superseded by
this combined file.

This dictionary describes the source Parquet contract. It does not define
additional columns or transformations for the Bronze or Silver Delta layers.

## Release and Snapshot Context

A student release package may contain a clean variant and a tainted variant
with intentionally injected data-quality issues. Both variants use the same
table names and column order. Instructor-only injection manifests are excluded.

Each variant has a historical baseline and may have monthly incrementals:

```text
<release_name>_initial_history
<release_name>_snapshot_YYYY_MM
```

The baseline covers the first `initial_history_month_count` monthly batches.
The `_snapshot_YYYY_MM` folders are monthly incrementals, despite their names.
For `player_master`, these are merge-ready deltas relative to the prior
snapshot, not full restatements of the player population.

| Term | Meaning |
| --- | --- |
| `snapshot_month` | Newest monthly batch month visible to the release; carried on every player row. |
| `snapshot_end_exclusive` | First day after `snapshot_month`; temporal filters use `< snapshot_end_exclusive`. |

Export filter: players registered before `snapshot_end_exclusive`, with latest
rating state before `snapshot_end_exclusive`. Incrementals include changed
players and players referenced by new facts or changed memberships.

## Release Metadata

In the full release package, each release folder includes `manifest.json`.
When supplied with the player file, it provides the following relevant metadata:

| Field | Description |
| --- | --- |
| `release_name` | Concrete release folder name. |
| `release_sequence_number` | One-based sequence in the release package. |
| `release_mode` | `baseline` or `monthly_incremental`. |
| `release_type` | `initial_snapshot` or `monthly_incremental`. |
| `load_strategy` | `full_load` for the baseline or `incremental_load` for later releases. |
| `snapshot_month` | Snapshot month used for as-of dimensions. |
| `snapshot_end_exclusive` | First day after `snapshot_month`. |
| `student_dataset_schema_version` | Schema contract version. |
| `parquet_compression` | Compression codec used for the Parquet files. |
| `output_files` | Per-file metadata: table name, file name, file path, row count, columns, schema hash, and checksum. |
| `row_counts` | Per-table row counts; use the `player_master` entry. |
| `ordered_columns` | Published column order by table; use the `player_master` entry. |
| `schema_hashes` | Schema hash by table; use the `player_master` entry. |
| `file_checksums` | File checksum by table; use the `player_master` entry. |
| `build_timestamp` | UTC timestamp when the manifest was written. |
| `validation_status` | Release validation status. |
| `validation_summary` | DuckDB validation summary. |

## General Conventions

- File format is Parquet; column names are lower snake case.
- Types below describe exported Parquet logical types, not source ORM types.
- Date and datetime values are normalized to ISO-8601 strings before writing.
- Integer-valued columns are written as Parquet `INT64`.
- UUID values are exported as plain strings.
- Decimal-valued columns are written as Parquet `DECIMAL`. For non-empty files,
  precision is inferred from the first streamed chunk of observed values; empty
  files fall back to the projection schema derived from source precision/scale.
- Primary keys are stable exported identifiers within a generation run.
- Nullable means the exported Parquet column may contain null values.
- Hidden simulation values, operational timestamps, job metadata, raw seed
  tables, tournament tables, and instructor-only artifacts are excluded.

## External Relationship

`player_master.home_region_id` is a nullable foreign-key reference to
`regions.id` in the same release state. `country_code` is derived from
`regions.country_code`. The regions dataset is outside this document's scope;
validating the foreign-key relationship requires that reference dataset.

## Table Dictionary

### `player_master`

Business description: player dimension with current public rating state as of
the release snapshot.

Grain: one row per exported player.

Source composition: `players` plus the latest eligible row from
`player_rating_history`, with `country_code` derived from the player's home
region.

| Column | Type | Nullable | FK | Description |
| --- | --- | --- | --- | --- |
| `player_id` | INT64 | no | none | Player identifier from source `players.id`. |
| `external_player_key` | STRING (UUID text) | no | none | Stable external player identifier exported as a plain string. |
| `first_name` | STRING | no | none | Player first name. |
| `last_name` | STRING | no | none | Player last name. |
| `gender` | STRING | yes | none | Player gender value. |
| `birth_date` | STRING (ISO date) | no | none | Player birth date. |
| `dominant_hand` | STRING | yes | none | Dominant hand. |
| `home_region_id` | INT64 | yes | `regions.id` | Player home region. |
| `country_code` | STRING | yes | none | Home-region country code resolved from `regions.country_code`. |
| `registration_date` | STRING (ISO date) | no | none | Date the player entered the population. |
| `player_status` | STRING | no | none | Player lifecycle status. |
| `rating` | DECIMAL (scale 3) | yes | none | Compatibility alias of `rating_value` for ranking-oriented consumers. |
| `rating_value` | DECIMAL (scale 3) | yes | none | Latest public rating before `snapshot_end_exclusive`. |
| `confidence_score` | DECIMAL (scale 3) | yes | none | Latest rating confidence score. |
| `volatility_score` | DECIMAL (scale 3) | yes | none | Latest rating volatility measure. |
| `global_percentile` | DECIMAL (scale 2) | yes | none | Latest global percentile ranking. |
| `match_count_used` | INT64 | yes | none | Match count reflected in the latest rating. |
| `rating_date` | STRING (ISO date) | yes | none | Effective date of the latest included rating row. |
| `rating_batch_id` | INT64 | yes | none | Source monthly batch for the latest included rating row. |
| `snapshot_month` | STRING (ISO date) | no | none | Release snapshot month carried on every player row. |

Excluded source columns: source `players.id` is renamed to `player_id`;
`initial_skill_seed`, `generation_run_id`, `created_at`, and `updated_at` are
not exported. `player_rating_history` is not exported as a standalone table.
