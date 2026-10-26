# Regions Dataset Data Dictionary

## Status and Scope

This document contains the `regions` specifications extracted from the
Student-Facing Analytics Dataset Data Dictionary, schema version `1.6`.

The input file is `regions.parquet`. This dictionary describes its source
Parquet contract, without additional Bronze or Silver columns or transformations.

## Release and Export Scope

Regions are included when referenced by exported players, clubs, registrations,
or matches. The historical baseline contains the regions referenced by its
included data. Monthly incrementals contain newly referenced or changed regions;
they are merge-ready deltas relative to the prior snapshot, not full restatements
of the regions dimension.

Clean and tainted release variants use the same table name and column order.
Tainted variants may contain intentionally injected data-quality issues.

The release folder's `manifest.json` identifies the schema version and load
strategy. Its `regions` entries provide row counts, ordered columns, schema
hashes, and file checksums.

## General Conventions

- File format is Parquet; column names are lower snake case.
- Types below describe exported Parquet logical types, not source ORM types.
- Integer-valued columns are written as Parquet `INT64`.
- Decimal-valued columns are written as Parquet `DECIMAL`. For non-empty files,
  precision is inferred from the first streamed chunk of observed values; empty
  files fall back to the projection schema derived from source precision/scale.
- Primary keys are stable exported identifiers within a generation run.
- Nullable means the exported Parquet column may contain null values.

## Table Dictionary

### `regions`

Business description: geographic markets used for player homes, clubs,
registrations, and matches.

Grain: one row per referenced region.

Source table: `regions`.

| Column | Type | Nullable | FK | Description |
| --- | --- | --- | --- | --- |
| `id` | INT64 | no | none | Region identifier. |
| `country_code` | STRING | no | none | Country code such as `US` or `CA`. |
| `region_type` | STRING | yes | none | Region classification. |
| `region_name` | STRING | no | none | Human-readable region name. |
| `state_province_code` | STRING | yes | none | State or province code when applicable. |
| `population` | INT64 | yes | none | Approximate market population. |
| `latitude` | DECIMAL (scale 6) | yes | none | Latitude. |
| `longitude` | DECIMAL (scale 6) | yes | none | Longitude. |

Excluded source columns: `selection_probability`,
`competitiveness_multiplier`, `created_at`, `updated_at`.

## Referencing Relationships

These foreign keys reference `regions.id` in the same release state.

| Child table | Child column | Nullable |
| --- | --- | --- |
| `clubs` | `region_id` | no |
| `matches` | `region_id` | yes |
| `player_master` | `home_region_id` | yes |
| `player_registrations` | `assigned_region_id` | yes |

`player_master.country_code` is derived from the home region's
`regions.country_code`. See the [Player Master Data Dictionary](player_master_data_dictionary.md)
for the player contract.
