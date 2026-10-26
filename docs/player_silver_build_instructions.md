# NAPA Tutorial — Player Silver Layer Build Instructions

**Audience:** DSB6000 students using VS Code, Codex, GitHub, and Databricks Free Edition  
**Deliverable:** `ai_dev_class_demo.silver_layer.player_silver`  
**Approach:** Simple, repeatable, full-table build from two existing Bronze Delta tables

## 1. Purpose and scope

Build a player-level Silver data product that combines player information with geographic information about each player's home region. The exercise demonstrates using Codex to develop a straightforward medallion-layer transformation: **read → validate → join/cast → write → verify**.

The tutorial input data is expected to be **clean**. Basic data-quality checks are included to demonstrate good practice, not to build an enterprise exception-handling framework.

**For this tutorial only:** Assume the instructor has loaded matching, **complete current-state** Bronze tables from the same dataset. The source data dictionaries also describe baseline and monthly incremental release files; **do not** attempt to process those incrementals in this exercise. If the supplied Bronze tables contain only monthly change files, stop and ask the instructor for a complete matching pair instead.

### Authoritative source references

- `docs/player_master_data_dictionary.md`
- `docs/regions_data_dictionary.md`

These dictionaries define the available source fields and original meaning. **Do not invent Bronze fields, derived business attributes, or hidden simulation values.** This document defines the intended Silver output and a small set of teaching-oriented processing rules.

## 2. Inputs, output, and relationship

| Role | Delta table |
|---|---|
| Player source | `ai_dev_class_demo.bronze_layer.player_master` |
| Region source | `ai_dev_class_demo.bronze_layer.regions` |
| Silver output | `ai_dev_class_demo.silver_layer.player_silver` |

- **Silver grain:** one row per player in the complete Bronze player snapshot.
- **Unique key:** `player_id`.
- **Join:** `player_master.home_region_id = regions.id`.
- **Join type:** **LEFT OUTER** from players to regions.
- **Expected row count:** equal to the number of Bronze player rows when validations pass.
- A null `home_region_id` is **allowed**. Keep that player, with null region enrichment fields and null `country_code`.
- A **non-null** `home_region_id` that does not match any region is an error.
- `player_master.country_code` is derived from the home region in the source; compare it to matched `regions.country_code`. A disagreement, or a null player country when a region is matched, is an error. Publish the matched region's `country_code` in Silver.
- If `home_region_id` is null but the player has a non-null `country_code`, treat this as an error; do not guess a region.
- Never join on player names or country codes.

## 3. Required Silver output schema

Write **exactly these 26 columns, in this order**, with the listed Databricks/Delta types. `p` denotes `player_master`; `r` denotes `regions`.

| # | Silver column | Type | Source / rule |
|---:|---|---|---|
| 1 | `player_id` | `BIGINT` | `p.player_id` |
| 2 | `external_player_key` | `STRING` | `p.external_player_key` |
| 3 | `first_name` | `STRING` | `p.first_name` |
| 4 | `last_name` | `STRING` | `p.last_name` |
| 5 | `gender` | `STRING` | `p.gender` |
| 6 | `birth_date` | `DATE` | Parse `p.birth_date` |
| 7 | `dominant_hand` | `STRING` | `p.dominant_hand` |
| 8 | `home_region_id` | `BIGINT` | `p.home_region_id` |
| 9 | `country_code` | `STRING` | `r.country_code` for a matching region; otherwise null |
| 10 | `region_type` | `STRING` | `r.region_type` |
| 11 | `region_name` | `STRING` | `r.region_name` |
| 12 | `state_province_code` | `STRING` | `r.state_province_code` |
| 13 | `population` | `BIGINT` | `r.population` |
| 14 | `latitude` | `DECIMAL(18,6)` | `r.latitude` |
| 15 | `longitude` | `DECIMAL(18,6)` | `r.longitude` |
| 16 | `registration_date` | `DATE` | Parse `p.registration_date` |
| 17 | `player_status` | `STRING` | `p.player_status` |
| 18 | `rating` | `DECIMAL(18,3)` | `p.rating` |
| 19 | `rating_value` | `DECIMAL(18,3)` | `p.rating_value` |
| 20 | `confidence_score` | `DECIMAL(18,3)` | `p.confidence_score` |
| 21 | `volatility_score` | `DECIMAL(18,3)` | `p.volatility_score` |
| 22 | `global_percentile` | `DECIMAL(18,2)` | `p.global_percentile` |
| 23 | `match_count_used` | `BIGINT` | `p.match_count_used` |
| 24 | `rating_date` | `DATE` | Parse `p.rating_date` |
| 25 | `rating_batch_id` | `BIGINT` | `p.rating_batch_id` |
| 26 | `snapshot_month` | `DATE` | Parse `p.snapshot_month` |

**Type rules:** Dates are ISO-formatted strings in the original Parquet contract; convert them to real Silver `DATE` values. Preserve nulls where allowed. Cast decimals to the fixed types above without silently losing non-null values or changing their numerical meaning. Use `BIGINT` for the source's `INT64` fields. Keep `rating` and `rating_value` as separate documented columns.

No ingestion timestamps, surrogate keys, quality-status flags, or undocumented attributes belong in `player_silver`.

## 4. Basic validations

Run these checks **before overwriting Silver**. The source data is expected to pass all checks.

### Player checks

1. `player_id` is non-null and unique.
2. Required player fields are non-null: `player_id`, `external_player_key`, `first_name`, `last_name`, `birth_date`, `registration_date`, `player_status`, `snapshot_month`.
3. Required string fields (`external_player_key`, `first_name`, `last_name`, `player_status`) are not blank or whitespace-only.
4. Required dates parse successfully; a populated optional `rating_date` also parses successfully.
5. `birth_date` is not after `registration_date`.
6. `match_count_used`, when present, is non-negative; `global_percentile`, when present, is between 0 and 100.
7. `rating` and `rating_value` agree when present (both null or equal).
8. Every non-null `home_region_id` matches a valid `regions.id`; the player and region country codes agree, including null consistency as described in Section 2.
9. Numeric/date casts do not turn non-null source values into null or lose meaningful precision.

### Region checks

1. `id` is non-null and unique.
2. Required fields (`id`, `country_code`, `region_name`) are non-null; required strings are not blank.
3. `population`, when present, is non-negative.
4. `latitude`, when present, lies between -90 and 90; `longitude`, when present, lies between -180 and 180.
5. Numeric casts do not fail or silently lose meaningful precision.

**Nullable means allowed:** Do not reject null `gender`, `dominant_hand`, `home_region_id`, rating-related optional fields, `region_type`, `state_province_code`, `population`, or coordinates merely because they are null. Do not invent allowed-value lists for gender, player status, country, hand, or region type. Advanced UUID checks, outlier modeling, and speculative business rules are out of scope.

## 5. Simple data-quality issue logs

Create two Delta issue tables in the Silver schema:

- `ai_dev_class_demo.silver_layer.player_master_dq_issues`
- `ai_dev_class_demo.silver_layer.regions_dq_issues`

Use this **same five-column schema** for both:

| Column | Delta type | Meaning |
|---|---|---|
| `run_timestamp` | `TIMESTAMP` | UTC time of this validation run |
| `source_table` | `STRING` | `player_master` or `regions` |
| `record_id` | `STRING` | `player_id` or region `id` rendered as text; null if missing |
| `issue_code` | `STRING` | Short, consistent code such as `NULL_KEY`, `DUPLICATE_KEY`, `INVALID_DATE`, `ORPHAN_REGION` |
| `issue_description` | `STRING` | Readable description including relevant column/value where useful |

Add **one row per detected issue**; a source record may have multiple issues. Region problems belong in the region log. Player-specific problems, including missing region references or player/region country mismatches, belong in the player log. Append findings rather than erasing previous runs. When no issues are found, keep the tables available but append no issue rows.

`record_id` is a **business-key reference, not a unique physical source-row number**. The dictionaries have no row-position column; duplicates may legitimately produce the same `record_id`. Do not fabricate row numbers. Full rejected-row storage and separate quarantine tables are not needed for this tutorial.

**On failure:** Append the detected issues, show counts by source/issue code, then stop with a clear error **without overwriting the existing Silver table**. If validation passes, write the Silver output. There is no rejection tolerance, automated repair, or partial publication.

## 6. Build and rerun behavior

1. Read both complete Bronze tables.
2. Validate both sources and the player-to-region relationship.
3. Ensure the two issue tables exist and append any findings.
4. If any validation fails, report the issues and stop. Leave any previous `player_silver` unchanged.
5. If validation succeeds, LEFT JOIN the sources and select/cast the 26 specified columns.
6. Write `player_silver` as a Delta table using a **full-table overwrite**. Create the `silver_layer` schema if needed and permitted.
7. Print a brief execution summary: Bronze player count, Bronze region count, issue count, and Silver row count.

Running the build again with unchanged, valid Bronze inputs should produce the same logical Silver content, without accumulating duplicate Silver rows. This tutorial does not implement incremental merges, deletion events, release metadata validation, job orchestration, or multi-table atomic transactions.

## 7. Verification / acceptance tests

Codex should include a short SQL verification file or README section demonstrating the checks below. The actual data counts will depend on the supplied dataset; **do not hard-code row counts**.

```sql
-- 1. Every validated Bronze player should appear once in Silver.
SELECT
  (SELECT COUNT(*) FROM ai_dev_class_demo.bronze_layer.player_master) AS bronze_players,
  (SELECT COUNT(*) FROM ai_dev_class_demo.silver_layer.player_silver) AS silver_players;

-- 2. Silver player keys must be unique (expect zero rows).
SELECT player_id, COUNT(*) AS occurrences
FROM ai_dev_class_demo.silver_layer.player_silver
GROUP BY player_id
HAVING COUNT(*) > 1;

-- 3. Every non-null Silver region reference must resolve (expect zero rows).
SELECT s.player_id, s.home_region_id
FROM ai_dev_class_demo.silver_layer.player_silver s
LEFT JOIN ai_dev_class_demo.bronze_layer.regions r
  ON s.home_region_id = r.id
WHERE s.home_region_id IS NOT NULL AND r.id IS NULL;

-- 4. Examine enriched players.
SELECT player_id, first_name, last_name, home_region_id,
       country_code, region_name, state_province_code
FROM ai_dev_class_demo.silver_layer.player_silver
LIMIT 20;

-- 5. Review DQ logs (typically empty for this clean tutorial).
SELECT * FROM ai_dev_class_demo.silver_layer.player_master_dq_issues
ORDER BY run_timestamp DESC LIMIT 20;
SELECT * FROM ai_dev_class_demo.silver_layer.regions_dq_issues
ORDER BY run_timestamp DESC LIMIT 20;

-- 6. Confirm exact Silver column names, order, and types.
DESCRIBE TABLE ai_dev_class_demo.silver_layer.player_silver;
```

**Acceptance criteria:** (a) both inputs read successfully; (b) no critical DQ findings for the supplied clean inputs; (c) Silver exists with the exact 26-column schema above; (d) player counts match; (e) no duplicate `player_id`; (f) region enrichment and nullable home regions behave as specified; (g) a successful rerun replaces, rather than appends to, the Silver data.

## 8. Suggested student Codex prompt

> Read `docs/player_master_data_dictionary.md`, `docs/regions_data_dictionary.md`, and `docs/player_silver_build_instructions.md`. Implement the Silver player pipeline exactly as specified, using simple and readable PySpark and our existing Databricks project conventions. Include the two basic DQ issue tables and SQL verification queries. Do not add features beyond these instructions. First explain the intended code changes; ask before deploying or running against Databricks.

## 9. Intentional limitations

This is a **guided introductory build**, not a production-grade ingestion or DQ framework. It deliberately omits monthly incremental processing, automatic corrections, historical snapshots, detailed source-row provenance, sophisticated quarantine handling, configurable tolerances, transactional coordination across tables, and automated recovery. Those are useful later topics but **not requirements for this tutorial**.
