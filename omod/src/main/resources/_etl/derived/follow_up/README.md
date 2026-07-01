# `derived/follow_up/` — Materialized follow-up form

This directory materializes the wide `Follow Up` encounter form (encounter_type_uuid
`136b2ded-22a3-4831-a39a-088d35a50ef5`) into a single denormalized table
`mamba_fact_follow_up` so HMIS, DATIM, and line-list reports can read the wide row
without re-running a 10-way `LEFT JOIN` over the column-overflow split flat tables
(`mamba_flat_encounter_follow_up`, `_1` … `_9`).

## Why a separate folder

Before this folder existed, the materialization was implemented as a single
`CREATE TABLE tmp_hmis_follow_up AS SELECT ...` inside
`derived/hmis_dhis_2/facts/hmis_dhis_2_queries/sp_fact_hmis_create_follow_up_tmp.sql`.
That arrangement had three problems:

1. The table name `tmp_hmis_follow_up` was misleading — it is a permanent table,
   not a temporary one.
2. The materialization was **rebuilt in full on every incremental ETL run**,
   even when only a handful of encounters had changed.
3. The pattern was not reusable for the other encounter forms that suffer
   from the same column-overflow split.

This folder solves all three: the table is named `mamba_fact_follow_up` (peer of
`mamba_fact_client` and `mamba_fact_location_*`), the rebuild is a clean
blue-green (staging + atomic rename) rebuild on every ETL run, and the layout
is replicated mechanically for the other 8 forms.

## Layout

```
follow_up/
├── README.md                                          this file
├── sp_makefile                                        lists sp_data_processing_derived_follow_up.sql
├── sp_data_processing_derived_follow_up.sql           entry-point called by sp_mamba_data_processing_etl
└── facts/
    └── follow_up/
        ├── sp_makefile                                lists the 3 SP files below
        ├── sp_fact_follow_up.sql                  wrapper: _create + _insert
        ├── sp_fact_follow_up_create.sql           CREATE TABLE IF NOT EXISTS mamba_fact_follow_up ...
        └── sp_fact_follow_up_insert.sql           TRUNCATE + 10-way LEFT JOIN INSERT
```

The 3-file shape is a simplification of the `location/` 4-file pattern:
since the materialization is always full-rebuild, there is no separate
`_update.sql` for delta refresh. (The original `_update.sql` was reverted —
see "Full rebuild on every run" below.)

## Full rebuild on every run (blue-green)

The entry-point `sp_data_processing_derived_follow_up` performs a full rebuild
of `mamba_fact_follow_up` on every ETL run, using a blue-green (staging +
atomic swap) strategy so the live table stays available with old data for
the whole rebuild:

1. `DROP TABLE IF EXISTS mamba_fact_follow_up_staging` — clean up any leftover
   staging table from a previously aborted run.
2. `CALL sp_fact_follow_up()` — creates `mamba_fact_follow_up_staging` (with
   `ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8`) and populates it via `TRUNCATE`
   + 10-way LEFT JOIN INSERT. The live `mamba_fact_follow_up` is untouched
   and fully queryable throughout this step.
3. `DROP TABLE IF EXISTS mamba_fact_follow_up_old` — guard against a
   pre-existing `_old` table left by a previously crashed run.
4. Atomic `RENAME TABLE mamba_fact_follow_up TO mamba_fact_follow_up_old,
   mamba_fact_follow_up_staging TO mamba_fact_follow_up` — readers see either
   the fully old or fully new table, never a partial rebuild. The archived
   `_old` table is dropped immediately after.

**Why full rebuild instead of incremental:**

- ETL is user-triggered (after data changes), not a high-frequency timer.
- The user expects a known-correct state after each run, with no drift and
  no dependence on a `_mamba_etl_schedule` watermark.
- The performance cost is acceptable: a 10-way JOIN over 5M rows takes
  ~5–10 seconds on a typical 8 GB / HDD box. Within the headroom of a
  5-minute ETL schedule.
- Blue-green keeps the live table queryable throughout, instead of dropping
  it up front and leaving a "table not found" gap for the duration of the
  rebuild.

**Behavior change from the previous incremental design:**

- The `sp_fact_follow_up_update.sql` (delta refresh) was deleted. It is
  no longer called from any SP.
- The wrapper `sp_fact_follow_up.sql` lost its `etl_incremental_mode`
  parameter and the IF/ELSE branch. It always calls `_create` then `_insert`,
  building into `mamba_fact_follow_up_staging`.
- The entry-point `sp_data_processing_derived_follow_up.sql` no longer drops
  the live table up front — it drops leftover staging/`_old` tables, builds
  staging, then does the atomic rename swap described above.
- The `etl_incremental_mode` parameter is kept in the entry-point's
  signature for backward compatibility with the call site in
  `sp_mamba_data_processing_etl`, but is intentionally ignored.

## Compression

`mamba_fact_follow_up` (built as `mamba_fact_follow_up_staging`, then renamed
into place) is created with `ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8` in
`sp_fact_follow_up_create.sql`, idempotent via `CREATE TABLE IF NOT EXISTS`.
The trade is cheap CPU for less I/O on both small (8 GB / HDD) and large
deployments.

The 10 source flat tables (`mamba_flat_encounter_follow_up`, `_1`…`_9`) are
**not** compressed by this repo. They're created by the core
(`sp_mamba_flat_encounter_table_create`, separate repo) with the default
`ROW_FORMAT=DYNAMIC`, and we can't change that from here. An earlier version
of this folder re-applied compression to those 10 tables via
`sp_compress_follow_up_flat_tables.sql`, called from the entry-point after
the materialization ran — that SP has been removed; only the materialized
fact table itself is compressed now.

### Why `KEY_BLOCK_SIZE=8` and not 4 or 16

- `KEY_BLOCK_SIZE=4`: ~70% compression, ~20% CPU, more risk of compression failures on busy pages
- `KEY_BLOCK_SIZE=8`: ~55% compression, ~10% CPU — sweet spot for HDD
- `KEY_BLOCK_SIZE=16`: ~30% compression, ~5% CPU — marginal win over DYNAMIC

## Known limitations (out of scope)

- **Drift correction** — the full-rebuild model makes this moot. Every
  ETL run is a clean rebuild; no drift can accumulate.
- **HMIS v1 → v2 for DATIM** — ~~DONE~~. All 10 DATIM `sp_dim_*` SPs were
  migrated from inline 10-way JOINs to reading from `mamba_fact_follow_up`
  as part of the naming standardization (same branch).
