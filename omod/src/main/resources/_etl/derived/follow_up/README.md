# `derived/follow_up/` — Materialized follow-up form

This directory materializes the wide `Follow Up` encounter form (encounter_type_uuid
`136b2ded-22a3-4831-a39a-088d35a50ef5`) into a single denormalized table
`mamba_fact_followup` so HMIS, DATIM, and line-list reports can read the wide row
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
3. The pattern was not reusable for the other 8 encounter forms that suffer
   from the same column-overflow split (see "Onboarding other forms" below).

This folder solves all three: the table is named `mamba_fact_followup` (peer of
`mamba_fact_client` and `mamba_fact_location_*`), the rebuild is a clean
DROP+rebuild on every ETL run, and the layout is replicated mechanically for
the other 8 forms.

## Layout

```
follow_up/
├── README.md                                          this file
├── sp_makefile                                        lists sp_data_processing_derived_follow_up.sql
├── sp_data_processing_derived_follow_up.sql           entry-point called by sp_mamba_data_processing_etl
├── sp_compress_follow_up_flat_tables.sql              post-create compression for the 10 source flat tables
└── facts/
    └── hmis_followup/
        ├── sp_makefile                                lists the 3 SP files below
        ├── sp_fact_hmis_followup.sql                  wrapper: _create + _insert
        ├── sp_fact_hmis_followup_create.sql           CREATE TABLE IF NOT EXISTS mamba_fact_followup ...
        └── sp_fact_hmis_followup_insert.sql           TRUNCATE + 10-way LEFT JOIN INSERT
```

The 3-file shape is a simplification of the `location/` 4-file pattern:
since the materialization is always full-rebuild, there is no separate
`_update.sql` for delta refresh. (The original `_update.sql` was reverted —
see "Full rebuild on every run" below.)

## Full rebuild on every run

The entry-point `sp_data_processing_derived_follow_up` performs a full rebuild
of `mamba_fact_followup` on every ETL run:

1. `DROP TABLE IF EXISTS mamba_fact_followup` — always, regardless of mode
2. `CALL sp_fact_hmis_followup()` — creates the table (with `ROW_FORMAT=COMPRESSED
   KEY_BLOCK_SIZE=8`) and populates it via `TRUNCATE` + 10-way LEFT JOIN INSERT
3. `CALL sp_compress_follow_up_flat_tables()` — re-applies compression to the
   10 source flat tables and the new materialization (idempotent metadata-only
   change on already-compressed tables)

**Why full rebuild instead of incremental:**

- ETL is user-triggered (after data changes), not a high-frequency timer.
- The user expects a known-correct state after each run, with no drift and
  no dependence on a `_mamba_etl_schedule` watermark.
- The performance cost is acceptable: a 10-way JOIN over 5M rows + 10
  idempotent `ALTER TABLE` statements takes ~5–10 seconds on a typical
  8 GB / HDD box. Within the headroom of a 5-minute ETL schedule.
- Compression keeps the disk footprint small and the I/O bounded.

**Behavior change from the previous incremental design:**

- The `sp_fact_hmis_followup_update.sql` (delta refresh) was deleted. It is
  no longer called from any SP.
- The wrapper `sp_fact_hmis_followup.sql` lost its `etl_incremental_mode`
  parameter and the IF/ELSE branch. It always calls `_create` then `_insert`.
- The entry-point `sp_data_processing_derived_follow_up.sql` always does
  `DROP TABLE IF EXISTS mamba_fact_followup` (no mode gating).
- The `etl_incremental_mode` parameter is kept in the entry-point's
  signature for backward compatibility with the call site in
  `sp_mamba_data_processing_etl`, but is intentionally ignored.

## Compression

Both `mamba_fact_followup` and the 10 source flat tables are configured with
`ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8`. This applies to deployments on both
small (8 GB / HDD) and large (powerful) machines — the trade is cheap CPU for
less I/O, which is the right trade on either profile.

### How compression is applied

- **`mamba_fact_followup`**: created with `ROW_FORMAT=COMPRESSED
  KEY_BLOCK_SIZE=8` in `sp_fact_hmis_followup_create.sql`. Idempotent via
  `CREATE TABLE IF NOT EXISTS`.
- **The 10 source flat tables**: the core (separate repo) creates them as
  default `ROW_FORMAT=DYNAMIC` via `sp_mamba_flat_encounter_table_create`.
  We can't change that from this repo, so we re-apply compression via
  `sp_compress_follow_up_flat_tables.sql`, called from the entry-point
  after the materialization runs. The 10 `ALTER TABLE` statements are
  idempotent: on an already-compressed table MySQL 8 performs a
  metadata-only change (no rewrite); on an uncompressed table it rewrites
  the table once.

### Why `KEY_BLOCK_SIZE=8` and not 4 or 16

- `KEY_BLOCK_SIZE=4`: ~70% compression, ~20% CPU, more risk of compression failures on busy pages
- `KEY_BLOCK_SIZE=8`: ~55% compression, ~10% CPU — sweet spot for HDD
- `KEY_BLOCK_SIZE=16`: ~30% compression, ~5% CPU — marginal win over DYNAMIC

### What about existing deployments

This folder assumes a fresh deploy: the next ETL run that hits the
full-rebuild path will produce compressed tables. For an existing
deployment with >5 GB of uncompressed data already on disk, run a
one-time `ALTER TABLE mamba_flat_encounter_follow_up[_1.._9]
ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8` on each of the 10 flat tables
during a maintenance window. The compression cost is ~minutes per
GB on HDD.

## Known limitations (out of scope)

- **Drift correction** — the full-rebuild model makes this moot. Every
  ETL run is a clean rebuild; no drift can accumulate.
- **Other 8 forms** — `pre_exposure_followup`, `ncd_followup`,
  `pre_exposure_screening`, `ict_index_contact_follow_up`, `hei_follow_up`,
  `re_test`, `phrh_followup`, and `hei_hiv_test` all have the same
  column-overflow split and would benefit from a copy of this folder.
  See "Onboarding other forms" below.
- **HMIS v1 → v2 for DATIM** — the 4-5 DATIM `tx_*` indicators still
  inline the 10-way join in their CTEs. Migrating them to read from
  `mamba_fact_followup` (as the HMIS `_v2` SPs do) would deliver
  proportional speedups. Separate PR.

## Onboarding other forms (recipe)

To add a materialization for another form, e.g. `pre_exposure_followup`
(encounter_type_uuid `bc423d48-af6f-4354-af22-fec8ff1c0308`, 2 overflow
tables):

1. Copy this entire folder to `derived/pre_exposure_followup/`.
2. In each of the SQL files, replace:
   - `mamba_fact_followup` → `mamba_fact_pre_exposure_followup`
   - `mamba_flat_encounter_follow_up` → `mamba_flat_encounter_pre_exposure_followup`
   - `sp_fact_hmis_followup` → `sp_fact_hmis_pre_exposure_followup`
   - `hmis_followup` → `hmis_pre_exposure_followup` (sub-folder name)
   - The column list in `_create.sql` and `_insert.sql` (project from
     the base + overflow tables; the existing 10-way join is a template)
3. Add the entry-point line to the top-level `_etl/sp_makefile`:
   ```
   derived/pre_exposure_followup/sp_data_processing_derived_pre_exposure_followup.sql
   ```
4. Add a `CALL` to `_etl/sp_mamba_data_processing_etl.sql`:
   ```sql
   CALL sp_data_processing_derived_pre_exposure_followup(etl_incremental_mode);
   ```
5. (Optional) If reports read this materialization, update their v1 SPs
   to read from `mamba_fact_pre_exposure_followup` (the v2 pattern used
   by HMIS).

The 3-file shape is enforced by convention only — there is no automatic
registration.
