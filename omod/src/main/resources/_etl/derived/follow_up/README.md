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
`mamba_fact_client` and `mamba_fact_location_*`), the incremental run performs a
**delta refresh** against `mamba_z_encounter_obs`, and the layout is replicated
mechanically for the other 8 forms.

## Layout

```
follow_up/
├── README.md                                          this file
├── sp_makefile                                        lists sp_data_processing_derived_follow_up.sql
├── sp_data_processing_derived_follow_up.sql           entry-point called by sp_mamba_data_processing_etl
└── facts/
    └── hmis_followup/
        ├── sp_makefile                                lists the 4 SP files below
        ├── sp_fact_hmis_followup.sql                  wrapper: dispatches on etl_incremental_mode
        ├── sp_fact_hmis_followup_create.sql           CREATE TABLE IF NOT EXISTS mamba_fact_followup ...
        ├── sp_fact_hmis_followup_insert.sql           full rebuild (TRUNCATE + 10-way LEFT JOIN INSERT)
        └── sp_fact_hmis_followup_update.sql           delta refresh (DELETE + INSERT against delta)
```

This is the same 4-file shape used by `derived/location/facts/*/` (see
`sp_fact_location_tag.sql`, `_create.sql`, `_insert.sql`, `_update.sql`).

## How the incremental delta works

On the **full-rebuild path** (`etl_incremental_mode = 0`):
- The entry-point `sp_data_processing_derived_follow_up` drops `mamba_fact_followup`
  (if it exists) and calls the wrapper with `etl_incremental_mode = 0`.
- The wrapper calls `_create` (idempotent), then `_insert` (TRUNCATE + full INSERT).
- Wall time: O(total rows in the follow-up form).

On the **incremental path** (`etl_incremental_mode = 1`, the default after the
first run):
- The entry-point skips the drop and calls the wrapper with
  `etl_incremental_mode = 1`.
- The wrapper calls `_create` (idempotent — the table already exists), then
  `_update` (delta refresh).
- `_update` uses `_mamba_etl_schedule.start_time` of the last COMPLETED run as
  the watermark. Encounters with rows in `mamba_z_encounter_obs` where
  `date_created >= watermark` OR `date_voided >= watermark` AND
  `encounter_type_uuid = '<follow-up UUID>'` are considered "touched" and
  are re-projected.
- Wall time: O(delta rows) — typically ~50–200 ms for a 5-minute ETL window.

`_mamba_etl_schedule` is the existing logging table populated by the core
ETL scheduler. The watermark query is the same one used elsewhere by the
core for the same purpose — see
`xf_system/etl_incremental_columns_index_new/sp_mamba_etl_incremental_columns_index_new_insert.sql`.

The `incremental_record` flag on `mamba_z_encounter_obs` is **not** used here
because it is reset to 0 by `sp_mamba_reset_incremental_update_flag_all` at
the end of `sp_mamba_data_processing_increment_and_flatten` — that runs
**before** `sp_mamba_data_processing_etl` (i.e. before this folder is
invoked), so the flag is unreliable by the time the materialization
executes.

## Known limitations (out of scope for this PR)

- **Drift correction** — encounters that are deleted from the source `obs`
  table are not detected. The next `_mamba_etl_schedule.start_time`
  watermark may be in the past, but a "delete event" leaves no marker in
  `mamba_z_encounter_obs`. A nightly full-rebuild or a delete-tracking
  table would address this; both are future work.
- **Compression** — `mamba_fact_followup` is `ENGINE=InnoDB` (default
  row format). A separate PR should switch it (and the 10 source flat
  tables) to `ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8` for ~50–70%
  storage and I/O reduction on HDD-backed systems.
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
2. In each of the 5 SQL files, replace:
   - `mamba_fact_followup` → `mamba_fact_pre_exposure_followup`
   - `mamba_flat_encounter_follow_up` → `mamba_flat_encounter_pre_exposure_followup`
   - `sp_fact_hmis_followup` → `sp_fact_hmis_pre_exposure_followup`
   - `hmis_followup` → `hmis_pre_exposure_followup` (sub-folder name)
   - The encounter_type_uuid constant in `_update.sql`
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
5. Add the new table name to `_etl/derived/sp_mamba_drop_all_derived_tables.sql`
   only if you want the **full** path to drop the table. (Note: the
   entry-point already drops it on `etl_incremental_mode = 0`, so this is
   a no-op safety net and is optional.)
6. (Optional) If reports read this materialization, update their v1 SPs
   to read from `mamba_fact_pre_exposure_followup` (the v2 pattern used
   by HMIS).

The 4-file shape is enforced by convention only — there is no automatic
registration.
