-- Post-create compression for the 10 follow-up source flat tables.
--
-- The core (mamba-core-api, in a separate repo) creates these tables with
-- the default ROW_FORMAT=DYNAMIC via sp_mamba_flat_encounter_table_create.
-- We can't change that from this repo, so we re-apply compression here
-- AFTER the core ETL has (re)built the tables and BEFORE the follow-up
-- materialization reads from them.
--
-- The 10 ALTERs are idempotent: on an already-compressed table MySQL 8
-- performs a metadata-only change (no rewrite). On an uncompressed table
-- (first run, or after a full-rebuild) MySQL rewrites the table with
-- compression. On the incremental path the core's
-- sp_mamba_flat_table_incremental_insert_all updates rows in-place
-- without touching row format, so the tables stay compressed.
--
-- Why KEY_BLOCK_SIZE=8:
--   * ~55% on-disk size reduction
--   * ~10% CPU overhead for (de)compression (negligible on modern x86)
--   * Sweet spot for HDD-backed MySQL 8 deployments
--   * Same buffer pool RAM usage; compressed pages take 8 KB on disk
--     but the 16 KB buffer pool slots are populated from decompressed pages

DELIMITER //

DROP PROCEDURE IF EXISTS sp_compress_follow_up_flat_tables;

CREATE PROCEDURE sp_compress_follow_up_flat_tables()
BEGIN
    ALTER TABLE mamba_flat_encounter_follow_up   ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
    ALTER TABLE mamba_flat_encounter_follow_up_1 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
    ALTER TABLE mamba_flat_encounter_follow_up_2 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
    ALTER TABLE mamba_flat_encounter_follow_up_3 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
    ALTER TABLE mamba_flat_encounter_follow_up_4 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
    ALTER TABLE mamba_flat_encounter_follow_up_5 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
    ALTER TABLE mamba_flat_encounter_follow_up_6 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
    ALTER TABLE mamba_flat_encounter_follow_up_7 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
    ALTER TABLE mamba_flat_encounter_follow_up_8 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
    ALTER TABLE mamba_flat_encounter_follow_up_9 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;

    ANALYZE TABLE mamba_flat_encounter_follow_up,
                mamba_flat_encounter_follow_up_1,
                mamba_flat_encounter_follow_up_2,
                mamba_flat_encounter_follow_up_3,
                mamba_flat_encounter_follow_up_4,
                mamba_flat_encounter_follow_up_5,
                mamba_flat_encounter_follow_up_6,
                mamba_flat_encounter_follow_up_7,
                mamba_flat_encounter_follow_up_8,
                mamba_flat_encounter_follow_up_9;
END //

DELIMITER ;
