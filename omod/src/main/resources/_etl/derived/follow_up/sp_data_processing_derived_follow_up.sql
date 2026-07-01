-- Entry point for the follow_up derived domain.
-- Uses a blue-green (atomic swap) strategy so the live table stays
-- available with old data throughout the rebuild:
--
--   1. Clean up any leftover staging table from a prior failed run.
--   2. Build mamba_fact_follow_up_staging (create + full INSERT).
--   3. Atomic RENAME: swap staging → live, archive old → _old.
--   4. Drop the archived _old table.
--
-- Reports querying mamba_fact_follow_up see old data until step 3,
-- then immediately see new data — zero "table not found" windows.

DELIMITER //

DROP PROCEDURE IF EXISTS sp_data_processing_derived_follow_up;

CREATE PROCEDURE sp_data_processing_derived_follow_up()
BEGIN
    -- Remove any leftover staging from a previously aborted ETL run
    DROP TABLE IF EXISTS mamba_fact_follow_up_staging;

    -- Populate staging while the live table remains fully accessible
    CALL sp_fact_follow_up_create();
    CALL sp_fact_follow_up_insert();

    -- Guard against a pre-existing _old table left by a previously crashed ETL run
    DROP TABLE IF EXISTS mamba_fact_follow_up_old;

    -- Atomic swap: old data stays live until this single RENAME completes
    IF (SELECT COUNT(*) FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mamba_fact_follow_up') > 0 THEN
        RENAME TABLE mamba_fact_follow_up         TO mamba_fact_follow_up_old,
                     mamba_fact_follow_up_staging TO mamba_fact_follow_up;
        DROP TABLE IF EXISTS mamba_fact_follow_up_old;
    ELSE
        RENAME TABLE mamba_fact_follow_up_staging TO mamba_fact_follow_up;
    END IF;

END //

DELIMITER ;
