-- Entry point for the follow_up derived domain.
-- Always performs a full rebuild of mamba_fact_follow_up on every
-- ETL run.
--
-- Sequence:
--   1. DROP TABLE mamba_fact_follow_up (always)
--   2. CALL sp_fact_follow_up()  -- creates + TRUNCATE+INSERT
--   3. CALL sp_compress_follow_up_flat_tables()  -- apply compression
--      to mamba_fact_follow_up (via _create) and the 10 source flat
--      tables. Idempotent on already-compressed tables.
--
-- The etl_incremental_mode parameter is accepted but intentionally
-- ignored: the materialization is always a full rebuild.

DELIMITER //

DROP PROCEDURE IF EXISTS sp_data_processing_derived_follow_up;

CREATE PROCEDURE sp_data_processing_derived_follow_up(IN etl_incremental_mode INT)
BEGIN
    DROP TABLE IF EXISTS mamba_fact_follow_up;

    CALL sp_fact_follow_up();

    CALL sp_compress_follow_up_flat_tables();
END //

DELIMITER ;
