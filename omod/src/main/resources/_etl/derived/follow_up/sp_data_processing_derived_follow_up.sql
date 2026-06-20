-- Entry point for the follow_up derived domain.
-- Always performs a full rebuild of mamba_fact_followup on every
-- ETL run. The previous incremental/delta-refresh optimization
-- was reverted in favor of full rebuild: simpler, no drift, no
-- reliance on the _mamba_etl_schedule watermark, and matches the
-- user-triggered ETL model where the operator expects a known-
-- correct state after each run.
--
-- Sequence:
--   1. DROP TABLE mamba_fact_followup (always)
--   2. CALL sp_fact_hmis_followup()  -- creates + TRUNCATE+INSERT
--   3. CALL sp_compress_follow_up_flat_tables()  -- apply compression
--      to mamba_fact_followup (via _create) and the 10 source flat
--      tables. Idempotent on already-compressed tables.
--
-- The etl_incremental_mode parameter is kept in the signature for
-- backward compatibility with the call site in
-- sp_mamba_data_processing_etl, but is intentionally ignored.

DELIMITER //

DROP PROCEDURE IF EXISTS sp_data_processing_derived_follow_up;

CREATE PROCEDURE sp_data_processing_derived_follow_up(IN etl_incremental_mode INT)
BEGIN
    DROP TABLE IF EXISTS mamba_fact_followup;

    CALL sp_fact_hmis_followup();

    CALL sp_compress_follow_up_flat_tables();
END //

DELIMITER ;
