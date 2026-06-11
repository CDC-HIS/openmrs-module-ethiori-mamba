-- Entry point for the follow_up derived domain.
-- Called from sp_mamba_data_processing_etl(etl_incremental_mode).
-- On full rebuild (etl_incremental_mode = 0) the materialized table
-- is dropped first so the wrapper's _create + _insert can rebuild
-- it from scratch. On incremental (etl_incremental_mode = 1) the
-- table is kept and the wrapper's _update performs a delta refresh.

DELIMITER //

DROP PROCEDURE IF EXISTS sp_data_processing_derived_follow_up;

CREATE PROCEDURE sp_data_processing_derived_follow_up(IN etl_incremental_mode INT)
BEGIN
    IF etl_incremental_mode = 0 THEN
        DROP TABLE IF EXISTS mamba_fact_followup;
    END IF;

    CALL sp_fact_hmis_followup(etl_incremental_mode);
END //

DELIMITER ;
