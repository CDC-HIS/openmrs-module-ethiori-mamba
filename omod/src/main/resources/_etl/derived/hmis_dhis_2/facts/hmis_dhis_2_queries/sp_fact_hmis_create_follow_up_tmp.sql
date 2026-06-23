-- RETIRED: superseded by sp_fact_follow_up / mamba_fact_follow_up.
-- The canonical materialization is now owned by sp_data_processing_derived_follow_up
-- (called from sp_mamba_data_processing_etl), which runs sp_fact_follow_up() to
-- create and populate mamba_fact_follow_up. All HMIS v2 and DATIM consumers now
-- read from mamba_fact_follow_up directly.
--
-- This file is kept so the DROP below cleanly removes the SP from any database
-- that still has the old definition registered. The CREATE stub delegates to the
-- canonical SP so any existing external CALL sp_fact_hmis_create_follow_up_tmp()
-- continues to work without rebuilding the data a second time.
DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_create_follow_up_tmp;

CREATE PROCEDURE sp_fact_hmis_create_follow_up_tmp()
BEGIN
    CALL sp_fact_follow_up();
END //

DELIMITER ;
