DELIMITER //
-- Needed for now till incremental is full implemented. We will just drop all tables and recreate them
DROP PROCEDURE IF EXISTS sp_mamba_drop_all_derived_tables;
CREATE PROCEDURE sp_mamba_drop_all_derived_tables()
BEGIN

    DROP TABLE IF EXISTS mamba_fact_client;
    DROP TABLE IF EXISTS mamba_dim_client;
    DROP TABLE IF EXISTS mamba_fact_location_attribute;
    DROP TABLE IF EXISTS mamba_fact_location_attribute_type;
    DROP TABLE IF EXISTS mamba_fact_location_tag;
    DROP TABLE IF EXISTS mamba_fact_location_tag_map;
    -- mamba_fact_followup is dropped by sp_data_processing_derived_follow_up
    -- (only on etl_incremental_mode = 0). It must NOT be dropped here on
    -- every ETL run, otherwise the incremental delta refresh is defeated.
END //
DELIMITER ;