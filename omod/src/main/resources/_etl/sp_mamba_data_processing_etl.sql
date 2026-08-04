DELIMITER //

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_etl;

CREATE PROCEDURE sp_mamba_data_processing_etl(IN etl_incremental_mode INT)

BEGIN
    -- add base folder SP here if any --

    -- Rename location tables to _staging (sub-millisecond gap before CREATE TABLE)
    CALL sp_mamba_drop_all_derived_tables();

    -- Follow-up must run first: fact_client CTEs query mamba_fact_follow_up
    CALL sp_data_processing_derived_follow_up();

    -- Rebuild client tables (dim_client/fact_client use their own blue-green swap)
    -- and location tables under their live names
    CALL sp_data_processing_derived_client();
    CALL sp_data_processing_derived_location_tag();

    -- Drop location staging now that the live tables are fully rebuilt
    DROP TABLE IF EXISTS mamba_fact_location_attribute_staging;
    DROP TABLE IF EXISTS mamba_fact_location_attribute_type_staging;
    DROP TABLE IF EXISTS mamba_fact_location_tag_staging;
    DROP TABLE IF EXISTS mamba_fact_location_tag_map_staging;

END //

DELIMITER ;