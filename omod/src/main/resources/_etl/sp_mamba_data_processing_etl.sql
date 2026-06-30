DELIMITER //

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_etl;

CREATE PROCEDURE sp_mamba_data_processing_etl(IN etl_incremental_mode INT)

BEGIN
    -- add base folder SP here if any --

    -- Rename client/location tables to _staging (sub-millisecond gap before CREATE TABLE)
    CALL sp_mamba_drop_all_derived_tables();

    -- Rebuild client and location tables under their live names
    CALL sp_data_processing_derived_client();
    CALL sp_data_processing_derived_location_tag();

    -- Drop client/location staging now that their live tables are fully rebuilt
    DROP TABLE IF EXISTS mamba_dim_client_staging;
    DROP TABLE IF EXISTS mamba_fact_location_attribute_staging;
    DROP TABLE IF EXISTS mamba_fact_location_attribute_type_staging;
    DROP TABLE IF EXISTS mamba_fact_location_tag_staging;
    DROP TABLE IF EXISTS mamba_fact_location_tag_map_staging;

    -- Follow-up uses full blue-green: builds _staging while live table stays available,
    -- then a single atomic RENAME swaps them.
    CALL sp_data_processing_derived_follow_up();

END //

DELIMITER ;