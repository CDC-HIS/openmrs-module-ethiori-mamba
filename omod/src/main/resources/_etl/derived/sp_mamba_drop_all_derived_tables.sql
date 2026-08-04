DELIMITER //
-- Rename location derived tables to _staging before rebuild.
-- This frees the live names for the immediate CREATE TABLE calls in the
-- processing SPs, reducing "table not found" to a sub-millisecond window.
-- The _staging tables are dropped by sp_mamba_data_processing_etl after
-- all processing SPs complete.
-- mamba_fact_follow_up and mamba_dim_client are excluded here: they are each
-- rebuilt via a full blue-green atomic swap (build into _staging while the
-- live table stays untouched, then a single RENAME swap) inside their own
-- processing SPs, so the live table is never absent even if a later step fails.
DROP PROCEDURE IF EXISTS sp_mamba_drop_all_derived_tables;
CREATE PROCEDURE sp_mamba_drop_all_derived_tables()
BEGIN
    -- Drop leftover staging tables from any previous failed ETL run
    DROP TABLE IF EXISTS mamba_fact_location_attribute_staging;
    DROP TABLE IF EXISTS mamba_fact_location_attribute_type_staging;
    DROP TABLE IF EXISTS mamba_fact_location_tag_staging;
    DROP TABLE IF EXISTS mamba_fact_location_tag_map_staging;

    -- Rename live tables to staging (atomic DDL, frees name for fresh CREATE TABLE)
    IF (SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mamba_fact_location_attribute') > 0 THEN
        RENAME TABLE mamba_fact_location_attribute TO mamba_fact_location_attribute_staging;
    END IF;
    IF (SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mamba_fact_location_attribute_type') > 0 THEN
        RENAME TABLE mamba_fact_location_attribute_type TO mamba_fact_location_attribute_type_staging;
    END IF;
    IF (SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mamba_fact_location_tag') > 0 THEN
        RENAME TABLE mamba_fact_location_tag TO mamba_fact_location_tag_staging;
    END IF;
    IF (SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mamba_fact_location_tag_map') > 0 THEN
        RENAME TABLE mamba_fact_location_tag_map TO mamba_fact_location_tag_map_staging;
    END IF;
END //
DELIMITER ;