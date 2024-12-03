DELIMITER //

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_etl;

CREATE PROCEDURE sp_mamba_data_processing_etl(IN etl_incremental_mode INT)

BEGIN
    -- add base folder SP here if any --

    -- Call the implementer ETL process
    CALL sp_mamba_drop_all_derived_tables();
    CALL sp_data_processing_derived_client();
    CALL sp_data_processing_derived_location_tag();

END //

DELIMITER ;