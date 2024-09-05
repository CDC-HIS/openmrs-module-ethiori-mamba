DELIMITER //

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_etl;

CREATE PROCEDURE sp_mamba_data_processing_etl(IN etl_incremental_mode INT)

-- Add the implementation-specific ETL processes here
CALL sp_mamba_drop_all_derived_tables();
CALL sp_data_processing_derived_art_follow_up();
CALL sp_data_processing_derived_transfer_in();
CALL sp_data_processing_derived_transfer_out();
-- $END