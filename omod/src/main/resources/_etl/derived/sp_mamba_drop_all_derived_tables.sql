DELIMITER //
-- Needed for now till incremental is full implemented. We will just drop all tables and recreate them
DROP PROCEDURE IF EXISTS sp_mamba_drop_all_derived_tables;
CREATE PROCEDURE sp_mamba_drop_all_derived_tables()
BEGIN
    DROP TABLE IF EXISTS mamba_fact_tx_new;
    DROP TABLE IF EXISTS mamba_fact_tx_curr;
    DROP TABLE IF EXISTS mamba_fact_art_latest_follow_up;
    DROP TABLE IF EXISTS mamba_fact_art_first_follow_up;
    DROP TABLE IF EXISTS mamba_fact_art_follow_up;
    DROP TABLE IF EXISTS mamba_fact_client_tranfer_in;
    DROP TABLE IF EXISTS mamba_fact_client_tranfer_out;
END //
DELIMITER ;