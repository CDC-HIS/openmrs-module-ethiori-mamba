DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_followup;

CREATE PROCEDURE sp_fact_hmis_followup(IN etl_incremental_mode INT)
BEGIN
    CALL sp_fact_hmis_followup_create();

    IF etl_incremental_mode = 0 THEN
        CALL sp_fact_hmis_followup_insert();
    ELSE
        CALL sp_fact_hmis_followup_update();
    END IF;
END //

DELIMITER ;
