DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_patient_summary_query;

CREATE PROCEDURE sp_fact_line_list_patient_summary_query(IN PATIENT_UUID VARCHAR(50))
BEGIN
   select * from mamba_fact_client where patient_uuid = PATIENT_UUID;
END //

DELIMITER ;
