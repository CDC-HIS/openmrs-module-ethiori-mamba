DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_patient_summary_query;

CREATE PROCEDURE sp_fact_line_list_patient_summary_query(IN in_patient_uuid VARCHAR(50))
BEGIN
   select * from mamba_fact_client where patient_uuid = in_patient_uuid;
END //

DELIMITER ;
