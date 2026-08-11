DELIMITER //

DROP PROCEDURE IF EXISTS sp_tmp_build_family_planning;

CREATE PROCEDURE sp_tmp_build_family_planning(IN p_as_of_date DATE)
BEGIN
    DROP TEMPORARY TABLE IF EXISTS mamba_temp_family_planning;

    CREATE TEMPORARY TABLE mamba_temp_family_planning AS
    SELECT client_id, method_of_family_planning
    FROM (SELECT client_id,
                 method_of_family_planning,
                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS rn
          FROM mamba_fact_follow_up
          WHERE method_of_family_planning IS NOT NULL
            AND method_of_family_planning != ''
            AND method_of_family_planning != '-'
            AND (p_as_of_date IS NULL OR follow_up_date_followup_ <= p_as_of_date)) ranked
    WHERE rn = 1;

    ALTER TABLE mamba_temp_family_planning ADD PRIMARY KEY (client_id);
END //

DELIMITER ;
