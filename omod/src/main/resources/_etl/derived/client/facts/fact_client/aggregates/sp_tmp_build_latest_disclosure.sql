DELIMITER //

DROP PROCEDURE IF EXISTS sp_tmp_build_latest_disclosure;

CREATE PROCEDURE sp_tmp_build_latest_disclosure(IN p_as_of_date DATE)
BEGIN
    DROP TEMPORARY TABLE IF EXISTS mamba_temp_latest_disclosure;

    CREATE TEMPORARY TABLE mamba_temp_latest_disclosure AS
    SELECT client_id, stages_of_disclosure
    FROM (SELECT client_id,
                 stages_of_disclosure,
                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS rn
          FROM mamba_fact_follow_up
          WHERE stages_of_disclosure IS NOT NULL
            AND (p_as_of_date IS NULL OR follow_up_date_followup_ <= p_as_of_date)) ranked
    WHERE rn = 1;

    ALTER TABLE mamba_temp_latest_disclosure ADD PRIMARY KEY (client_id);
END //

DELIMITER ;
