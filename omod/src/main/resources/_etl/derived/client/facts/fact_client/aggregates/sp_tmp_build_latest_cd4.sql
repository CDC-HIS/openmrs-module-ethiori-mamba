DELIMITER //

DROP PROCEDURE IF EXISTS sp_tmp_build_latest_cd4;

-- Builds a session-scoped temp table with each client's most recent CD4 result,
-- in one indexed pass over mamba_fact_follow_up.
-- p_as_of_date = NULL means "no cutoff" (matches the ETL's full-history behavior).
CREATE PROCEDURE sp_tmp_build_latest_cd4(IN p_as_of_date DATE)
BEGIN
    DROP TEMPORARY TABLE IF EXISTS mamba_temp_latest_cd4;

    CREATE TEMPORARY TABLE mamba_temp_latest_cd4 AS
    SELECT client_id, cd4_count, visitect_cd4_result
    FROM (SELECT client_id,
                 cd4_count,
                 visitect_cd4_result,
                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS rn
          FROM mamba_fact_follow_up
          WHERE (cd4_count IS NOT NULL OR visitect_cd4_result IS NOT NULL)
            AND (p_as_of_date IS NULL OR follow_up_date_followup_ <= p_as_of_date)) ranked
    WHERE rn = 1;

    ALTER TABLE mamba_temp_latest_cd4 ADD PRIMARY KEY (client_id);
END //

DELIMITER ;
