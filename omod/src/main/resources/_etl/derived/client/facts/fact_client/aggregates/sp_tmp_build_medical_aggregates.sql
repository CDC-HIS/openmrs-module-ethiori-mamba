DELIMITER //

DROP PROCEDURE IF EXISTS sp_tmp_build_medical_aggregates;

-- Builds a session-scoped temp table with per-client TPT/TB/transfer-in aggregate
-- dates, in one indexed pass over mamba_fact_follow_up.
-- p_as_of_date = NULL means "no cutoff" (matches the ETL's full-history behavior).
CREATE PROCEDURE sp_tmp_build_medical_aggregates(IN p_as_of_date DATE)
BEGIN
    DROP TEMPORARY TABLE IF EXISTS mamba_temp_medical_aggregates;

    CREATE TEMPORARY TABLE mamba_temp_medical_aggregates AS
    SELECT client_id,
           MIN(CASE
                   WHEN transferred_in_check_this_for_all_t IN ('Yes', 'True', '1') OR
                        follow_up_status IN ('Transfer in', 'Transfer In', 'TI')
                       THEN follow_up_date_followup_ END) AS transfer_in_date,
           MIN(art_antiretroviral_start_date)             AS art_start_date,
           MAX(date_started_on_tuberculosis_prophy)       AS tpt_start_date,
           MAX(date_completed_tuberculosis_prophyl)       AS tpt_completed_date,
           MAX(date_discontinued_tuberculosis_prop)       AS tpt_discontinued_date,
           MAX(CASE
                   WHEN reason_not_eligible_for_tuberculosi = 'Contraindication' THEN 1
                   ELSE 0 END)                             AS tpt_is_contraindicated,
           MAX(diagnosis_date)                             AS active_tb_diagnosis_date,
           MAX(tuberculosis_drug_treatment_start_d)        AS tb_treatment_start_date,
           MAX(date_active_tbrx_dc)                        AS tb_treatment_discontinued_date,
           MAX(date_active_tbrx_completed)                 AS tb_treatment_completed_date
    FROM mamba_fact_follow_up
    WHERE (p_as_of_date IS NULL OR follow_up_date_followup_ <= p_as_of_date)
    GROUP BY client_id;

    ALTER TABLE mamba_temp_medical_aggregates ADD PRIMARY KEY (client_id);
END //

DELIMITER ;
