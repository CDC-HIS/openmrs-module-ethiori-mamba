DELIMITER //

DROP PROCEDURE IF EXISTS sp_tmp_build_vl_eligibility;

-- Builds a session-scoped temp table with each client's viral load eligibility
-- scenario/status, in one self-contained statement queried directly off
-- mamba_fact_follow_up (indexed on client_id, follow_up_date_followup_ DESC).
-- All internal self-joins (VL sent vs. performed pairing, regimen-switch lookup,
-- latest-follow-up lookup) are resolved inside this single statement via CTEs,
-- since a TEMPORARY table cannot be referenced twice in the same query.
-- p_as_of_date = NULL means "no cutoff" (matches the ETL's full-history behavior);
-- v_end (defaulting to CURDATE() when p_as_of_date is NULL) is the reference point
-- used for all "days since"/"eligible by" date arithmetic, matching the ETL's use
-- of CURDATE() today.
CREATE PROCEDURE sp_tmp_build_vl_eligibility(IN p_as_of_date DATE)
BEGIN
    DECLARE v_end DATE;
    SET v_end = COALESCE(p_as_of_date, CURDATE());

    DROP TEMPORARY TABLE IF EXISTS mamba_temp_vl_eligibility;

    CREATE TEMPORARY TABLE mamba_temp_vl_eligibility AS
    WITH LatestFollowUp AS (SELECT *
                             FROM (SELECT client_id,
                                          follow_up_date_followup_                                                                             AS visit_date,
                                          follow_up_status,
                                          art_antiretroviral_start_date                                                                        AS art_start_date,
                                          pregnancy_status,
                                          currently_breastfeeding_child,
                                          ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS rn
                                   FROM mamba_fact_follow_up
                                   WHERE follow_up_date_followup_ IS NOT NULL
                                     AND (p_as_of_date IS NULL OR follow_up_date_followup_ <= p_as_of_date)) ranked
                             WHERE rn = 1),

         Vl_Events AS (SELECT encounter_id,
                              client_id,
                              follow_up_date_followup_                                                                                                                                           AS FollowupDate,
                              date_of_reported_hiv_viral_load                                                                                                                                    AS viral_load_sent_date,
                              date_viral_load_results_received                                                                                                                                   AS viral_load_perform_date,
                              regimen_change,
                              routine_viral_load_test_indication,
                              viral_load_test_status,
                              hiv_viral_load                                                                                                                                                     AS viral_load_count,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY (date_of_reported_hiv_viral_load IS NOT NULL) DESC, date_of_reported_hiv_viral_load DESC, encounter_id DESC)   AS rn_sent,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY (regimen_change IS NOT NULL) DESC, follow_up_date_followup_ DESC, encounter_id DESC)                           AS rn_switch,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY (date_viral_load_results_received IS NOT NULL) DESC, date_viral_load_results_received DESC, encounter_id DESC) AS rn_perf
                       FROM mamba_fact_follow_up
                       WHERE (p_as_of_date IS NULL OR date_of_reported_hiv_viral_load IS NULL OR date_of_reported_hiv_viral_load <= p_as_of_date)
                         AND (p_as_of_date IS NULL OR date_viral_load_results_received IS NULL OR date_viral_load_results_received <= p_as_of_date)),

         vl_performed AS (SELECT vl_perf.encounter_id,
                                 vl_perf.client_id,
                                 vl_sent.viral_load_sent_date                                            AS VL_Sent_Date,
                                 CASE
                                     WHEN vl_perf.viral_load_perform_date < vl_sent.viral_load_sent_date THEN NULL
                                     ELSE vl_perf.viral_load_perform_date END                            AS viral_load_perform_date,
                                 CASE
                                     WHEN vl_perf.viral_load_perform_date < vl_sent.viral_load_sent_date THEN NULL
                                     ELSE vl_perf.viral_load_test_status END                             AS viral_load_status,
                                 CASE
                                     WHEN vl_perf.viral_load_count >= 0 AND
                                          vl_perf.viral_load_perform_date >= vl_sent.viral_load_sent_date
                                         THEN CAST(vl_perf.viral_load_count AS DECIMAL(12, 2))
                                     ELSE NULL END                                                       AS viral_load_count,
                                 CASE
                                     WHEN vl_perf.viral_load_test_status IS NULL AND
                                          vl_perf.viral_load_perform_date >= vl_sent.viral_load_sent_date THEN NULL
                                     WHEN vl_perf.viral_load_perform_date >= vl_sent.viral_load_sent_date AND
                                          (vl_perf.viral_load_test_status LIKE 'Det%' OR
                                           vl_perf.viral_load_test_status LIKE 'Uns%' OR
                                           vl_perf.viral_load_test_status LIKE 'High VL%' OR
                                           vl_perf.viral_load_test_status LIKE 'Low Level Viremia%') THEN 'U'
                                     WHEN vl_perf.viral_load_perform_date >= vl_sent.viral_load_sent_date AND
                                          (vl_perf.viral_load_test_status LIKE 'Su%' OR
                                           vl_perf.viral_load_test_status LIKE 'Undet%') THEN 'S'
                                     WHEN vl_perf.viral_load_perform_date >= vl_sent.viral_load_sent_date AND
                                          (COALESCE(vl_perf.viral_load_count, 0) > 50) THEN 'U'
                                     WHEN vl_perf.viral_load_perform_date >= vl_sent.viral_load_sent_date AND
                                          (COALESCE(vl_perf.viral_load_count, 0) <= 50) THEN 'S'
                                     ELSE NULL
                                     END                                                                 AS viral_load_status_inferred,
                                 vl_perf.routine_viral_load_test_indication
                          FROM Vl_Events vl_perf
                                   LEFT JOIN Vl_Events vl_sent
                                             ON vl_perf.client_id = vl_sent.client_id AND vl_sent.rn_sent = 1 AND
                                                vl_sent.viral_load_sent_date IS NOT NULL
                          WHERE vl_perf.rn_perf = 1),

         vl_scenario AS (SELECT f_case.client_id,
                                f_case.art_start_date,
                                f_case.visit_date,
                                f_case.follow_up_status,
                                sub_switch_date.FollowupDate AS switchDate,
                                vlperfdate.VL_Sent_Date,
                                vlperfdate.viral_load_perform_date,
                                vlperfdate.viral_load_status_inferred,
                                CASE
                                    WHEN vlperfdate.VL_Sent_Date IS NULL AND f_case.follow_up_status = 'Restart medication'
                                        THEN 'RESTART_NULL'
                                    WHEN vlperfdate.VL_Sent_Date IS NULL AND sub_switch_date.FollowupDate IS NOT NULL
                                        THEN 'SWITCH_NULL'
                                    WHEN vlperfdate.VL_Sent_Date IS NULL AND f_case.pregnancy_status = 'Yes' AND
                                         TIMESTAMPDIFF(DAY, f_case.art_start_date, v_end) > 90 THEN 'PREG_NULL'
                                    WHEN vlperfdate.VL_Sent_Date IS NULL AND
                                         TIMESTAMPDIFF(DAY, f_case.art_start_date, v_end) <= 180 THEN 'INELIGIBLE_NULL'
                                    WHEN vlperfdate.VL_Sent_Date IS NULL AND
                                         TIMESTAMPDIFF(DAY, f_case.art_start_date, v_end) > 180 THEN 'FIRST_NULL'
                                    WHEN vlperfdate.VL_Sent_Date IS NOT NULL AND
                                         vlperfdate.VL_Sent_Date < f_case.visit_date AND
                                         f_case.follow_up_status = 'Restart medication' THEN 'RESTART_NOT_NULL'
                                    WHEN vlperfdate.VL_Sent_Date IS NOT NULL AND
                                         vlperfdate.VL_Sent_Date < sub_switch_date.FollowupDate AND
                                         sub_switch_date.FollowupDate IS NOT NULL THEN 'SWITCH_NOT_NULL'
                                    WHEN vlperfdate.VL_Sent_Date IS NOT NULL AND vlperfdate.viral_load_status_inferred = 'U'
                                        THEN 'UNSUPPRESSED'
                                    WHEN vlperfdate.VL_Sent_Date IS NOT NULL AND (f_case.pregnancy_status = 'Yes' OR
                                                                                  f_case.currently_breastfeeding_child =
                                                                                  'Yes') AND
                                         vlperfdate.routine_viral_load_test_indication IN
                                         ('First viral load test at 6 months or longer post ART',
                                          'Viral load after EAC: repeat viral load where initial viral load greater than 50 and less than 1000 copies per ml',
                                          'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml')
                                        THEN 'PMTCT_EARLY'
                                    WHEN vlperfdate.VL_Sent_Date IS NOT NULL AND (f_case.pregnancy_status = 'Yes' OR
                                                                                  f_case.currently_breastfeeding_child =
                                                                                  'Yes') AND
                                         vlperfdate.routine_viral_load_test_indication IS NOT NULL THEN 'PMTCT_LATE'
                                    WHEN vlperfdate.VL_Sent_Date IS NOT NULL THEN 'ANNUAL'
                                    ELSE 'UNASSIGNED'
                                    END                      AS scenario_type,
                                f_case.pregnancy_status,
                                f_case.currently_breastfeeding_child,
                                vlperfdate.viral_load_count,
                                vlperfdate.viral_load_status
                         FROM LatestFollowUp f_case
                                  LEFT JOIN vl_performed AS vlperfdate ON vlperfdate.client_id = f_case.client_id
                                  LEFT JOIN Vl_Events AS sub_switch_date ON sub_switch_date.client_id = f_case.client_id AND
                                                                            sub_switch_date.rn_switch = 1 AND
                                                                            sub_switch_date.regimen_change IS NOT NULL)

    SELECT client_id,
           art_start_date,
           visit_date,
           follow_up_status,
           VL_Sent_Date,
           viral_load_perform_date,
           viral_load_status_inferred,
           viral_load_count,
           viral_load_status,
           CASE scenario_type
               WHEN 'RESTART_NULL' THEN DATE_ADD(visit_date, INTERVAL 91 DAY)
               WHEN 'SWITCH_NULL' THEN DATE_ADD(switchDate, INTERVAL 181 DAY)
               WHEN 'PREG_NULL' THEN DATE_ADD(art_start_date, INTERVAL 91 DAY)
               WHEN 'INELIGIBLE_NULL' THEN NULL
               WHEN 'FIRST_NULL' THEN DATE_ADD(art_start_date, INTERVAL 181 DAY)
               WHEN 'RESTART_NOT_NULL' THEN DATE_ADD(visit_date, INTERVAL 91 DAY)
               WHEN 'SWITCH_NOT_NULL' THEN DATE_ADD(switchDate, INTERVAL 181 DAY)
               WHEN 'UNSUPPRESSED' THEN DATE_ADD(VL_Sent_Date, INTERVAL 91 DAY)
               WHEN 'PMTCT_EARLY' THEN DATE_ADD(VL_Sent_Date, INTERVAL 91 DAY)
               WHEN 'PMTCT_LATE' THEN DATE_ADD(VL_Sent_Date, INTERVAL 181 DAY)
               WHEN 'ANNUAL' THEN DATE_ADD(VL_Sent_Date, INTERVAL 365 DAY)
               ELSE DATE_ADD(v_end, INTERVAL 100 YEAR)
               END AS eligiblityDate,
           CASE scenario_type
               WHEN 'RESTART_NULL' THEN 'client restarted ART'
               WHEN 'SWITCH_NULL' THEN 'Regimen Change'
               WHEN 'PREG_NULL' THEN 'First VL for Pregnant'
               WHEN 'INELIGIBLE_NULL' THEN 'N/A'
               WHEN 'FIRST_NULL' THEN 'First VL'
               WHEN 'RESTART_NOT_NULL' THEN 'client restarted ART'
               WHEN 'SWITCH_NOT_NULL' THEN 'Regimen Change'
               WHEN 'UNSUPPRESSED' THEN 'Repeat/Confirmatory Viral Load test'
               WHEN 'PMTCT_EARLY' THEN 'Pregnant/Breastfeeding and needs retesting'
               WHEN 'PMTCT_LATE' THEN 'Pregnant/Breastfeeding and needs retesting'
               WHEN 'ANNUAL' THEN 'Annual Viral Load Test'
               ELSE 'Unassigned'
               END AS vl_status_final
    FROM vl_scenario;

    ALTER TABLE mamba_temp_vl_eligibility ADD PRIMARY KEY (client_id);
END //

DELIMITER ;
