DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_dqi_query;

CREATE PROCEDURE sp_fact_line_list_dqi_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    DECLARE v_end DATE;
    SET v_end = COALESCE(REPORT_END_DATE, CURDATE());

    WITH FollowUp AS (SELECT client_id,
                             encounter_id,
                             follow_up_date_followup_,
                             next_visit_date,
                             current_who_hiv_stage,
                             follow_up_status,
                             regimen,
                             antiretroviral_art_dispensed_dose_i,
                             treatment_end_date,
                             nutritional_status_of_adult,
                             nutritional_status_of_older_child_a,
                             mid_upper_arm_circumference,
                             hiv_viral_load                                                 AS viral_load_count,
                             pregnancy_status,
                             currently_breastfeeding_child,
                             method_of_family_planning,
                             dsd_category,
                             weight_text_                                                   AS weight,
                             date_of_event,
                             transferred_in_check_this_for_all_t                           AS transfer_in,
                             art_antiretroviral_start_date                                  AS art_start_date,
                             date_of_reported_hiv_viral_load,
                             date_viral_load_results_received,
                             cervical_cancer_screening_status                               AS cx_ca_screening_status,
                             cervical_cancer_screening_method_strategy                      AS screening_method,
                             via_screening_result,
                             hpv_dna_screening_result,
                             cytology_result,
                             date_visual_inspection_of_the_cervi                            AS via_date,
                             hpv_dna_result_received_date                                   AS hpv_received_date,
                             date_cytology_result_received                                  AS cytology_received_date,
                             eligible_for_cxca_screening,
                             reason_for_not_being_eligible,
                             other_reason_for_not_being_eligible_for_cxca,
                             treatment_start_date                                           AS ccs_treat_received_date,
                             colposcopy_of_cervix_findings                                  AS colposcopy_exam_finding,
                             biopsy_result_received_date,
                             biopsy_result,
                             date_patient_referred_out,
                             next_follow_up_screening_date,
                             routine_viral_load_test_indication,
                             hiv_viral_load,
                             viral_load_test_status,
                             date_started_on_tuberculosis_prophy,
                             date_completed_tuberculosis_prophyl,
                             date_discontinued_tuberculosis_prop,
                             reason_not_eligible_for_tuberculosi,
                             diagnosis_date,
                             tuberculosis_drug_treatment_start_d,
                             regimen_change,
                             date_active_tbrx_completed,
                             date_active_tbrx_dc
                      FROM mamba_fact_follow_up
                      WHERE follow_up_date_followup_ <= v_end),

         LatestFollowUp AS (SELECT *
                            FROM (SELECT client_id,
                                         follow_up_date_followup_                                                                             AS visit_date,
                                         next_visit_date,
                                         follow_up_status,
                                         regimen,
                                         antiretroviral_art_dispensed_dose_i                                                                  AS regimen_dose,
                                         treatment_end_date,
                                         nutritional_status_of_adult,
                                         nutritional_status_of_older_child_a,
                                         mid_upper_arm_circumference,
                                         pregnancy_status,
                                         art_start_date,
                                         current_who_hiv_stage                                                                                AS who_stage,
                                         currently_breastfeeding_child,
                                         weight,
                                         method_of_family_planning,
                                         dsd_category,
                                         date_of_event                                                                                        AS hiv_confirmed_date,
                                         transfer_in,
                                         eligible_for_cxca_screening,
                                         reason_for_not_being_eligible,
                                         other_reason_for_not_being_eligible_for_cxca,
                                         cx_ca_screening_status                                                                               AS screening_status,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS rn
                                  FROM FollowUp
                                  WHERE follow_up_date_followup_ IS NOT NULL) ranked
                            WHERE rn = 1),

         Medical_Aggregates AS (SELECT client_id,
                                       MIN(CASE
                                               WHEN transfer_in IN ('Yes', 'True', '1') OR
                                                    follow_up_status IN ('Transfer in', 'Transfer In', 'TI')
                                                   THEN follow_up_date_followup_ END) AS transfer_in_date,
                                       MIN(art_start_date)                            AS art_start_date,
                                       MAX(date_started_on_tuberculosis_prophy)       AS tpt_start_date,
                                       MAX(date_completed_tuberculosis_prophyl)       AS tpt_completed_date,
                                       MAX(date_discontinued_tuberculosis_prop)       AS tpt_discontinued_date,
                                       MAX(CASE
                                               WHEN reason_not_eligible_for_tuberculosi = 'Contraindication' THEN 1
                                               ELSE 0 END)                            AS tpt_is_contraindicated,
                                       MAX(diagnosis_date)                            AS active_tb_diagnosis_date,
                                       MAX(tuberculosis_drug_treatment_start_d)       AS tb_treatment_start_date,
                                       MAX(date_active_tbrx_dc)                       AS tb_treatment_discontinued_date,
                                       MAX(date_active_tbrx_completed)                AS tb_treatment_completed_date
                                FROM FollowUp
                                GROUP BY client_id),

         Vl_Events AS (SELECT encounter_id,
                              client_id,
                              follow_up_date_followup_,
                              date_of_reported_hiv_viral_load                                                                                                                                   AS viral_load_sent_date,
                              date_viral_load_results_received                                                                                                                                  AS viral_load_perform_date,
                              regimen_change,
                              routine_viral_load_test_indication,
                              follow_up_date_followup_                                                                                                                                          AS FollowupDate,
                              currently_breastfeeding_child                                                                                                                                     AS breastfeeding_status,
                              viral_load_test_status,
                              viral_load_count,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY (date_of_reported_hiv_viral_load IS NOT NULL) DESC, date_of_reported_hiv_viral_load DESC, encounter_id DESC)   AS rn_sent,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY (regimen_change IS NOT NULL) DESC, follow_up_date_followup_ DESC, encounter_id DESC)                           AS rn_switch,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY (date_viral_load_results_received IS NOT NULL) DESC, date_viral_load_results_received DESC, encounter_id DESC) AS rn_perf
                       FROM FollowUp
                       -- Gate VL dates explicitly to v_end (follow-up date and VL sent/received may differ)
                       WHERE (date_of_reported_hiv_viral_load IS NULL OR date_of_reported_hiv_viral_load <= v_end)
                         AND (date_viral_load_results_received IS NULL OR date_viral_load_results_received <= v_end)),

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
                                 COALESCE(vl_sent.viral_load_sent_date,
                                          vl_perf.viral_load_perform_date)                               AS viral_load_ref_date,
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
                                sub_switch_date.FollowupDate                                             AS switchDate,
                                vlperfdate.VL_Sent_Date,
                                vlperfdate.viral_load_perform_date,
                                vlperfdate.viral_load_status_inferred,
                                CASE
                                    WHEN vlperfdate.VL_Sent_Date IS NULL AND
                                         f_case.follow_up_status = 'Restart medication'
                                        THEN 'RESTART_NULL'
                                    WHEN vlperfdate.VL_Sent_Date IS NULL AND
                                         sub_switch_date.FollowupDate IS NOT NULL
                                        THEN 'SWITCH_NULL'
                                    WHEN vlperfdate.VL_Sent_Date IS NULL AND
                                         f_case.pregnancy_status = 'Yes' AND
                                         TIMESTAMPDIFF(DAY, f_case.art_start_date, v_end) > 90
                                        THEN 'PREG_NULL'
                                    WHEN vlperfdate.VL_Sent_Date IS NULL AND
                                         TIMESTAMPDIFF(DAY, f_case.art_start_date, v_end) <= 180
                                        THEN 'INELIGIBLE_NULL'
                                    WHEN vlperfdate.VL_Sent_Date IS NULL AND
                                         TIMESTAMPDIFF(DAY, f_case.art_start_date, v_end) > 180
                                        THEN 'FIRST_NULL'
                                    WHEN vlperfdate.VL_Sent_Date IS NOT NULL AND
                                         vlperfdate.VL_Sent_Date < f_case.visit_date AND
                                         f_case.follow_up_status = 'Restart medication'
                                        THEN 'RESTART_NOT_NULL'
                                    WHEN vlperfdate.VL_Sent_Date IS NOT NULL AND
                                         vlperfdate.VL_Sent_Date < sub_switch_date.FollowupDate AND
                                         sub_switch_date.FollowupDate IS NOT NULL
                                        THEN 'SWITCH_NOT_NULL'
                                    WHEN vlperfdate.VL_Sent_Date IS NOT NULL AND
                                         vlperfdate.viral_load_status_inferred = 'U'
                                        THEN 'UNSUPPRESSED'
                                    WHEN vlperfdate.VL_Sent_Date IS NOT NULL AND
                                         (f_case.pregnancy_status = 'Yes' OR
                                          f_case.currently_breastfeeding_child = 'Yes') AND
                                         vlperfdate.routine_viral_load_test_indication IN
                                         ('First viral load test at 6 months or longer post ART',
                                          'Viral load after EAC: repeat viral load where initial viral load greater than 50 and less than 1000 copies per ml',
                                          'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml')
                                        THEN 'PMTCT_EARLY'
                                    WHEN vlperfdate.VL_Sent_Date IS NOT NULL AND
                                         (f_case.pregnancy_status = 'Yes' OR
                                          f_case.currently_breastfeeding_child = 'Yes') AND
                                         vlperfdate.routine_viral_load_test_indication IS NOT NULL
                                        THEN 'PMTCT_LATE'
                                    WHEN vlperfdate.VL_Sent_Date IS NOT NULL
                                        THEN 'ANNUAL'
                                    ELSE 'UNASSIGNED'
                                    END                                                                  AS scenario_type,
                                f_case.pregnancy_status,
                                f_case.currently_breastfeeding_child,
                                f_case.hiv_confirmed_date,
                                f_case.weight,
                                f_case.regimen_dose,
                                f_case.regimen,
                                f_case.next_visit_date,
                                f_case.treatment_end_date,
                                vlperfdate.viral_load_count,
                                vlperfdate.viral_load_status
                         FROM LatestFollowUp f_case
                                  LEFT JOIN vl_performed AS vlperfdate ON vlperfdate.client_id = f_case.client_id
                                  LEFT JOIN Vl_Events AS sub_switch_date
                                            ON sub_switch_date.client_id = f_case.client_id AND
                                               sub_switch_date.rn_switch = 1 AND
                                               sub_switch_date.regimen_change IS NOT NULL),

         vl_eligibility AS (SELECT *,
                                   CASE scenario_type
                                       WHEN 'RESTART_NULL'    THEN DATE_ADD(visit_date, INTERVAL 91 DAY)
                                       WHEN 'SWITCH_NULL'     THEN DATE_ADD(switchDate, INTERVAL 181 DAY)
                                       WHEN 'PREG_NULL'       THEN DATE_ADD(art_start_date, INTERVAL 91 DAY)
                                       WHEN 'INELIGIBLE_NULL' THEN NULL
                                       WHEN 'FIRST_NULL'      THEN DATE_ADD(art_start_date, INTERVAL 181 DAY)
                                       WHEN 'RESTART_NOT_NULL' THEN DATE_ADD(visit_date, INTERVAL 91 DAY)
                                       WHEN 'SWITCH_NOT_NULL' THEN DATE_ADD(switchDate, INTERVAL 181 DAY)
                                       WHEN 'UNSUPPRESSED'    THEN DATE_ADD(VL_Sent_Date, INTERVAL 91 DAY)
                                       WHEN 'PMTCT_EARLY'     THEN DATE_ADD(VL_Sent_Date, INTERVAL 91 DAY)
                                       WHEN 'PMTCT_LATE'      THEN DATE_ADD(VL_Sent_Date, INTERVAL 181 DAY)
                                       WHEN 'ANNUAL'          THEN DATE_ADD(VL_Sent_Date, INTERVAL 365 DAY)
                                       -- UNASSIGNED: push eligibility far into the future (not eligible)
                                       ELSE DATE_ADD(v_end, INTERVAL 100 YEAR)
                                       END AS eligiblityDate,
                                   CASE scenario_type
                                       WHEN 'RESTART_NULL'    THEN 'client restarted ART'
                                       WHEN 'SWITCH_NULL'     THEN 'Regimen Change'
                                       WHEN 'PREG_NULL'       THEN 'First VL for Pregnant'
                                       WHEN 'INELIGIBLE_NULL' THEN 'N/A'
                                       WHEN 'FIRST_NULL'      THEN 'First VL'
                                       WHEN 'RESTART_NOT_NULL' THEN 'client restarted ART'
                                       WHEN 'SWITCH_NOT_NULL' THEN 'Regimen Change'
                                       WHEN 'UNSUPPRESSED'    THEN 'Repeat/Confirmatory Viral Load test'
                                       WHEN 'PMTCT_EARLY'     THEN 'Pregnant/Breastfeeding and needs retesting'
                                       WHEN 'PMTCT_LATE'      THEN 'Pregnant/Breastfeeding and needs retesting'
                                       WHEN 'ANNUAL'          THEN 'Annual Viral Load Test'
                                       ELSE 'Unassigned'
                                       END AS vl_status_final
                            FROM vl_scenario),

         Identifiers AS (SELECT patient_id,
                                MAX(CASE WHEN pit.name = 'PHRH' THEN pi.identifier END) AS phrh_code,
                                MAX(CASE WHEN pit.name = 'NCD'  THEN pi.identifier END) AS ncd_code,
                                MAX(CASE WHEN pit.name = 'ICD'  THEN pi.identifier END) AS icd_number
                         FROM mamba_dim_patient_identifier pi
                                  JOIN mamba_dim_patient_identifier_type pit
                                       ON pi.identifier_type = pit.patient_identifier_type_id
                         WHERE pit.name IN ('PHRH', 'NCD', 'ICD')
                         GROUP BY patient_id),

         ICT_Events AS (SELECT *
                        FROM (SELECT client_id,
                                     CASE
                                         WHEN accepted = 'Yes' THEN 'Accepted'
                                         WHEN accepted = 'No'  THEN 'Refused'
                                         ELSE 'Offered' END                                                AS ict_status,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY offered_date DESC) AS rn
                              FROM mamba_flat_encounter_ict_offer
                              -- ICT offers on or before v_end
                              WHERE offered_date IS NOT NULL AND offered_date <= v_end) ranked
                        WHERE rn = 1),

         ICT_General_Events AS (SELECT *
                                FROM (SELECT client_id,
                                             ict_serial_number                                                                              AS ict_number,
                                             ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY encounter_datetime DESC, encounter_id DESC) AS rn
                                      FROM mamba_flat_encounter_ict_general
                                      WHERE ict_serial_number IS NOT NULL
                                        AND ict_serial_number != '') ranked
                                WHERE rn = 1),

         NCD_Screening_Events AS (SELECT *
                                  FROM (SELECT client_id,
                                               baseline_diagnosis,
                                               screening_date,
                                               CASE
                                                   WHEN baseline_diagnosis LIKE '%Hypertension%'
                                                       AND baseline_diagnosis LIKE '%Diabetes%'            THEN 'DM_HTN'
                                                   WHEN baseline_diagnosis LIKE '%Hypertension%'           THEN 'HTN'
                                                   WHEN baseline_diagnosis LIKE '%Diabetes Mellitus%'      THEN 'DM'
                                                   WHEN baseline_diagnosis LIKE '%Prediabetes%'
                                                       OR baseline_diagnosis LIKE '%Pre-diabetes%'         THEN 'PREDIABETES'
                                                   WHEN baseline_diagnosis LIKE '%Normal BG%'
                                                       OR baseline_diagnosis LIKE '%Normal Blood Glucose%' THEN 'NORMAL_BG'
                                                   WHEN baseline_diagnosis LIKE '%Normal BP%'
                                                       OR baseline_diagnosis LIKE '%Normal Blood Pressure%' THEN 'NORMAL_BP'
                                                   ELSE NULL
                                                   END                                                     AS ncd_class,
                                               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY screening_date DESC) AS rn
                                        FROM mamba_flat_encounter_ncd_screening
                                        -- NCD screenings on or before v_end
                                        WHERE screening_date IS NOT NULL AND screening_date <= v_end) ranked
                                  WHERE rn = 1),

         NCD_FollowUp_Events AS (SELECT *
                                 FROM (SELECT f.client_id,
                                              f1.return_visit_date                                                                           AS next_screening_date,
                                              f1.systolic_blood_pressure,
                                              f1.diastolic_blood_pressure,
                                              ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY follow_up_date DESC, f.encounter_id DESC) AS rn
                                       FROM mamba_flat_encounter_ncd_followup f
                                                LEFT JOIN mamba_flat_encounter_ncd_followup_1 f1
                                                          ON f.encounter_id = f1.encounter_id
                                       WHERE (f1.return_visit_date IS NOT NULL
                                           OR f1.systolic_blood_pressure IS NOT NULL
                                           OR f1.diastolic_blood_pressure IS NOT NULL)
                                         -- NCD follow-ups on or before v_end
                                         AND f.follow_up_date <= v_end) ranked
                                 WHERE rn = 1),

         CXCA_PrevScreening AS (SELECT *
                                FROM (SELECT client_id,
                                             follow_up_date_followup_                                                                             AS follow_up_date,
                                             via_screening_result                                                                                 AS ccs_via_result,
                                             via_date,
                                             ccs_treat_received_date,
                                             date_patient_referred_out,
                                             biopsy_result,
                                             hpv_dna_screening_result                                                                             AS ccs_hpv_result,
                                             hpv_received_date                                                                                    AS hpv_dna_result_received_date,
                                             cytology_result,
                                             cytology_received_date                                                                               AS cytology_result_date,
                                             colposcopy_exam_finding,
                                             biopsy_result_received_date,
                                             next_follow_up_screening_date,
                                             ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS rn
                                      FROM FollowUp
                                      -- FollowUp already gated to v_end; screening-performed filter added here
                                      WHERE cx_ca_screening_status = 'Cervical cancer screening performed') ranked
                                WHERE rn = 1),

         CXCA_Eligibility_Base AS (SELECT lf.client_id,
                                          lf.visit_date,
                                          lf.follow_up_status                                     AS final_follow_up_status,
                                          lf.eligible_for_cxca_screening,
                                          lf.reason_for_not_being_eligible,
                                          lf.other_reason_for_not_being_eligible_for_cxca,
                                          lf.screening_status,
                                          lf.art_start_date,
                                          ps.follow_up_date                                       AS previous_screening_follow_up_date,
                                          ps.ccs_via_result,
                                          ps.via_date,
                                          ps.ccs_treat_received_date,
                                          ps.date_patient_referred_out,
                                          ps.biopsy_result,
                                          ps.ccs_hpv_result,
                                          ps.hpv_dna_result_received_date,
                                          ps.cytology_result,
                                          ps.cytology_result_date,
                                          ps.colposcopy_exam_finding,
                                          ps.biopsy_result_received_date,
                                          ps.next_follow_up_screening_date,
                                          c.sex,
                                          -- Age at v_end, matching providers_view pattern
                                          TIMESTAMPDIFF(YEAR, c.date_of_birth, v_end)            AS Age,
                                          CASE
                                              WHEN ps.client_id IS NULL AND lf.eligible_for_cxca_screening = 'No'
                                                  THEN CONCAT('Not Eligible ',
                                                              COALESCE(lf.reason_for_not_being_eligible,
                                                                       lf.other_reason_for_not_being_eligible_for_cxca))
                                              WHEN (ps.biopsy_result = 'Carcinoma in situ' OR
                                                    ps.biopsy_result = 'Invasive cervical cancer' OR
                                                    ps.biopsy_result = 'Other')
                                                   AND (ps.ccs_treat_received_date <= v_end OR
                                                        ps.date_patient_referred_out <= v_end)
                                                  THEN 'Not Eligible Confirmed Cirvical Cancer'
                                              WHEN ps.client_id IS NULL AND
                                                   lf.screening_status != 'Cervical cancer screening performed' AND
                                                   lf.eligible_for_cxca_screening = 'Yes'
                                                  THEN 'Eligible Labeled by User'
                                              WHEN ps.client_id IS NULL AND lf.eligible_for_cxca_screening IS NULL
                                                  THEN 'Eligible Never Screened/Assessed'
                                              WHEN ps.ccs_via_result = 'Unknown'
                                                  THEN 'Eligible Unknown VIA Screening Result'
                                              WHEN TIMESTAMPDIFF(DAY, ps.via_date, v_end) > 730 AND
                                                   ps.ccs_via_result = 'VIA negative'
                                                  THEN 'Eligible Needs Re-Screening'
                                              WHEN ps.ccs_via_result = 'VIA positive: eligible for cryo/thermo-coagula'
                                                  THEN CASE
                                                           WHEN ((TIMESTAMPDIFF(DAY, ps.ccs_treat_received_date, v_end)) > 365 OR
                                                                 (TIMESTAMPDIFF(DAY, ps.date_patient_referred_out, v_end)) > 365) AND
                                                                ps.biopsy_result IS NULL
                                                               THEN 'Eligible Post Treatment/Referral Follow-Up'
                                                           WHEN ps.ccs_treat_received_date IS NULL AND
                                                                ps.date_patient_referred_out IS NULL
                                                               THEN 'Eligible Treatment Not Received or Not Referred'
                                                           ELSE 'Not Eligible (Screening Up-to-Date)'
                                                       END
                                              WHEN (ps.ccs_via_result = 'VIA positive: non-eligible for cryo/thermo-coagula' OR
                                                    ps.ccs_via_result = 'suspected cervical cancer') AND
                                                   ps.biopsy_result IS NULL
                                                  THEN 'Eligible Biopsy Test Not Done'
                                              WHEN ps.ccs_hpv_result = 'Negative result' AND
                                                   TIMESTAMPDIFF(DAY, ps.hpv_dna_result_received_date, v_end) > 1095
                                                  THEN 'Eligible Needs Re-Screening'
                                              WHEN ps.ccs_via_result IS NULL AND ps.ccs_hpv_result = 'Positive'
                                                  THEN 'Eligible Needs VIA Triage'
                                              WHEN ps.cytology_result = 'Negative result' AND
                                                   TIMESTAMPDIFF(DAY, ps.cytology_result_date, v_end) > 1095
                                                  THEN 'Eligible Needs Re-Screening'
                                              WHEN (ps.cytology_result = 'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear' OR
                                                    ps.cytology_result = '> Ascus')
                                                  THEN CASE
                                                           WHEN ps.hpv_dna_result_received_date IS NULL
                                                               THEN 'Eligible Needs HPV Triage'
                                                           WHEN ps.ccs_hpv_result = 'Negative result' AND
                                                                TIMESTAMPDIFF(DAY, ps.hpv_dna_result_received_date, v_end) > 730
                                                               THEN 'Eligible Needs Re-Screening'
                                                           WHEN ps.ccs_hpv_result = 'Positive' AND
                                                                ps.colposcopy_exam_finding IS NULL
                                                               THEN 'Eligible Needs Colposcopy Test'
                                                           ELSE 'Not Eligible (Screening Up-to-Date)'
                                                       END
                                              WHEN (ps.biopsy_result = 'CIN (1-3)' OR ps.biopsy_result = 'CIN-2') AND
                                                   TIMESTAMPDIFF(DAY, ps.biopsy_result_received_date, v_end) > 365
                                                  THEN 'Eligible Needs Re-Screening'
                                              ELSE 'Not Eligible (Screening Up-to-Date)'
                                              END                                                  AS EligibilityStatus
                                   FROM LatestFollowUp lf
                                            LEFT JOIN CXCA_PrevScreening ps ON lf.client_id = ps.client_id
                                            JOIN mamba_dim_client c ON lf.client_id = c.client_id),

         CXCA_Status AS (SELECT base.*,
                                CASE
                                    WHEN base.sex = 'Male' OR Age < 25 OR Age > 65 OR
                                         base.final_follow_up_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away')
                                        THEN 'Not Applicable (Blue)'
                                    WHEN base.art_start_date IS NULL
                                        THEN 'ART Not Started (Black)'
                                    WHEN base.EligibilityStatus = 'Not Eligible Confirmed Cirvical Cancer'
                                        THEN 'Confirmed CXCA (RED)'
                                    WHEN base.EligibilityStatus = 'Not Eligible (Screening Up-to-Date)'
                                        THEN 'Previously Screened (Green)'
                                    WHEN base.EligibilityStatus LIKE 'Eligible%'
                                        THEN 'Eligible (Yellow)'
                                    WHEN base.EligibilityStatus LIKE 'Not Eligible%'
                                        THEN 'Not Eligible (White)'
                                    ELSE 'Unknown Status'
                                    END AS cxca_screening_status
                         FROM CXCA_Eligibility_Base AS base),

         PMTCT_CTE AS (SELECT client_id,
                              MAX(date_of_enrollment_or_booking) AS pmtct_start_date
                       FROM mamba_flat_encounter_pmtct_enrollment
                       WHERE date_of_enrollment_or_booking IS NOT NULL
                         AND date_of_enrollment_or_booking <= v_end
                       GROUP BY client_id),

         PMTCT_Discharge_CTE AS (SELECT client_id,
                                        MAX(discharge_date) AS pmtct_discharge_date
                                 FROM mamba_flat_encounter_pmtct_discharge
                                 WHERE discharge_date IS NOT NULL
                                   AND discharge_date <= v_end
                                 GROUP BY client_id),

         Registration AS (SELECT client_id,
                                 MIN(registration_date) AS registration_date
                          FROM mamba_flat_encounter_registration
                          WHERE registration_date IS NOT NULL
                          GROUP BY client_id),

         PHRH_Target_Population AS (SELECT *
                                    FROM (SELECT client_id,
                                                 target_population,
                                                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY followup_date DESC, encounter_id DESC) AS rn
                                          FROM mamba_flat_encounter_phrh_followup
                                          WHERE target_population IS NOT NULL
                                            AND target_population != ''
                                            AND followup_date <= v_end) ranked
                                    WHERE rn = 1),

         FamilyPlanning AS (SELECT *
                            FROM (SELECT client_id,
                                         method_of_family_planning,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS rn
                                  FROM FollowUp
                                  WHERE method_of_family_planning IS NOT NULL
                                    AND method_of_family_planning != ''
                                    AND method_of_family_planning != '-') ranked
                            WHERE rn = 1),

         Latest_CD4 AS (SELECT *
                        FROM (SELECT client_id,
                                     cd4_count,
                                     visitect_cd4_result,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS rn
                              FROM mamba_fact_follow_up
                              WHERE (cd4_count IS NOT NULL OR visitect_cd4_result IS NOT NULL)
                                AND follow_up_date_followup_ <= v_end) ranked
                        WHERE rn = 1),

         Latest_Disclosure AS (SELECT *
                               FROM (SELECT client_id,
                                            stages_of_disclosure,
                                            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS rn
                                     FROM mamba_fact_follow_up
                                     WHERE stages_of_disclosure IS NOT NULL
                                       AND follow_up_date_followup_ <= v_end) ranked
                               WHERE rn = 1),

         Latest_TPT_Contraindication AS (SELECT *
                                         FROM (SELECT client_id,
                                                      reason_not_eligible_for_tuberculosi,
                                                      ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS rn
                                               FROM mamba_fact_follow_up
                                               WHERE reason_not_eligible_for_tuberculosi IS NOT NULL
                                                 AND follow_up_date_followup_ <= v_end) ranked
                                         WHERE rn = 1)

    SELECT c.client_id,
           c.patient_uuid,
           COALESCE(c.mrn, '-')                                                                                         AS mrn,
           COALESCE(c.patient_name, '-')                                                                                AS patient_name,
           c.sex,
           c.date_of_birth                                                                                              AS birthdate,
           TIMESTAMPDIFF(YEAR, c.date_of_birth, v_end)                                                                 AS age,

           COALESCE(c.uan, '-')                                                                                         AS uan,
           COALESCE(id.phrh_code, '-')                                                                                  AS phrh_code,
           COALESCE(id.ncd_code, '-')                                                                                   AS ncd_code,
           COALESCE(id.icd_number, '-')                                                                                 AS icd_number,
           COALESCE(ict_gen.ict_number, '-')                                                                            AS ict_number,

           reg.registration_date,
           ma.art_start_date,
           TIMESTAMPDIFF(MONTH, ma.art_start_date, v_end)                                                              AS months_on_art,
           lfu.next_visit_date                                                                                          AS next_appointment_date,
           lfu.hiv_confirmed_date,
           ma.transfer_in_date,

           COALESCE(c.state_province, '-')                                                                              AS region,
           COALESCE(c.county_district, '-')                                                                             AS zone,
           COALESCE(c.city_village, '-')                                                                                AS woreda,
           COALESCE(c.kebele, '-')                                                                                      AS kebele,
           COALESCE(c.house_number, '-')                                                                                AS house_number,
           COALESCE(c.mobile_no, '-')                                                                                   AS mobile_phone,
           CASE
               WHEN (c.patient_name IS NOT NULL AND c.state_province IS NOT NULL AND c.mobile_no IS NOT NULL)
                   THEN 'GREEN'
               ELSE 'YELLOW' END                                                                                        AS address_completeness,

           CASE
               WHEN lfu.visit_date IS NULL THEN '-'
               WHEN lfu.follow_up_status IN ('Dead', 'Died') THEN 'Dead'
               WHEN lfu.follow_up_status IN ('Transferred out', 'TO') THEN 'Transferred Out'
               WHEN lfu.treatment_end_date >= v_end THEN 'Active'
               ELSE COALESCE(lfu.follow_up_status, '-')
               END                                                                                                      AS current_status,

           COALESCE(lfu.regimen, '-')                                                                                   AS current_regimen,
           COALESCE(lfu.regimen_dose, '-')                                                                              AS regimen_dose,
           CASE
               WHEN lfu.regimen IS NULL THEN '-'
               WHEN lfu.regimen LIKE '1%' THEN 'First Line'
               WHEN lfu.regimen LIKE '2%' THEN 'Second Line'
               ELSE 'Other'
               END                                                                                                      AS regimen_line,

           lfu.treatment_end_date                                                                                       AS tx_curr_end_date,
           CASE COALESCE(lfu.nutritional_status_of_adult, lfu.nutritional_status_of_older_child_a)
               WHEN 'normal' THEN 'Normal'
               WHEN 'Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)' THEN 'Mild Malnutrition'
               WHEN 'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)'
                   THEN 'Moderate Malnutrition'
               WHEN 'Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)'
                   THEN 'Severe Malnutrition'
               WHEN 'Overweight' THEN 'Overweight'
               WHEN 'Obese Abdomen' THEN 'Obese'
               ELSE COALESCE(lfu.nutritional_status_of_adult, lfu.nutritional_status_of_older_child_a, '-')
               END                                                                                                      AS nutritional_status,
           COALESCE(lfu.pregnancy_status, '-')                                                                          AS pregnancy_status,
           CASE
               WHEN (pmtct.pmtct_start_date IS NOT NULL AND
                     (pmtct_dis.pmtct_discharge_date IS NULL OR
                      pmtct_dis.pmtct_discharge_date < pmtct.pmtct_start_date))
                   THEN 'Currently on PMTCT'
               ELSE 'Not Applicable'
               END                                                                                                      AS pmtct_status,
           COALESCE(fp.method_of_family_planning, '-')                                                                  AS family_planning_method,
           COALESCE(lfu.who_stage, '-')                                                                                 AS who_stage,

           lfu.visit_date                                                                                               AS last_visit_date,
           CASE
               WHEN DATEDIFF(v_end, COALESCE(lfu.next_visit_date, lfu.visit_date)) > 0
                   THEN DATEDIFF(v_end, COALESCE(lfu.next_visit_date, lfu.visit_date))
               ELSE 0 END                                                                                               AS days_overdue,

           lab.viral_load_perform_date                                                                                  AS last_vl_date,
           lab.viral_load_count                                                                                         AS last_vl_result,
           CASE
               WHEN lab.viral_load_status_inferred = 'S' THEN 1
               WHEN lab.viral_load_status_inferred = 'U' THEN 0
               ELSE NULL
               END                                                                                                      AS is_suppressed,

           CASE
               WHEN lfu.visit_date IS NULL THEN '-'
               WHEN lab.vl_status_final = 'N/A' THEN 'Not Applicable'
               WHEN lab.eligiblityDate <= v_end THEN 'Eligible for Viral Load'
               WHEN lab.eligiblityDate > v_end THEN 'Viral Load Done (Currently not Eligible)'
               WHEN lab.art_start_date IS NULL AND lab.follow_up_status IS NULL THEN 'Not Started ART'
               ELSE '-'
               END                                                                                                      AS vl_status,

           lab.eligiblityDate                                                                                           AS vl_eligibility_date,

           CASE
               WHEN ma.tpt_completed_date IS NOT NULL THEN 'Gold (TPT Completed)'
               WHEN ma.tpt_start_date IS NOT NULL AND ma.tpt_completed_date IS NULL AND ma.tpt_discontinued_date IS NULL
                   THEN 'On TPT'
               WHEN ma.tpt_is_contraindicated = 1 THEN 'Contraindicated'
               ELSE 'Not Started'
               END                                                                                                      AS tpt_status,

           ma.tpt_start_date,
           ma.tpt_completed_date,
           ma.tpt_discontinued_date,
           ma.active_tb_diagnosis_date,
           ma.tb_treatment_start_date,
           ma.tb_treatment_discontinued_date,
           ma.tb_treatment_completed_date,

           COALESCE(lfu.dsd_category, '-')                                                                              AS dsd_category,
           COALESCE(ict.ict_status, 'Not Screened')                                                                     AS ict_screening_status,
           COALESCE(ncd.baseline_diagnosis,
                    CASE WHEN ncd.client_id IS NOT NULL THEN 'Screened' ELSE 'Not Screened' END)                       AS ncd_screening_status,
           ncd_fu.next_screening_date                                                                                   AS next_ncd_screening_date,
           COALESCE(cxca.cxca_screening_status, 'Not Applicable')                                                       AS cxca_screening_status,
           cxca.next_follow_up_screening_date                                                                           AS next_cca_screening_date,
           ncd_fu.systolic_blood_pressure,
           ncd_fu.diastolic_blood_pressure,
           COALESCE(phrh.target_population, c.key_population, '-')                                                      AS target_population,

           COALESCE(lfu.currently_breastfeeding_child, '-')                                                             AS breast_feeding_status,
           COALESCE(disc.stages_of_disclosure, '-')                                                                     AS disclosure_stage,

           pmtct.pmtct_start_date                                                                                       AS pmtct_booking_date,
           fn_gregorian_to_ethiopian_calendar(pmtct.pmtct_start_date, 'Y-M-D')                                         AS pmtct_booking_date_ec,
           pmtct_dis.pmtct_discharge_date,
           fn_gregorian_to_ethiopian_calendar(pmtct_dis.pmtct_discharge_date, 'Y-M-D')                                 AS pmtct_discharge_date_ec,

           lab.VL_Sent_Date                                                                                             AS vl_sent_date,
           fn_gregorian_to_ethiopian_calendar(lab.VL_Sent_Date, 'Y-M-D')                                               AS vl_sent_date_ec,
           lab.viral_load_perform_date                                                                                   AS vl_received_date,
           fn_gregorian_to_ethiopian_calendar(lab.viral_load_perform_date, 'Y-M-D')                                     AS vl_received_date_ec,

           lfu_cd4.cd4_count                                                                                            AS cd4_result,
           lfu_cd4.visitect_cd4_result,

           fn_gregorian_to_ethiopian_calendar(ma.transfer_in_date, 'Y-M-D')                                            AS transfer_in_date_ec,
           fn_gregorian_to_ethiopian_calendar(ma.active_tb_diagnosis_date, 'Y-M-D')                                    AS active_tb_diagnosis_date_ec,
           fn_gregorian_to_ethiopian_calendar(ma.tb_treatment_start_date, 'Y-M-D')                                     AS tb_treatment_start_date_ec,
           fn_gregorian_to_ethiopian_calendar(ma.tb_treatment_discontinued_date, 'Y-M-D')                              AS tb_treatment_discontinued_date_ec,
           fn_gregorian_to_ethiopian_calendar(ma.tb_treatment_completed_date, 'Y-M-D')                                 AS tb_treatment_completed_date_ec,
           fn_gregorian_to_ethiopian_calendar(ma.tpt_start_date, 'Y-M-D')                                              AS tpt_start_date_ec,
           fn_gregorian_to_ethiopian_calendar(ma.tpt_discontinued_date, 'Y-M-D')                                       AS tpt_discontinued_date_ec,
           fn_gregorian_to_ethiopian_calendar(ma.tpt_completed_date, 'Y-M-D')                                          AS tpt_completed_date_ec,

           CASE
               WHEN ma.active_tb_diagnosis_date IS NULL THEN NULL
               WHEN ma.tb_treatment_completed_date IS NOT NULL THEN 'Completed TB Treatment'
               WHEN ma.tb_treatment_start_date IS NOT NULL
                    AND TIMESTAMPDIFF(YEAR, ma.tb_treatment_start_date, v_end) < 2
                    AND ma.tb_treatment_completed_date IS NULL THEN 'On TB Treatment'
               WHEN ma.tb_treatment_start_date IS NOT NULL
                    AND TIMESTAMPDIFF(YEAR, ma.tb_treatment_start_date, v_end) >= 2
                    AND ma.tb_treatment_completed_date IS NULL THEN 'Unknown TB Treatment Completion'
               ELSE 'Active TB Diagnosed & No RX'
               END                                                                                                      AS tb_treatment_rx_status,

           CASE
               WHEN pmtct.pmtct_start_date IS NULL THEN NULL
               WHEN pmtct_dis.pmtct_discharge_date IS NULL
                    OR pmtct_dis.pmtct_discharge_date < pmtct.pmtct_start_date
                    OR pmtct_dis.pmtct_discharge_date > v_end THEN 'Currently on PMTCT'
               ELSE 'Enrolled & Currently Discharged'
               END                                                                                                      AS pmtct_enrollment_status,

           CASE
               WHEN c.date_of_birth IS NOT NULL
                    AND TIMESTAMPDIFF(YEAR, c.date_of_birth, v_end) < 5 THEN 'Yes'
               WHEN (c.date_of_birth IS NULL OR TIMESTAMPDIFF(YEAR, c.date_of_birth, v_end) >= 5)
                    AND ((lfu_cd4.visitect_cd4_result IS NULL
                        AND lfu_cd4.cd4_count IS NOT NULL
                        AND lfu_cd4.cd4_count < 200)
                        OR lfu_cd4.visitect_cd4_result = 'VISITECT <200 copies/ml') THEN 'Yes'
               WHEN (c.date_of_birth IS NULL OR TIMESTAMPDIFF(YEAR, c.date_of_birth, v_end) >= 5)
                    AND lfu.who_stage IN ('WHO stage 3 adult', 'WHO stage 3 peds',
                                          'WHO stage 4 peds', 'WHO stage 4 adult') THEN 'Yes'
               ELSE 'No'
               END                                                                                                      AS advanced_hiv_disease,

           ncd.screening_date                                                                                            AS ncd_last_screening_date,

           CASE
               WHEN lfu.follow_up_status IN ('Dead', 'Died', 'Transferred out', 'TO', 'Stop all') THEN 'Not Applicable'
               WHEN ncd.client_id IS NULL THEN 'Not Screened'
               WHEN ncd.ncd_class = 'DM_HTN'     THEN 'Confirmed DM & HTN'
               WHEN ncd.ncd_class = 'HTN'         THEN 'Confirmed HTN'
               WHEN ncd.ncd_class = 'DM'          THEN 'Confirmed DM'
               WHEN ncd.ncd_class = 'PREDIABETES' THEN 'Need Rescreening Prediabetes'
               WHEN ncd.ncd_class = 'NORMAL_BG'
                    AND TIMESTAMPDIFF(YEAR, c.date_of_birth, v_end) <= 50 THEN 'Needs Rescreening Normal BG (≤50 Yrs)'
               WHEN ncd.ncd_class = 'NORMAL_BG'
                    AND TIMESTAMPDIFF(YEAR, c.date_of_birth, v_end) > 50  THEN 'Needs Rescreening Normal BG (>50 Yrs)'
               WHEN ncd.ncd_class = 'NORMAL_BP'   THEN 'Needs Rescreening Normal BP'
               ELSE 'Not Applicable'
               END                                                                                                      AS ncd_screening_eligibility_status,

           ncd_fu.next_screening_date                                                                                   AS ncd_screening_eligibility_date,

           CASE
               WHEN lfu.follow_up_status IN ('Dead', 'Died', 'Transferred out', 'TO', 'Stop all') THEN 'NOT APPLICABLE'
               WHEN ncd.client_id IS NULL THEN NULL
               WHEN ncd.ncd_class = 'DM_HTN'     THEN 'Confirmed Hypertension and Diabetes Mellitus'
               WHEN ncd.ncd_class = 'HTN'         THEN 'Confirmed Hypertension'
               WHEN ncd.ncd_class = 'DM'          THEN 'Confirmed Diabetes Mellitus'
               WHEN ncd.ncd_class = 'PREDIABETES' THEN 'Prediabetes - Needs Rescreening'
               WHEN ncd.ncd_class = 'NORMAL_BG'   THEN 'Normal Blood Glucose - Needs Rescreening'
               WHEN ncd.ncd_class = 'NORMAL_BP'   THEN 'Normal Blood Pressure - Needs Rescreening'
               ELSE 'NOT APPLICABLE'
               END                                                                                                      AS ncd_screening_reason,

           -- DQI corrected status columns
           CASE
               WHEN ma.art_start_date IS NULL THEN 'ART Not Started'
               WHEN lfu.visit_date IS NULL THEN '-'
               WHEN lab.vl_status_final = 'N/A' THEN 'Not Applicable'
               WHEN lab.eligiblityDate <= v_end THEN 'Eligible for Viral Load'
               WHEN lab.eligiblityDate > v_end THEN 'Viral Load Done (Currently not Eligible)'
               ELSE '-'
               END                                                                                                      AS vl_status_dqi,

           CASE
               WHEN ma.art_start_date IS NULL THEN 'ART Not Started'
               WHEN ma.tpt_completed_date IS NOT NULL THEN 'Gold (TPT Completed)'
               WHEN ma.tpt_start_date IS NOT NULL
                    AND ma.tpt_completed_date IS NULL
                    AND ma.tpt_discontinued_date IS NULL THEN 'On TPT'
               WHEN tpt_ci.reason_not_eligible_for_tuberculosi = 'Contraindication' THEN 'Contraindicated for TPT'
               WHEN ma.tb_treatment_completed_date IS NOT NULL
                    AND TIMESTAMPDIFF(YEAR, ma.tb_treatment_completed_date, v_end) > 3 THEN 'Bronze 5'
               ELSE 'Not Started'
               END                                                                                                      AS tpt_status_dqi,

           CASE
               WHEN ma.art_start_date IS NULL THEN 'ART Not Started'
               ELSE COALESCE(ict.ict_status, 'Not Screened')
               END                                                                                                      AS ict_screening_status_dqi,

           CASE
               WHEN ma.art_start_date IS NULL THEN 'ART Not Started'
               ELSE COALESCE(ncd.baseline_diagnosis,
                             CASE WHEN ncd.client_id IS NOT NULL THEN 'Screened' ELSE 'Not Screened' END)
               END                                                                                                      AS ncd_screening_status_dqi,

           CASE cxca.cxca_screening_status
               WHEN 'Confirmed CXCA (RED)' THEN 'Confirmed Cervical Cancer (Red)'
               WHEN 'Eligible (Yellow)' THEN 'Eligible for Cervical Cancer Screening/RX (Yellow)'
               ELSE COALESCE(cxca.cxca_screening_status, 'Not Applicable (Blue)')
               END                                                                                                      AS cxca_screening_status_dqi

    FROM mamba_dim_client c
             LEFT JOIN LatestFollowUp lfu ON c.client_id = lfu.client_id
             LEFT JOIN Medical_Aggregates ma ON c.client_id = ma.client_id
             LEFT JOIN vl_eligibility lab ON c.client_id = lab.client_id
             LEFT JOIN Identifiers id ON c.client_id = id.patient_id
             LEFT JOIN ICT_Events ict ON c.client_id = ict.client_id
             LEFT JOIN ICT_General_Events ict_gen ON c.client_id = ict_gen.client_id
             LEFT JOIN NCD_Screening_Events ncd ON c.client_id = ncd.client_id
             LEFT JOIN NCD_FollowUp_Events ncd_fu ON c.client_id = ncd_fu.client_id
             LEFT JOIN CXCA_Status cxca ON c.client_id = cxca.client_id
             LEFT JOIN PMTCT_CTE pmtct ON c.client_id = pmtct.client_id
             LEFT JOIN PMTCT_Discharge_CTE pmtct_dis ON c.client_id = pmtct_dis.client_id
             LEFT JOIN Registration reg ON c.client_id = reg.client_id
             LEFT JOIN PHRH_Target_Population phrh ON c.client_id = phrh.client_id
             LEFT JOIN FamilyPlanning fp ON c.client_id = fp.client_id
             LEFT JOIN Latest_CD4 lfu_cd4 ON c.client_id = lfu_cd4.client_id
             LEFT JOIN Latest_Disclosure disc ON c.client_id = disc.client_id
             LEFT JOIN Latest_TPT_Contraindication tpt_ci ON c.client_id = tpt_ci.client_id
    WHERE c.mrn IS NOT NULL
      AND lfu.visit_date IS NOT NULL
      AND lfu.visit_date <= v_end
      AND (REPORT_START_DATE IS NULL OR lfu.visit_date >= REPORT_START_DATE)
    ORDER BY c.patient_name;

END //

DELIMITER ;
