-- $BEGIN
INSERT INTO mamba_fact_client_staging
(client_id, patient_uuid, mrn, patient_name, sex, birthdate, age, uan, phrh_code, ncd_code, icd_number, ict_number,
 registration_date, art_start_date, months_on_art, next_appointment_date, hiv_confirmed_date, transfer_in_date,
 region, zone, woreda, kebele, house_number, mobile_phone, address_completeness, current_status,
 current_regimen, regimen_dose, regimen_line, tx_curr_end_date, nutritional_status, pregnancy_status,
 pmtct_status, family_planning_method, who_stage, last_visit_date, days_overdue, last_vl_date, last_vl_result,
 is_suppressed, vl_status, vl_eligibility_date, tpt_status, tpt_start_date, tpt_completed_date,
 tpt_discontinued_date, active_tb_diagnosis_date, tb_treatment_start_date, tb_treatment_discontinued_date,
 tb_treatment_completed_date, dsd_category, ict_screening_status, ncd_screening_status, next_ncd_screening_date,
 cxca_screening_status, next_cca_screening_date, systolic_blood_pressure, diastolic_blood_pressure, target_population,
 breast_feeding_status, disclosure_stage,
 pmtct_booking_date, pmtct_booking_date_ec, pmtct_discharge_date, pmtct_discharge_date_ec,
 vl_sent_date, vl_sent_date_ec, vl_received_date, vl_received_date_ec,
 cd4_result, visitect_cd4_result,
 transfer_in_date_ec,
 active_tb_diagnosis_date_ec, tb_treatment_start_date_ec, tb_treatment_discontinued_date_ec, tb_treatment_completed_date_ec,
 tpt_start_date_ec, tpt_discontinued_date_ec, tpt_completed_date_ec,
 tb_treatment_rx_status, pmtct_enrollment_status, advanced_hiv_disease,
 ncd_last_screening_date, ncd_screening_eligibility_status, ncd_screening_eligibility_date, ncd_screening_reason,
 vl_status_dqi, tpt_status_dqi, ict_screening_status_dqi, ncd_screening_status_dqi, cxca_screening_status_dqi)
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
                         biopsy_result,
                         biopsy_result_received_date,
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
                  FROM mamba_fact_follow_up),

     LatestFollowUp AS (SELECT *
                        FROM (SELECT client_id,
                                     follow_up_date_followup_                                                                             as visit_date,
                                     next_visit_date,
                                     follow_up_status,
                                     regimen,
                                     antiretroviral_art_dispensed_dose_i                                                                  as regimen_dose,
                                     treatment_end_date,
                                     nutritional_status_of_adult,
                                     nutritional_status_of_older_child_a,
                                     mid_upper_arm_circumference,
                                     pregnancy_status,
                                     art_start_date,
                                     current_who_hiv_stage                                                                                as who_stage,
                                     currently_breastfeeding_child,
                                     weight,
                                     method_of_family_planning,
                                     dsd_category,
                                     date_of_event                                                                                        as hiv_confirmed_date,
                                     transfer_in,
                                     eligible_for_cxca_screening,
                                     reason_for_not_being_eligible,
                                     other_reason_for_not_being_eligible_for_cxca,
                                     cx_ca_screening_status                                                                               as screening_status,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) as rn
                              FROM FollowUp
                              WHERE follow_up_date_followup_ IS NOT NULL) ranked
                        WHERE rn = 1),

     Medical_Aggregates AS (SELECT client_id,
                                   MIN(CASE
                                           WHEN transfer_in IN ('Yes', 'True', '1') OR
                                                follow_up_status IN ('Transfer in', 'Transfer In', 'TI')
                                               THEN follow_up_date_followup_ END) as transfer_in_date,
                                   MIN(art_start_date)                            as art_start_date,
                                   MAX(date_started_on_tuberculosis_prophy)       as tpt_start_date,
                                   MAX(date_completed_tuberculosis_prophyl)       as tpt_completed_date,
                                   MAX(date_discontinued_tuberculosis_prop)       as tpt_discontinued_date,
                                   MAX(CASE
                                           WHEN reason_not_eligible_for_tuberculosi = 'Contraindication' THEN 1
                                           ELSE 0 END)                            as tpt_is_contraindicated,
                                   MAX(diagnosis_date)                            as active_tb_diagnosis_date,
                                   MAX(tuberculosis_drug_treatment_start_d)       as tb_treatment_start_date,
                                   MAX(date_active_tbrx_dc)                       as tb_treatment_discontinued_date,
                                   MAX(date_active_tbrx_completed)                as tb_treatment_completed_date
                            FROM FollowUp
                            GROUP BY client_id),

     Vl_Events AS (SELECT encounter_id,
                          client_id,
                          follow_up_date_followup_,
                          date_of_reported_hiv_viral_load                                                                                                                                   as viral_load_sent_date,
                          date_viral_load_results_received                                                                                                                                  as viral_load_perform_date,
                          regimen_change,
                          routine_viral_load_test_indication,
                          follow_up_date_followup_                                                                                                                                          as FollowupDate,
                          currently_breastfeeding_child                                                                                                                                     as breastfeeding_status,
                          viral_load_test_status,
                          viral_load_count,
                          ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY (date_of_reported_hiv_viral_load IS NOT NULL) DESC, date_of_reported_hiv_viral_load DESC, encounter_id DESC)   AS rn_sent,
                          ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY (regimen_change IS NOT NULL) DESC, follow_up_date_followup_ DESC, encounter_id DESC)                           AS rn_switch,
                          ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY (date_viral_load_results_received IS NOT NULL) DESC, date_viral_load_results_received DESC, encounter_id DESC) AS rn_perf
                   FROM FollowUp),

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
                            sub_switch_date.FollowupDate as switchDate,
                            vlperfdate.VL_Sent_Date,
                            vlperfdate.viral_load_perform_date,
                            vlperfdate.viral_load_status_inferred,
                            CASE
                                WHEN vlperfdate.VL_Sent_Date IS NULL AND f_case.follow_up_status = 'Restart medication'
                                    THEN 'RESTART_NULL'
                                WHEN vlperfdate.VL_Sent_Date IS NULL AND sub_switch_date.FollowupDate IS NOT NULL
                                    THEN 'SWITCH_NULL'
                                WHEN vlperfdate.VL_Sent_Date IS NULL AND f_case.pregnancy_status = 'Yes' AND
                                     TIMESTAMPDIFF(DAY, f_case.art_start_date, CURDATE()) > 90 THEN 'PREG_NULL'
                                WHEN vlperfdate.VL_Sent_Date IS NULL AND
                                     TIMESTAMPDIFF(DAY, f_case.art_start_date, CURDATE()) <= 180 THEN 'INELIGIBLE_NULL'
                                WHEN vlperfdate.VL_Sent_Date IS NULL AND
                                     TIMESTAMPDIFF(DAY, f_case.art_start_date, CURDATE()) > 180 THEN 'FIRST_NULL'
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
                            f_case.hiv_confirmed_date,
                            f_case.weight,
                            f_case.regimen_dose,
                            f_case.regimen,
                            f_case.next_visit_date,
                            f_case.treatment_end_date,
                            vlperfdate.viral_load_count,
                            vlperfdate.viral_load_status
                     FROM LatestFollowUp f_case
                              LEFT JOIN vl_performed as vlperfdate ON vlperfdate.client_id = f_case.client_id
                              LEFT JOIN Vl_Events as sub_switch_date ON sub_switch_date.client_id = f_case.client_id AND
                                                                        sub_switch_date.rn_switch = 1 AND
                                                                        sub_switch_date.regimen_change IS NOT NULL
                     WHERE f_case.rn = 1),

     vl_eligibility AS (SELECT *,
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
                                   ELSE DATE_ADD(CURDATE(), INTERVAL 100 YEAR)
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
                        FROM vl_scenario),

     Identifiers AS (SELECT patient_id,
                            MAX(CASE WHEN pit.name = 'PHRH' THEN pi.identifier END) as phrh_code,
                            MAX(CASE WHEN pit.name = 'NCD' THEN pi.identifier END)  as ncd_code,
                            MAX(CASE WHEN pit.name = 'ICD' THEN pi.identifier END)  as icd_number
                     FROM mamba_dim_patient_identifier pi
                              JOIN mamba_dim_patient_identifier_type pit
                                   ON pi.identifier_type = pit.patient_identifier_type_id
                     WHERE pit.name IN ('PHRH', 'NCD', 'ICD')
                     GROUP BY patient_id),

     ICT_Events AS (SELECT *
                    FROM (SELECT client_id,
                                 CASE
                                     WHEN accepted = 'Yes' THEN 'Accepted'
                                     WHEN accepted = 'No' THEN 'Refused'
                                     ELSE 'Offered' END                                                as ict_status,
                                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY offered_date DESC) as rn
                          FROM mamba_flat_encounter_ict_offer
                          WHERE offered_date IS NOT NULL) ranked
                    WHERE rn = 1),

     ICT_General_Events AS (SELECT *
                            FROM (SELECT client_id,
                                         ict_serial_number                                                                              as ict_number,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY encounter_datetime DESC, encounter_id DESC) as rn
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
                                                   AND baseline_diagnosis LIKE '%Diabetes%'       THEN 'DM_HTN'
                                               WHEN baseline_diagnosis LIKE '%Hypertension%'      THEN 'HTN'
                                               WHEN baseline_diagnosis LIKE '%Diabetes Mellitus%' THEN 'DM'
                                               WHEN baseline_diagnosis LIKE '%Prediabetes%'
                                                   OR baseline_diagnosis LIKE '%Pre-diabetes%'    THEN 'PREDIABETES'
                                               WHEN baseline_diagnosis LIKE '%Normal BG%'
                                                   OR baseline_diagnosis LIKE '%Normal Blood Glucose%' THEN 'NORMAL_BG'
                                               WHEN baseline_diagnosis LIKE '%Normal BP%'
                                                   OR baseline_diagnosis LIKE '%Normal Blood Pressure%' THEN 'NORMAL_BP'
                                               ELSE NULL
                                               END                                                AS ncd_class,
                                           ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY screening_date DESC) as rn
                                    FROM mamba_flat_encounter_ncd_screening
                                    WHERE screening_date IS NOT NULL) ranked
                              WHERE rn = 1),

     NCD_FollowUp_Events AS (SELECT *
                             FROM (SELECT f.client_id,
                                          f1.return_visit_date                                                                           as next_screening_date,
                                          f1.systolic_blood_pressure,
                                          f1.diastolic_blood_pressure,
                                          ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY follow_up_date DESC, f.encounter_id DESC) as rn
                                   FROM mamba_flat_encounter_ncd_followup f
                                            LEFT JOIN mamba_flat_encounter_ncd_followup_1 f1
                                                      on f.encounter_id = f1.encounter_id
                                   WHERE f1.return_visit_date IS NOT NULL
                                      OR f1.systolic_blood_pressure IS NOT NULL
                                      OR f1.diastolic_blood_pressure IS NOT NULL) ranked
                             WHERE rn = 1),

     CXCA_PrevScreening AS (SELECT *
                            FROM (SELECT client_id,
                                         follow_up_date_followup_                                                                             as follow_up_date,
                                         via_screening_result                                                                                 as ccs_via_result,
                                         via_date                                                                                             as via_date,
                                         ccs_treat_received_date,
                                         date_patient_referred_out,
                                         biopsy_result,
                                         hpv_dna_screening_result                                                                             as ccs_hpv_result,
                                         hpv_received_date                                                                                    as hpv_dna_result_received_date,
                                         cytology_result,
                                         cytology_received_date                                                                               as cytology_result_date,
                                         colposcopy_exam_finding,
                                         biopsy_result_received_date,
                                         next_follow_up_screening_date,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) as rn
                                  FROM FollowUp
                                  WHERE cx_ca_screening_status = 'Cervical cancer screening performed') ranked
                            WHERE rn = 1),

     CXCA_Eligibility_Base AS (SELECT lf.client_id,
                                      lf.visit_date,
                                      lf.follow_up_status                             as final_follow_up_status,
                                      lf.eligible_for_cxca_screening,
                                      lf.reason_for_not_being_eligible,
                                      lf.other_reason_for_not_being_eligible_for_cxca,
                                      lf.screening_status,
                                      lf.art_start_date,
                                      ps.follow_up_date                               AS previous_screening_follow_up_date,
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
                                      TIMESTAMPDIFF(YEAR, c.date_of_birth, CURDATE()) AS Age,

                                      CASE
                                          WHEN ps.client_id IS NULL AND lf.eligible_for_cxca_screening = 'No'
                                              THEN CONCAT('Not Eligible ',
                                                          COALESCE(lf.reason_for_not_being_eligible,
                                                                   lf.other_reason_for_not_being_eligible_for_cxca))
                                          WHEN (ps.biopsy_result = 'Carcinoma in situ' OR
                                                ps.biopsy_result = 'Invasive cervical cancer' OR
                                                ps.biopsy_result = 'Other')
                                              AND (ps.ccs_treat_received_date <= CURDATE() OR
                                                   ps.date_patient_referred_out <= CURDATE())
                                              THEN 'Not Eligible Confirmed Cirvical Cancer'
                                          WHEN ps.client_id IS NULL AND
                                               lf.screening_status != 'Cervical cancer screening performed' AND
                                               lf.eligible_for_cxca_screening = 'Yes'
                                              THEN 'Eligible Labeled by User'
                                          WHEN ps.client_id IS NULL AND lf.eligible_for_cxca_screening IS NULL
                                              THEN 'Eligible Never Screened/Assessed'
                                          WHEN ps.ccs_via_result = 'Unknown'
                                              THEN 'Eligible Unknown VIA Screening Result'
                                          WHEN (TIMESTAMPDIFF(DAY, ps.via_date, CURDATE())) > 730 AND
                                               ps.ccs_via_result = 'VIA negative'
                                              THEN 'Eligible Needs Re-Screening'
                                          WHEN ps.ccs_via_result = 'VIA positive: eligible for cryo/thermo-coagula'
                                              THEN CASE
                                                       WHEN
                                                           ((TIMESTAMPDIFF(DAY, ps.ccs_treat_received_date, CURDATE())) >
                                                            365 OR
                                                            (TIMESTAMPDIFF(DAY, ps.date_patient_referred_out, CURDATE())) >
                                                            365) AND
                                                           ps.biopsy_result IS NULL
                                                           THEN 'Eligible Post Treatment/Referral Follow-Up'
                                                       WHEN ps.ccs_treat_received_date IS NULL AND
                                                            ps.date_patient_referred_out IS NULL
                                                           THEN 'Eligible Treatment Not Received or Not Referred'
                                                       ELSE 'Not Eligible (Screening Up-to-Date)'
                                              END
                                          WHEN (ps.ccs_via_result =
                                                'VIA positive: non-eligible for cryo/thermo-coagula' OR
                                                ps.ccs_via_result = 'suspected cervical cancer') AND
                                               ps.biopsy_result IS NULL
                                              THEN 'Eligible Biopsy Test Not Done'
                                          WHEN ps.ccs_hpv_result = 'Negative result' AND
                                               TIMESTAMPDIFF(DAY, ps.hpv_dna_result_received_date, CURDATE()) > 1095
                                              THEN 'Eligible Needs Re-Screening'
                                          WHEN ps.ccs_via_result IS NULL AND ps.ccs_hpv_result = 'Positive'
                                              THEN 'Eligible Needs VIA Triage'
                                          WHEN ps.cytology_result = 'Negative result' AND
                                               TIMESTAMPDIFF(DAY, ps.cytology_result_date, CURDATE()) > 1095
                                              THEN 'Eligible Needs Re-Screening'
                                          WHEN (ps.cytology_result =
                                                'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear' OR
                                                ps.cytology_result = '> Ascus')
                                              THEN CASE
                                                       WHEN ps.hpv_dna_result_received_date IS NULL
                                                           THEN 'Eligible Needs HPV Triage'
                                                       WHEN ps.ccs_hpv_result = 'Negative result' AND
                                                            TIMESTAMPDIFF(DAY, ps.hpv_dna_result_received_date, CURDATE()) >
                                                            730
                                                           THEN 'Eligible Needs Re-Screening'
                                                       WHEN ps.ccs_hpv_result = 'Positive' AND ps.colposcopy_exam_finding IS NULL
                                                           THEN 'Eligible Needs Colposcopy Test'
                                                       ELSE 'Not Eligible (Screening Up-to-Date)'
                                              END
                                          WHEN (ps.biopsy_result = 'CIN (1-3)' OR ps.biopsy_result = 'CIN-2') AND
                                               TIMESTAMPDIFF(DAY, ps.biopsy_result_received_date, CURDATE()) > 365
                                              THEN 'Eligible Needs Re-Screening'
                                          ELSE 'Not Eligible (Screening Up-to-Date)'
                                          END                                         AS EligibilityStatus
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
                          MAX(date_of_enrollment_or_booking) as pmtct_start_date
                   FROM mamba_flat_encounter_pmtct_enrollment
                   WHERE date_of_enrollment_or_booking IS NOT NULL
                   GROUP BY client_id),

     PMTCT_Discharge_CTE AS (SELECT client_id,
                                    MAX(discharge_date) as pmtct_discharge_date
                             FROM mamba_flat_encounter_pmtct_discharge
                             WHERE discharge_date IS NOT NULL
                             GROUP BY client_id),

     Registration AS (SELECT client_id,
                             MIN(registration_date) as registration_date
                      FROM mamba_flat_encounter_registration
                      WHERE registration_date IS NOT NULL
                      GROUP BY client_id),

     PHRH_Target_Population AS (SELECT *
                                FROM (SELECT client_id,
                                             target_population,
                                             ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY followup_date DESC, encounter_id DESC) as rn
                                      FROM mamba_flat_encounter_phrh_followup
                                      WHERE target_population IS NOT NULL
                                        AND target_population != '') ranked
                                WHERE rn = 1),

     FamilyPlanning AS (SELECT *
                        FROM (SELECT client_id,
                                     method_of_family_planning,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) as rn
                              FROM FollowUp
                              WHERE method_of_family_planning IS NOT NULL
                                AND method_of_family_planning != ''
                                AND method_of_family_planning != '-') ranked
                        WHERE rn = 1),

     Latest_CD4 AS (SELECT *
                    FROM (SELECT client_id,
                                 cd4_count,
                                 visitect_cd4_result,
                                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) as rn
                          FROM mamba_fact_follow_up
                          WHERE cd4_count IS NOT NULL OR visitect_cd4_result IS NOT NULL) ranked
                    WHERE rn = 1),

     Latest_Disclosure AS (SELECT *
                           FROM (SELECT client_id,
                                        stages_of_disclosure,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) as rn
                                 FROM mamba_fact_follow_up
                                 WHERE stages_of_disclosure IS NOT NULL) ranked
                           WHERE rn = 1),

     Latest_TPT_Contraindication AS (SELECT *
                                     FROM (SELECT client_id,
                                                  reason_not_eligible_for_tuberculosi,
                                                  ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) as rn
                                           FROM mamba_fact_follow_up
                                           WHERE reason_not_eligible_for_tuberculosi IS NOT NULL) ranked
                                     WHERE rn = 1)


SELECT c.client_id,
       c.patient_uuid,
       COALESCE(c.mrn, '-')                                                                                          as mrn,
       COALESCE(c.patient_name, '-')                                                                                 as patient_name,
       c.sex,
       c.date_of_birth                                                                                               as birthdate,
       TIMESTAMPDIFF(YEAR, c.date_of_birth, CURDATE())                                                               as age,

       COALESCE(c.uan, '-')                                                                                          as uan,
       COALESCE(id.phrh_code, '-')                                                                                   as phrh_code,
       COALESCE(id.ncd_code, '-')                                                                                    as ncd_code,
       COALESCE(id.icd_number, '-')                                                                                  as icd_number,
       COALESCE(ict_gen.ict_number, '-')                                                                             as ict_number,

       reg.registration_date                                                                                         as registration_date,
       ma.art_start_date                                                                                             as art_start_date,
       TIMESTAMPDIFF(MONTH, ma.art_start_date, CURDATE())                                                            as months_on_art,
       lfu.next_visit_date                                                                                           as next_appointment_date,

       lfu.hiv_confirmed_date                                                                                        as hiv_confirmed_date,
       ma.transfer_in_date                                                                                           as transfer_in_date,

       COALESCE(c.state_province, '-')                                                                               as region,
       COALESCE(c.county_district, '-')                                                                              as zone,
       COALESCE(c.city_village, '-')                                                                                 as woreda,
       COALESCE(c.kebele, '-')                                                                                       as kebele,
       COALESCE(c.house_number, '-')                                                                                 as house_number,
       COALESCE(c.mobile_no, '-')                                                                                    as mobile_phone,
       CASE
           WHEN (c.patient_name IS NOT NULL AND c.state_province IS NOT NULL AND c.mobile_no IS NOT NULL) THEN 'GREEN'
           ELSE 'YELLOW' END                                                                                         as address_completeness,

       CASE
           WHEN lfu.visit_date IS NULL THEN '-'
           WHEN lfu.follow_up_status IN ('Dead', 'Died') THEN 'Dead'
           WHEN lfu.follow_up_status IN ('Transferred out', 'TO') THEN 'Transferred Out'
           WHEN lfu.treatment_end_date >= CURDATE() THEN 'Active'
           ELSE COALESCE(lfu.follow_up_status, '-')
           END                                                                                                       as current_status,

       COALESCE(lfu.regimen, '-')                                                                                    as current_regimen,
       COALESCE(lfu.regimen_dose, '-')                                                                               as regimen_dose,

       CASE
           WHEN lfu.regimen IS NULL THEN '-'
           WHEN lfu.regimen LIKE '1%' THEN 'First Line'
           WHEN lfu.regimen LIKE '2%' THEN 'Second Line'
           ELSE 'Other'
           END                                                                                                       as regimen_line,

       lfu.treatment_end_date                                                                                        as tx_curr_end_date,
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
           END                                                                                                       AS nutritional_status,
       COALESCE(lfu.pregnancy_status, '-')                                                                           as pregnancy_status,
       CASE
           WHEN (pmtct.pmtct_start_date IS NOT NULL AND
                 (pmtct_dis.pmtct_discharge_date IS NULL OR pmtct_dis.pmtct_discharge_date < pmtct.pmtct_start_date))
               THEN 'Currently on PMTCT'
           ELSE 'Not Applicable'
           END                                                                                                       as pmtct_status,
       COALESCE(fp.method_of_family_planning, '-')                                                                   as family_planning_method,
       COALESCE(lfu.who_stage, '-')                                                                                  as who_stage,

       lfu.visit_date                                                                                                as last_visit_date,
       CASE
           WHEN DATEDIFF(CURDATE(), COALESCE(lfu.next_visit_date, lfu.visit_date)) > 0 THEN DATEDIFF(CURDATE(),
                                                                                                     COALESCE(lfu.next_visit_date, lfu.visit_date))
           ELSE 0 END                                                                                                as days_overdue,

       lab.viral_load_perform_date                                                                                   as last_vl_date,
       lab.viral_load_count                                                                                          as last_vl_result,
       CASE
           WHEN lab.viral_load_status_inferred = 'S' THEN 1
           WHEN lab.viral_load_status_inferred = 'U' THEN 0
           ELSE NULL
           END                                                                                                       as is_suppressed,

       CASE
           WHEN lfu.visit_date IS NULL THEN '-'
           WHEN lab.vl_status_final = 'N/A' THEN 'Not Applicable'
           WHEN lab.eligiblityDate <= CURDATE() THEN 'Eligible for Viral Load'
           WHEN lab.eligiblityDate > CURDATE() THEN 'Viral Load Done (Currently not Eligible)'
           WHEN lab.art_start_date IS NULL AND lab.follow_up_status IS NULL THEN 'Not Started ART'
           ELSE '-'
           END                                                                                                       as vl_status,

       lab.eligiblityDate                                                                                            as vl_eligibility_date,

       CASE
           WHEN ma.tpt_completed_date IS NOT NULL THEN 'Gold (TPT Completed)'
           WHEN ma.tpt_start_date IS NOT NULL AND ma.tpt_completed_date IS NULL AND ma.tpt_discontinued_date IS NULL
               THEN 'On TPT'
           WHEN ma.tpt_is_contraindicated = 1 THEN 'Contraindicated'
           ELSE 'Not Started'
           END                                                                                                       as tpt_status,

       ma.tpt_start_date                                                                                             as tpt_start_date,
       ma.tpt_completed_date                                                                                         as tpt_completed_date,
       ma.tpt_discontinued_date                                                                                      as tpt_discontinued_date,

       ma.active_tb_diagnosis_date                                                                                   as active_tb_diagnosis_date,
       ma.tb_treatment_start_date                                                                                    as tb_treatment_start_date,
       ma.tb_treatment_discontinued_date                                                                             as tb_treatment_discontinued_date,
       ma.tb_treatment_completed_date                                                                                as tb_treatment_completed_date,

       COALESCE(lfu.dsd_category, '-')                                                                               as dsd_category,

       COALESCE(ict.ict_status, 'Not Screened')                                                                      as ict_screening_status,
       COALESCE(ncd.baseline_diagnosis, CASE
                                            WHEN ncd.client_id IS NOT NULL THEN 'Screened'
                                            ELSE 'Not Screened' END)                                                 as ncd_screening_status,
       ncd_fu.next_screening_date                                                                                    as next_ncd_screening_date,

       COALESCE(cxca.cxca_screening_status, 'Not Applicable')                                                        as cxca_screening_status,
       cxca.next_follow_up_screening_date                                                                            as next_cca_screening_date,
       ncd_fu.systolic_blood_pressure                                                                                as systolic_blood_pressure,
       ncd_fu.diastolic_blood_pressure                                                                               as diastolic_blood_pressure,

       COALESCE(phrh.target_population, c.key_population, '-')                                                       as target_population,

       COALESCE(lfu.currently_breastfeeding_child, '-')                                                             as breast_feeding_status,
       COALESCE(disc.stages_of_disclosure, '-')                                                                     as disclosure_stage,

       pmtct.pmtct_start_date                                                                                       as pmtct_booking_date,
       fn_gregorian_to_ethiopian_calendar(pmtct.pmtct_start_date, 'Y-M-D')                                         as pmtct_booking_date_ec,
       pmtct_dis.pmtct_discharge_date                                                                               as pmtct_discharge_date,
       fn_gregorian_to_ethiopian_calendar(pmtct_dis.pmtct_discharge_date, 'Y-M-D')                                 as pmtct_discharge_date_ec,

       lab.VL_Sent_Date                                                                                             as vl_sent_date,
       fn_gregorian_to_ethiopian_calendar(lab.VL_Sent_Date, 'Y-M-D')                                               as vl_sent_date_ec,
       lab.viral_load_perform_date                                                                                   as vl_received_date,
       fn_gregorian_to_ethiopian_calendar(lab.viral_load_perform_date, 'Y-M-D')                                     as vl_received_date_ec,

       lfu_cd4.cd4_count                                                                                            as cd4_result,
       lfu_cd4.visitect_cd4_result                                                                                  as visitect_cd4_result,

       fn_gregorian_to_ethiopian_calendar(ma.transfer_in_date, 'Y-M-D')                                            as transfer_in_date_ec,
       fn_gregorian_to_ethiopian_calendar(ma.active_tb_diagnosis_date, 'Y-M-D')                                    as active_tb_diagnosis_date_ec,
       fn_gregorian_to_ethiopian_calendar(ma.tb_treatment_start_date, 'Y-M-D')                                     as tb_treatment_start_date_ec,
       fn_gregorian_to_ethiopian_calendar(ma.tb_treatment_discontinued_date, 'Y-M-D')                              as tb_treatment_discontinued_date_ec,
       fn_gregorian_to_ethiopian_calendar(ma.tb_treatment_completed_date, 'Y-M-D')                                 as tb_treatment_completed_date_ec,
       fn_gregorian_to_ethiopian_calendar(ma.tpt_start_date, 'Y-M-D')                                              as tpt_start_date_ec,
       fn_gregorian_to_ethiopian_calendar(ma.tpt_discontinued_date, 'Y-M-D')                                       as tpt_discontinued_date_ec,
       fn_gregorian_to_ethiopian_calendar(ma.tpt_completed_date, 'Y-M-D')                                          as tpt_completed_date_ec,

       CASE
           WHEN ma.active_tb_diagnosis_date IS NULL THEN NULL
           WHEN ma.tb_treatment_completed_date IS NOT NULL THEN 'Completed TB Treatment'
           WHEN ma.tb_treatment_start_date IS NOT NULL
               AND TIMESTAMPDIFF(YEAR, ma.tb_treatment_start_date, CURDATE()) < 2
               AND ma.tb_treatment_completed_date IS NULL THEN 'On TB Treatment'
           WHEN ma.tb_treatment_start_date IS NOT NULL
               AND TIMESTAMPDIFF(YEAR, ma.tb_treatment_start_date, CURDATE()) >= 2
               AND ma.tb_treatment_completed_date IS NULL THEN 'Unknown TB Treatment Completion'
           ELSE 'Active TB Diagnosed & No RX'
           END                                                                                                       as tb_treatment_rx_status,

       CASE
           WHEN pmtct.pmtct_start_date IS NULL THEN NULL
           WHEN pmtct_dis.pmtct_discharge_date IS NULL
                OR pmtct_dis.pmtct_discharge_date < pmtct.pmtct_start_date
                OR pmtct_dis.pmtct_discharge_date > CURDATE() THEN 'Currently on PMTCT'
           ELSE 'Enrolled & Currently Discharged'
           END                                                                                                       as pmtct_enrollment_status,

       CASE
           WHEN c.date_of_birth IS NOT NULL
               AND TIMESTAMPDIFF(YEAR, c.date_of_birth, CURDATE()) < 5 THEN 'Yes'
           WHEN (c.date_of_birth IS NULL OR TIMESTAMPDIFF(YEAR, c.date_of_birth, CURDATE()) >= 5)
               AND ((lfu_cd4.visitect_cd4_result IS NULL
                   AND lfu_cd4.cd4_count IS NOT NULL
                   AND lfu_cd4.cd4_count < 200)
                   OR lfu_cd4.visitect_cd4_result = 'VISITECT <200 copies/ml') THEN 'Yes'
           WHEN (c.date_of_birth IS NULL OR TIMESTAMPDIFF(YEAR, c.date_of_birth, CURDATE()) >= 5)
               AND lfu.who_stage IN ('WHO stage 3 adult', 'WHO stage 3 peds',
                                     'WHO stage 4 peds', 'WHO stage 4 adult') THEN 'Yes'
           ELSE 'No'
           END                                                                                                       as advanced_hiv_disease,

       ncd.screening_date                                                                                             as ncd_last_screening_date,

       CASE
           WHEN lfu.follow_up_status IN ('Dead', 'Died', 'Transferred out', 'TO', 'Stop all') THEN 'Not Applicable'
           WHEN ncd.client_id IS NULL THEN 'Not Screened'
           WHEN ncd.ncd_class = 'DM_HTN'     THEN 'Confirmed DM & HTN'
           WHEN ncd.ncd_class = 'HTN'         THEN 'Confirmed HTN'
           WHEN ncd.ncd_class = 'DM'          THEN 'Confirmed DM'
           WHEN ncd.ncd_class = 'PREDIABETES' THEN 'Need Rescreening Prediabetes'
           WHEN ncd.ncd_class = 'NORMAL_BG'
               AND TIMESTAMPDIFF(YEAR, c.date_of_birth, CURDATE()) <= 50 THEN 'Needs Rescreening Normal BG (≤50 Yrs)'
           WHEN ncd.ncd_class = 'NORMAL_BG'
               AND TIMESTAMPDIFF(YEAR, c.date_of_birth, CURDATE()) > 50  THEN 'Needs Rescreening Normal BG (>50 Yrs)'
           WHEN ncd.ncd_class = 'NORMAL_BP'  THEN 'Needs Rescreening Normal BP'
           ELSE 'Not Applicable'
           END                                                                                                       as ncd_screening_eligibility_status,

       ncd_fu.next_screening_date                                                                                    as ncd_screening_eligibility_date,

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
           END                                                                                                       as ncd_screening_reason,

       -- DQI corrected status columns
       CASE
           WHEN ma.art_start_date IS NULL THEN 'ART Not Started'
           WHEN lfu.visit_date IS NULL THEN '-'
           WHEN lab.vl_status_final = 'N/A' THEN 'Not Applicable'
           WHEN lab.eligiblityDate <= CURDATE() THEN 'Eligible for Viral Load'
           WHEN lab.eligiblityDate > CURDATE() THEN 'Viral Load Done (Currently not Eligible)'
           ELSE '-'
           END                                                                                                       as vl_status_dqi,

       CASE
           WHEN ma.art_start_date IS NULL THEN 'ART Not Started'
           WHEN ma.tpt_completed_date IS NOT NULL THEN 'Gold (TPT Completed)'
           WHEN ma.tpt_start_date IS NOT NULL
               AND ma.tpt_completed_date IS NULL
               AND ma.tpt_discontinued_date IS NULL THEN 'On TPT'
           WHEN tpt_ci.reason_not_eligible_for_tuberculosi = 'Contraindication' THEN 'Contraindicated for TPT'
           WHEN ma.tb_treatment_completed_date IS NOT NULL
               AND TIMESTAMPDIFF(YEAR, ma.tb_treatment_completed_date, CURDATE()) > 3 THEN 'Bronze 5'
           ELSE 'Not Started'
           END                                                                                                       as tpt_status_dqi,

       CASE
           WHEN ma.art_start_date IS NULL THEN 'ART Not Started'
           ELSE COALESCE(ict.ict_status, 'Not Screened')
           END                                                                                                       as ict_screening_status_dqi,

       CASE
           WHEN ma.art_start_date IS NULL THEN 'ART Not Started'
           ELSE COALESCE(ncd.baseline_diagnosis, CASE
                                                     WHEN ncd.client_id IS NOT NULL THEN 'Screened'
                                                     ELSE 'Not Screened' END)
           END                                                                                                       as ncd_screening_status_dqi,

       CASE cxca.cxca_screening_status
           WHEN 'Confirmed CXCA (RED)' THEN 'Confirmed Cervical Cancer (Red)'
           WHEN 'Eligible (Yellow)' THEN 'Eligible for Cervical Cancer Screening/RX (Yellow)'
           ELSE COALESCE(cxca.cxca_screening_status, 'Not Applicable (Blue)')
           END                                                                                                       as cxca_screening_status_dqi

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
         LEFT JOIN Latest_TPT_Contraindication tpt_ci ON c.client_id = tpt_ci.client_id;
-- $END
