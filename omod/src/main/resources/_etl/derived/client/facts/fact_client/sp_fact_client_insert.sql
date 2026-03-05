-- $BEGIN
INSERT INTO mamba_fact_client
(client_id, patient_uuid, mrn, patient_name, sex, birthdate, age, uan, phrh_code, ncd_code, icd_number, ict_number,
 registration_date, art_start_date, months_on_art, next_appointment_date, hiv_confirmed_date, transfer_in_date,
 region, zone, woreda, kebele, house_number, mobile_phone, address_completeness, current_status,
 current_regimen, regimen_dose, regimen_line, tx_curr_end_date, nutritional_status, pregnancy_status,
 pmtct_status, family_planning_method, who_stage, last_visit_date, days_overdue, last_vl_date, last_vl_result,
 is_suppressed, vl_status, vl_eligibility_date, tpt_status, tpt_start_date, tpt_completed_date,
 tpt_discontinued_date, active_tb_diagnosis_date, tb_treatment_start_date, tb_treatment_discontinued_date,
 tb_treatment_completed_date, dsd_category, ict_screening_status, ncd_screening_status, next_ncd_screening_date,
 cxca_screening_status, next_cca_screening_date, systolic_blood_pressure, diastolic_blood_pressure, target_population)
WITH FollowUp AS (SELECT follow_up.client_id,
                         follow_up.encounter_id,
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
                         weight_text_                                                   as weight,
                         date_of_event,
                         transferred_in_check_this_for_all_t                            as transfer_in,
                         art_antiretroviral_start_date                                  as art_start_date,
                         date_of_reported_hiv_viral_load,
                         date_viral_load_results_received,

                         cervical_cancer_screening_status                               as cx_ca_screening_status,
                         cervical_cancer_screening_method_strategy                      as screening_method,
                         via_screening_result,
                         hpv_dna_screening_result,
                         cytology_result,
                         date_visual_inspection_of_the_cervi                            as via_date,
                         hpv_dna_result_received_date                                   as hpv_received_date,
                         date_cytology_result_received                                  as cytology_received_date,

                         eligible_for_cxca_screening,
                         reason_for_not_being_eligible,
                         other_reason_for_not_being_eligible_for_cxca,
                         treatment_start_date                                           as ccs_treat_received_date,
                         colposcopy_of_cervix_findings                                  as colposcopy_exam_finding,
                         biopsy_result_received_date,
                         biopsy_result,
                         date_patient_referred_out,
                         next_follow_up_screening_date,

                         COALESCE(at_3436_weeks_of_gestation,
                                  viral_load_after_eac_confirmatory_viral_load_where_initial_v,
                                  viral_load_after_eac_repeat_viral_load_where_initial_viral_l,
                                  every_six_months_until_mtct_ends,
                                  six_months_after_the_first_viral_load_test_at_postnatal_peri,
                                  three_months_after_delivery, at_the_first_antenatal_care_visit,
                                  annual_viral_load_test,
                                  second_viral_load_test_at_12_months_post_art,
                                  first_viral_load_test_at_6_months_or_longer_post_art,
                                  first_viral_load_test_at_3_months_or_longer_post_art) AS routine_viral_load_test_indication,
                         COALESCE(repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                                  suspected_antiretroviral_failure)                     AS targeted_viral_load_test_indication,
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
                  FROM mamba_flat_encounter_follow_up follow_up
                           LEFT JOIN mamba_flat_encounter_follow_up_1 USING (encounter_id)
                           LEFT JOIN mamba_flat_encounter_follow_up_2 USING (encounter_id)
                           LEFT JOIN mamba_flat_encounter_follow_up_3 USING (encounter_id)
                           LEFT JOIN mamba_flat_encounter_follow_up_4 USING (encounter_id)
                           LEFT JOIN mamba_flat_encounter_follow_up_5 USING (encounter_id)
                           LEFT JOIN mamba_flat_encounter_follow_up_6 USING (encounter_id)
                           LEFT JOIN mamba_flat_encounter_follow_up_7 USING (encounter_id)
                           LEFT JOIN mamba_flat_encounter_follow_up_8 USING (encounter_id)
                           LEFT JOIN mamba_flat_encounter_follow_up_9 USING (encounter_id)
                           LEFT JOIN mamba_flat_encounter_follow_up_10 USING (encounter_id)),

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
                                           ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY screening_date DESC) as rn
                                    FROM mamba_flat_encounter_ncd_screening
                                    WHERE screening_date IS NOT NULL) ranked
                              WHERE rn = 1),

     NCD_FollowUp_Events AS (SELECT *
                             FROM (SELECT f.client_id,
                                          return_visit_date                                                                              as next_screening_date,
                                          systolic_blood_pressure,
                                          diastolic_blood_pressure,
                                          ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY follow_up_date DESC, f.encounter_id DESC) as rn
                                   FROM mamba_flat_encounter_ncd_followup f
                                            LEFT JOIN mamba_flat_encounter_ncd_followup_1 f1
                                                      on f.encounter_id = f1.encounter_id
                                   WHERE return_visit_date IS NOT NULL
                                      OR systolic_blood_pressure IS NOT NULL
                                      OR diastolic_blood_pressure IS NOT NULL) ranked
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

       COALESCE(phrh.target_population, c.key_population, '-')                                                       as target_population

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
         LEFT JOIN FamilyPlanning fp ON c.client_id = fp.client_id;
-- $END
