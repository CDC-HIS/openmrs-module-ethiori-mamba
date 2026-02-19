-- $BEGIN
INSERT INTO mamba_fact_client
(client_id, patient_uuid, mrn, patient_name, sex, birthdate, age, uan, phrh_code, ncd_code, icd_number,
 registration_date, hiv_confirmed_date, transfer_in_date, months_on_art, region, zone, woreda, kebele, house_number,
 mobile_phone, address_completeness, art_start_date, current_status, current_regimen, regimen_dose, regimen_line,
 tx_curr_end_date, nutritional_status, pregnancy_status, pmtct_status, family_planning_method, last_visit_date,
 next_appointment_date, days_overdue, last_vl_date, last_vl_result, is_suppressed, vl_status, vl_eligibility_date,
 tpt_status, active_tb_diagnosis_date, tb_treatment_start_date, tb_treatment_discontinued_date,
 tb_treatment_completed_date, dsd_category, ict_screening_status, ncd_screening_status, cxca_screening_status,
 target_population)
WITH
    -- 1. Get Latest Clinical Follow-up with all flattened columns
    FollowUp AS (SELECT follow_up.client_id,
                        follow_up.encounter_id,
                        follow_up_date_followup_,
                        next_visit_date,
                        follow_up_status,
                        regimen,
                        antiretroviral_art_dispensed_dose_i,
                        treatment_end_date,
                        nutritional_status_of_adult,
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

                        COALESCE(at_3436_weeks_of_gestation,
                                 viral_load_after_eac_confirmatory_viral_load_where_initial_v,
                                 viral_load_after_eac_repeat_viral_load_where_initial_viral_l,
                                 every_six_months_until_mtct_ends,
                                 six_months_after_the_first_viral_load_test_at_postnatal_peri,
                                 three_months_after_delivery, at_the_first_antenatal_care_visit, annual_viral_load_test,
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
                        date_active_tbrx_completed
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

    -- 2. Isolate the absolute latest follow-up for baseline stats
    LatestFollowUp AS (SELECT client_id,
                              follow_up_date_followup_                                                                             as visit_date,
                              next_visit_date,
                              follow_up_status,
                              regimen,
                              antiretroviral_art_dispensed_dose_i                                                                  as regimen_dose,
                              treatment_end_date,
                              nutritional_status_of_adult,
                              pregnancy_status,
                              art_start_date,
                              currently_breastfeeding_child,
                              weight,
                              method_of_family_planning,
                              dsd_category,
                              date_of_event                                                                                        as hiv_confirmed_date,
                              transfer_in,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) as rn
                       FROM FollowUp
                       WHERE follow_up_date_followup_ IS NOT NULL),

    -- 3. Rank Viral Load Events (Sent, Switch, Performed)
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

    -- 4. Reconcile Performed vs Sent dates and infer VL status
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
                                WHEN vl_perf.viral_load_count > 0 AND
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

    -- 5. Centralize Clinical Scenario calculation
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

    -- 6. Evaluate final VL eligibility dates
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

    -- 7. ART Start Date
    ARTStart AS (SELECT client_id, MIN(art_start_date) as start_date
                 FROM FollowUp
                 WHERE art_start_date IS NOT NULL
                 GROUP BY client_id),

    -- 8. Identifiers
    Identifiers AS (SELECT patient_id,
                           MAX(CASE WHEN pit.name = 'PHRH' THEN pi.identifier END) as phrh_code,
                           MAX(CASE WHEN pit.name = 'NCD' THEN pi.identifier END)  as ncd_code,
                           MAX(CASE WHEN pit.name = 'ICD' THEN pi.identifier END)  as icd_number
                    FROM mamba_dim_patient_identifier pi
                             JOIN mamba_dim_patient_identifier_type pit
                                  ON pi.identifier_type = pit.patient_identifier_type_id
                    GROUP BY patient_id),

    -- 9. TPT Logic
    TPT_Events AS (SELECT client_id,
                          MAX(date_started_on_tuberculosis_prophy)                                                  as tpt_start_date,
                          MAX(date_completed_tuberculosis_prophyl)                                                  as tpt_completed_date,
                          MAX(date_discontinued_tuberculosis_prop)                                                  as tpt_discontinued_date,
                          MAX(CASE
                                  WHEN reason_not_eligible_for_tuberculosi = 'Contraindication' THEN 1
                                  ELSE 0 END)                                                                       as is_contraindicated
                   FROM FollowUp
                   GROUP BY client_id),

    -- 10. TB Treatment Dates
    TB_Events AS (SELECT client_id,
                         MAX(diagnosis_date)                      as active_tb_diagnosis_date,
                         MAX(tuberculosis_drug_treatment_start_d) as tb_treatment_start_date,
                         MAX(date_active_tbrx_completed)          as tb_treatment_completed_date,
                         MAX(date_discontinued_tuberculosis_prop) as tb_treatment_discontinued_date
                  FROM FollowUp
                  GROUP BY client_id),

    -- 11. ICT Screening Logic
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

    -- 12. NCD Screening Events
    NCD_Screening_Events AS (SELECT *
                             FROM (SELECT client_id,
                                          baseline_diagnosis,
                                          ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY screening_date DESC) as rn
                                   FROM mamba_flat_encounter_ncd_screening
                                   WHERE screening_date IS NOT NULL) ranked
                             WHERE rn = 1),

    -- 13. Cervical Cancer (CxCa) Screening Events
    CXCA_Events AS (SELECT *
                    FROM (SELECT client_id,
                                 CASE
                                     WHEN
                                         screening_method = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND
                                         via_screening_result = 'VIA negative' THEN 'Cervical Cancer screen: Negative'
                                     WHEN
                                         screening_method = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND
                                         via_screening_result IN ('VIA positive: eligible for cryo/thermo-coagulation',
                                                                  'VIA positive: eligible for cryo/thermo-coagula',
                                                                  'VIA positive: non-eligible for cryo/thermo-coagulation')
                                         THEN 'Cervical Cancer screen: Positive'
                                     WHEN
                                         screening_method = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND
                                         via_screening_result IN
                                         ('Suspicious for cervical cancer', 'suspected cervical cancer')
                                         THEN 'Cervical Cancer screen: Suspected Cancer'
                                     WHEN screening_method = 'Human Papillomavirus test' AND
                                          hpv_dna_screening_result = 'Negative result'
                                         THEN 'Cervical Cancer screen: Negative'
                                     WHEN screening_method = 'Human Papillomavirus test' AND
                                          hpv_dna_screening_result = 'Positive' AND via_screening_result IN
                                                                                    ('VIA positive: eligible for cryo/thermo-coagulation',
                                                                                     'VIA positive: eligible for cryo/thermo-coagula',
                                                                                     'VIA positive: non-eligible for cryo/thermo-coagulation')
                                         THEN 'Cervical Cancer screen: Positive'
                                     WHEN screening_method = 'Human Papillomavirus test' AND
                                          hpv_dna_screening_result = 'Positive' AND
                                          via_screening_result = 'VIA negative' THEN 'Cervical Cancer screen: Negative'
                                     WHEN screening_method = 'Cytology' AND cytology_result IN ('Negative', 'ASCUS')
                                         THEN 'Cervical Cancer screen: Negative'
                                     WHEN screening_method = 'Cytology' AND cytology_result = '> Ascus'
                                         THEN 'Cervical Cancer screen: Positive'
                                     ELSE 'Screened - Result Pending/Unknown'
                                     END                                                                                                        AS cxca_screening_status,
                                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY COALESCE(via_date,
                                                                                             hpv_received_date,
                                                                                             cytology_received_date,
                                                                                             follow_up_date_followup_) DESC, encounter_id DESC) as rn
                          FROM FollowUp
                          WHERE cx_ca_screening_status = 'Cervical cancer screening performed') ranked
                    WHERE rn = 1)


SELECT c.client_id,
       c.patient_uuid,
       c.mrn,
       c.patient_name,
       c.sex,
       c.date_of_birth,
       TIMESTAMPDIFF(YEAR, c.date_of_birth, CURDATE())                                                               as age,

       -- Identifiers
       c.uan,
       id.phrh_code,
       id.ncd_code,
       id.icd_number,

       -- Registration & Clinical Base
       NULL                                                                                                          as registration_date,
       lfu.hiv_confirmed_date,
       NULL                                                                                                          as transfer_in_date,
       TIMESTAMPDIFF(MONTH, art.start_date, CURDATE())                                                               as months_on_art,

       -- Address & Completeness
       c.state_province                                                                                              as region,
       c.county_district                                                                                             as zone,
       c.city_village                                                                                                as woreda,
       c.kebele,
       c.house_number,
       c.mobile_no                                                                                                   as mobile_phone,
       CASE
           WHEN (c.patient_name IS NOT NULL AND c.state_province IS NOT NULL AND c.mobile_no IS NOT NULL) THEN 'GREEN'
           ELSE 'YELLOW' END                                                                                         as address_completeness,

       -- ART Clinical
       art.start_date                                                                                                as art_start_date,

       -- Status Logic
       CASE
           WHEN lfu.follow_up_status IN ('Dead', 'Died') THEN 'Dead'
           WHEN lfu.follow_up_status IN ('Transferred out', 'TO') THEN 'Transferred Out'
           WHEN lfu.treatment_end_date >= CURDATE() THEN 'Active'
           WHEN lfu.visit_date IS NULL THEN 'No Clinical Contact'
           ELSE lfu.follow_up_status
           END                                                                                                       as current_status,

       lfu.regimen                                                                                                   as current_regimen,
       lfu.regimen_dose,

       -- Regimen Line
       CASE
           WHEN lfu.regimen LIKE '1%' THEN 'First Line'
           WHEN lfu.regimen LIKE '2%' THEN 'Second Line'
           ELSE 'Other' END                                                                                          as regimen_line,

       lfu.treatment_end_date                                                                                        as tx_curr_end_date,
       lfu.nutritional_status_of_adult                                                                               as nutritional_status,
       lfu.pregnancy_status,
       CASE
           WHEN (lfu.pregnancy_status = 'Yes' OR lfu.currently_breastfeeding_child = 'Yes') THEN 'On PMTCT'
           ELSE 'Not Applicable' END                                                                                 as pmtct_status,
       lfu.method_of_family_planning,

       lfu.visit_date                                                                                                as last_visit_date,
       lfu.next_visit_date,
       DATEDIFF(CURDATE(),
                COALESCE(lfu.next_visit_date, lfu.visit_date))                                                       as days_overdue,

       -- Viral Load
       lab.viral_load_perform_date                                                                                   as last_vl_date,
       lab.viral_load_count                                                                                          as last_vl_result,
       CASE lab.viral_load_status_inferred WHEN 'S' THEN 1 ELSE 0 END                                                as is_suppressed,

       CASE
           WHEN lab.vl_status_final = 'N/A' THEN 'Not Applicable'
           WHEN lab.eligiblityDate <= CURDATE() THEN 'Eligible for Viral Load'
           WHEN lab.eligiblityDate > CURDATE() THEN 'Viral Load Done (Currently not Eligible)'
           WHEN lab.art_start_date IS NULL AND lab.follow_up_status IS NULL THEN 'Not Started ART'
           END                                                                                                       as vl_status,

       lab.eligiblityDate                                                                                            as vl_eligibility_date,

       -- TPT Logic
       CASE
           WHEN tpt.tpt_completed_date IS NOT NULL THEN 'Gold (TPT Completed)'
           WHEN tpt.tpt_start_date IS NOT NULL AND tpt.tpt_completed_date IS NULL AND tpt.tpt_discontinued_date IS NULL
               THEN 'On TPT'
           WHEN tpt.is_contraindicated = 1 THEN 'Contraindicated'
           ELSE 'Not Started'
           END                                                                                                       as tpt_status,

       -- TB Treatment
       tb.active_tb_diagnosis_date,
       tb.tb_treatment_start_date,
       tb.tb_treatment_discontinued_date,
       tb.tb_treatment_completed_date,

       lfu.dsd_category,

       -- Screening Modules
       COALESCE(ict.ict_status, 'Not Screened')                                                                      as ict_screening_status,
       COALESCE(ncd.baseline_diagnosis, CASE
                                            WHEN ncd.client_id IS NOT NULL THEN 'Screened'
                                            ELSE 'Not Screened' END)                                                 as ncd_screening_status,

       -- CxCa Screening (With Age and Sex constraints)
       CASE
           WHEN c.sex IN ('F', 'Female') AND TIMESTAMPDIFF(YEAR, c.date_of_birth, CURDATE()) >= 15
               THEN COALESCE(cxca.cxca_screening_status, 'Not Screened')
           ELSE 'Not Applicable'
           END                                                                                                       as cxca_screening_status,

       c.key_population                                                                                              as target_population

FROM mamba_dim_client c
         LEFT JOIN LatestFollowUp lfu ON c.client_id = lfu.client_id
         LEFT JOIN vl_eligibility lab ON c.client_id = lab.client_id
         LEFT JOIN ARTStart art ON c.client_id = art.client_id
         LEFT JOIN Identifiers id ON c.client_id = id.patient_id
         LEFT JOIN TPT_Events tpt ON c.client_id = tpt.client_id
         LEFT JOIN TB_Events tb ON c.client_id = tb.client_id
         LEFT JOIN ICT_Events ict ON c.client_id = ict.client_id
         LEFT JOIN NCD_Screening_Events ncd ON c.client_id = ncd.client_id
         LEFT JOIN CXCA_Events cxca ON c.client_id = cxca.client_id;
-- $END
