-- $BEGIN
INSERT INTO mamba_fact_client
(client_id,
 patient_uuid,
 mrn,
 patient_name,
 sex,
 birthdate,
 age,
 uan,
 phrh_code,
 ncd_code,
 icd_number,
 registration_date,
 hiv_confirmed_date,
 transfer_in_date,
 months_on_art,
 region,
 zone,
 woreda,
 kebele,
 house_number,
 mobile_phone,
 address_completeness,
 art_start_date,
 current_status,
 current_regimen,
 regimen_dose,
 regimen_line,
 tx_curr_end_date,
 nutritional_status,
 pregnancy_status,
 pmtct_status,
 family_planning_method,
 last_visit_date,
 next_appointment_date,
 days_overdue,
 last_vl_date,
 last_vl_result,
 is_suppressed,
 vl_status,
 vl_eligibility_date,
 tpt_status,
 active_tb_diagnosis_date,
 tb_treatment_start_date,
 tb_treatment_discontinued_date,
 tb_treatment_completed_date,
 dsd_category,
 ict_screening_status,
 ncd_screening_status,
 cxca_screening_status,
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
                        hiv_viral_load                      AS viral_load_count,
                        pregnancy_status,
                        currently_breastfeeding_child,
                        method_of_family_planning,
                        dsd_category,
                        weight_text_                        as weight,
                        date_of_event,
                        transferred_in_check_this_for_all_t as transfer_in,
                        art_antiretroviral_start_date       as art_start_date,
                        date_of_reported_hiv_viral_load,
                        date_viral_load_results_received,
                        COALESCE(
                                at_3436_weeks_of_gestation,
                                viral_load_after_eac_confirmatory_viral_load_where_initial_v,
                                viral_load_after_eac_repeat_viral_load_where_initial_viral_l,
                                every_six_months_until_mtct_ends,
                                six_months_after_the_first_viral_load_test_at_postnatal_peri,
                                three_months_after_delivery,
                                at_the_first_antenatal_care_visit,
                                annual_viral_load_test,
                                second_viral_load_test_at_12_months_post_art,
                                first_viral_load_test_at_6_months_or_longer_post_art,
                                first_viral_load_test_at_3_months_or_longer_post_art
                        )                                   AS routine_viral_load_test_indication,
                        COALESCE(repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                                 suspected_antiretroviral_failure
                        )                                   AS targeted_viral_load_test_indication,
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

    -- 1. Get Latest Clinical Follow-up with all flattened columns
    LatestFollowUp AS (SELECT client_id                                                                                            as client_id,
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

    -- 2. Latest Lab (Viral Load) - Need Sent and Received dates for Logic

    Vl_Events AS (SELECT encounter_id,
                         client_id,
                         follow_up_date_followup_,
                         date_of_reported_hiv_viral_load  as viral_load_sent_date,
                         date_viral_load_results_received as viral_load_perform_date,
                         regimen_change,
                         routine_viral_load_test_indication,
                         follow_up_date_followup_         as FollowupDate,
                         currently_breastfeeding_child    as breastfeeding_status,
                         viral_load_test_status,
                         viral_load_count,

                         -- Rank VL Sent: Prioritize rows with Sent Date, then latest date
                         ROW_NUMBER() OVER (
                             PARTITION BY client_id
                             ORDER BY
                                 (date_of_reported_hiv_viral_load IS NOT NULL) DESC,
                                 date_of_reported_hiv_viral_load DESC,
                                 encounter_id DESC
                             )                            AS rn_sent,

                         -- Rank Switch: Prioritize rows with Regimen Change, then latest date
                         ROW_NUMBER() OVER (
                             PARTITION BY client_id
                             ORDER BY
                                 (regimen_change IS NOT NULL) DESC,
                                 follow_up_date_followup_ DESC,
                                 encounter_id DESC
                             )                            AS rn_switch,

                         -- Rank VL Performed: Prioritize rows with Performed Date, then latest date
                         ROW_NUMBER() OVER (
                             PARTITION BY client_id
                             ORDER BY
                                 (date_viral_load_results_received IS NOT NULL) DESC,
                                 date_viral_load_results_received DESC,
                                 encounter_id DESC
                             )                            AS rn_perf

                  FROM FollowUp),

    vl_performed AS (SELECT vl_perf.encounter_id,
                            vl_perf.client_id,
                            vl_sent.viral_load_sent_date AS VL_Sent_Date,

                            -- Logic comparing Performed vs Sent Date
                            CASE
                                WHEN vl_perf.viral_load_perform_date < vl_sent.viral_load_sent_date THEN NULL
                                ELSE vl_perf.viral_load_perform_date
                                END                      AS viral_load_perform_date,

                            CASE
                                WHEN vl_perf.viral_load_perform_date < vl_sent.viral_load_sent_date THEN NULL
                                ELSE vl_perf.viral_load_test_status
                                END                      AS viral_load_status,

                            CASE
                                WHEN vl_perf.viral_load_count > 0
                                    AND vl_perf.viral_load_perform_date >= vl_sent.viral_load_sent_date
                                    THEN CAST(vl_perf.viral_load_count AS DECIMAL(12, 2))
                                ELSE NULL
                                END                      AS viral_load_count,

                            -- Inferred Status Logic
                            CASE
                                WHEN vl_perf.viral_load_test_status IS NULL
                                    AND vl_perf.viral_load_perform_date >= vl_sent.viral_load_sent_date
                                    THEN NULL
                                WHEN vl_perf.viral_load_perform_date >= vl_sent.viral_load_sent_date
                                    AND (vl_perf.viral_load_test_status LIKE 'Det%'
                                        OR vl_perf.viral_load_test_status LIKE 'Uns%'
                                        OR vl_perf.viral_load_test_status LIKE 'High VL%'
                                        OR vl_perf.viral_load_test_status LIKE 'Low Level Viremia%')
                                    THEN 'U'
                                WHEN vl_perf.viral_load_perform_date >= vl_sent.viral_load_sent_date
                                    AND (vl_perf.viral_load_test_status LIKE 'Su%'
                                        OR vl_perf.viral_load_test_status LIKE 'Undet%')
                                    THEN 'S'
                                WHEN vl_perf.viral_load_perform_date >= vl_sent.viral_load_sent_date
                                    AND (ISNULL(vl_perf.viral_load_count) > CAST(50 AS float))
                                    THEN 'U'
                                WHEN vl_perf.viral_load_perform_date >= vl_sent.viral_load_sent_date
                                    AND (ISNULL(vl_perf.viral_load_count) <= CAST(50 AS float))
                                    THEN 'S'
                                ELSE NULL
                                END                      AS viral_load_status_inferred,

                            CASE
                                WHEN vl_sent.viral_load_sent_date IS NOT NULL THEN vl_sent.viral_load_sent_date
                                WHEN vl_perf.viral_load_perform_date IS NOT NULL THEN vl_perf.viral_load_perform_date
                                ELSE NULL
                                END                      AS viral_load_ref_date,

                            vl_perf.routine_viral_load_test_indication

                     FROM Vl_Events vl_perf
                              LEFT JOIN Vl_Events vl_sent
                                        ON vl_perf.client_id = vl_sent.client_id
                                            AND vl_sent.rn_sent = 1
                                            AND vl_sent.viral_load_sent_date IS NOT NULL
                     WHERE vl_perf.rn_perf = 1),
    vl_eligibility as (SELECT f_case.art_start_date                               as art_start_date,
                              f_case.currently_breastfeeding_child                as BreastFeeding,
                              f_case.hiv_confirmed_date                           as date_hiv_confirmed,
                              sub_switch_date.FollowupDate                        as date_regimen_change,
                              f_case.follow_up_status                             as follow_up_status,
                              f_case.visit_date                                   as FollowUpDate,
                              f_case.pregnancy_status                             as IsPregnant,
                              f_case.client_id                                    as PatientId,
                              vlperfdate.viral_load_count                         as viral_load_count,
                              vlperfdate.viral_load_perform_date                  as viral_load_perform_date,
                              vlsentdate.viral_load_sent_date                     as viral_load_sent_date,
                              vlperfdate.viral_load_status                        as viral_load_status,
                              f_case.weight,
                              regimen_dose,
                              f_case.regimen,
                              f_case.next_visit_date,
                              f_case.treatment_end_date,
                              CASE
                                  WHEN vlsentdate.viral_load_sent_date IS NOT NULL
                                      THEN vlsentdate.viral_load_sent_date
                                  WHEN vlperfdate.viral_load_perform_date IS NOT NULL
                                      THEN vlperfdate.viral_load_perform_date
                                  ELSE NULL END                                   AS viral_load_ref_date,
                              sub_switch_date.FollowupDate                        as switchDate,
                              vlperfdate.viral_load_status_inferred,

                              vlperfdate.routine_viral_load_test_indication       as viral_load_indication,

                              CASE

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NULL
                                          AND f_case.follow_up_status = 'Restart medication')
                                      THEN DATE_ADD(f_case.visit_date, INTERVAL 91 DAY)

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NULL
                                          AND sub_switch_date.FollowupDate IS NOt NULL
                                          )
                                      THEN DATE_ADD(sub_switch_date.FollowupDate, INTERVAL 181 DAY)

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NULL
                                          AND f_case.pregnancy_status = 'Yes'
                                          AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                            CURDATE()) > 90)
                                      THEN DATE_ADD(f_case.art_start_date, INTERVAL 91 DAY)

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NULL
                                          AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                            CURDATE()) <= 180)
                                      THEN NULL


                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NULL
                                          AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                            CURDATE()) > 180)
                                      THEN DATE_ADD(f_case.art_start_date, INTERVAL 181 DAY)

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NOT NULL
                                          AND vlperfdate.VL_Sent_Date < f_case.visit_date)
                                          AND (f_case.follow_up_status = 'Restart medication')
                                      THEN DATE_ADD(f_case.visit_date, INTERVAL 91 DAY)

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NOT NULL
                                          AND vlperfdate.VL_Sent_Date < sub_switch_date.FollowupDate
                                          AND sub_switch_date.FollowupDate IS NOT NULL
                                          )
                                      THEN DATE_ADD(sub_switch_date.FollowupDate, INTERVAL 181 DAY)

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NOT NULL
                                          AND vlperfdate.viral_load_status_inferred = 'U')
                                      THEN DATE_ADD(vlperfdate.VL_Sent_Date, INTERVAL 91 DAY)

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NOT NULL
                                          AND
                                       (f_case.pregnancy_status = 'Yes' OR f_case.currently_breastfeeding_child = 'Yes')
                                          AND vlperfdate.routine_viral_load_test_indication in
                                              ('First viral load test at 6 months or longer post ART',
                                               'Viral load after EAC: repeat viral load where initial viral load greater than 50 and less than 1000 copies per ml',
                                               'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml'))
                                      THEN DATE_ADD(vlperfdate.VL_Sent_Date, INTERVAL 91 DAY)

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NOT NULL
                                          AND
                                       (f_case.pregnancy_status = 'Yes' OR f_case.currently_breastfeeding_child = 'Yes')
                                          AND vlperfdate.routine_viral_load_test_indication IS NOT NULL
                                          AND vlperfdate.routine_viral_load_test_indication not in
                                              ('First viral load test at 6 months or longer post ART',
                                               'Viral load after EAC: repeat viral load where initial viral load greater than 50 and less than 1000 copies per ml',
                                               'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml'))
                                      THEN DATE_ADD(vlperfdate.VL_Sent_Date, INTERVAL 181 DAY)


                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NOT NULL)
                                      THEN DATE_ADD(vlperfdate.VL_Sent_Date, INTERVAL 365 DAY)

                                  ELSE DATE_ADD(CURDATE(), INTERVAL 100 YEAR) End AS eligiblityDate,

                              CASE

                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NULL
                                          AND f_case.follow_up_status = 'Restart medication')
                                      THEN DATE_ADD(f_case.visit_date, INTERVAL 91 DAY)

                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NULL
                                          AND sub_switch_date.FollowupDate IS NOt NULL
                                          )
                                      THEN DATE_ADD(sub_switch_date.FollowupDate, INTERVAL 181 DAY)

                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NULL
                                          AND f_case.pregnancy_status = 'Yes'
                                          AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                            CURDATE()) > 90)
                                      THEN DATE_ADD(f_case.art_start_date, INTERVAL 91 DAY)

                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NULL
                                          AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                            CURDATE()) <= 180)
                                      THEN NULL


                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NULL
                                          AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                            CURDATE()) > 180)
                                      THEN DATE_ADD(f_case.art_start_date, INTERVAL 181 DAY)

                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NOT NULL
                                          AND vlperfdate.viral_load_perform_date < f_case.visit_date)
                                          AND (f_case.follow_up_status = 'Restart medication')
                                      THEN DATE_ADD(f_case.visit_date, INTERVAL 91 DAY)

                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NOT NULL
                                          AND vlperfdate.viral_load_perform_date < sub_switch_date.FollowupDate
                                          AND sub_switch_date.FollowupDate IS NOT NULL
                                          )
                                      THEN DATE_ADD(sub_switch_date.FollowupDate, INTERVAL 181 DAY)

                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NOT NULL
                                          AND vlperfdate.viral_load_status_inferred = 'U')
                                      THEN DATE_ADD(vlperfdate.viral_load_perform_date, INTERVAL 91 DAY)

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NOT NULL
                                          AND
                                       (f_case.pregnancy_status = 'Yes' OR f_case.currently_breastfeeding_child = 'Yes')
                                          AND vlperfdate.routine_viral_load_test_indication in
                                              ('First viral load test at 6 months or longer post ART',
                                               'Viral load after EAC: repeat viral load where initial viral load greater than 50 and less than 1000 copies per ml',
                                               'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml'))
                                      THEN DATE_ADD(vlperfdate.viral_load_perform_date, INTERVAL 91 DAY)

                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NOT NULL
                                          AND
                                       (f_case.pregnancy_status = 'Yes' OR f_case.currently_breastfeeding_child = 'Yes')
                                          AND vlperfdate.routine_viral_load_test_indication IS NOT NULL
                                          AND vlperfdate.routine_viral_load_test_indication not in
                                              ('First viral load test at 6 months or longer post ART',
                                               'Viral load after EAC: repeat viral load where initial viral load greater than 50 and less than 1000 copies per ml',
                                               'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml'))
                                      THEN DATE_ADD(vlperfdate.viral_load_perform_date, INTERVAL 181 DAY)


                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NOT NULL)
                                      THEN DATE_ADD(vlperfdate.viral_load_perform_date, INTERVAL 365 DAY)

                                  ELSE DATE_ADD(CURDATE(), INTERVAL 100 YEAR) End AS eligiblityDateReceived,


                              CASE

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NULL
                                          AND f_case.follow_up_status = 'Restart medication')
                                      THEN 'client restarted ART'

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NULL
                                          AND sub_switch_date.FollowupDate IS NOt NULL
                                          )
                                      THEN 'Regimen Change'


                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NULL
                                          AND f_case.pregnancy_status = 'Yes'
                                          AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                            CURDATE()) > 90)
                                      THEN 'First VL for Pregnant'

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NULL
                                          AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                            CURDATE()) <= 180)
                                      THEN 'N/A'

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NULL
                                          AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                            CURDATE()) > 180)
                                      THEN 'First VL'


                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NOT NULL
                                          AND vlperfdate.VL_Sent_Date < f_case.visit_date)
                                          AND (f_case.follow_up_status = 'Restart medication')
                                      THEN 'client restarted ART'

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NOT NULL
                                          AND vlperfdate.VL_Sent_Date < sub_switch_date.FollowupDate
                                          AND sub_switch_date.FollowupDate IS NOT NULL
                                          )
                                      THEN 'Regimen Change'

                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NOT NULL
                                          AND vlperfdate.viral_load_status_inferred = 'U')
                                      THEN 'Repeat/Confirmatory Viral Load test'

                                  WHEN
                                      (vlperfdate.viral_load_status_inferred IS NOT NULL
                                          AND
                                       (f_case.pregnancy_status = 'Yes' OR
                                        f_case.currently_breastfeeding_child = 'Yes'))
                                      THEN 'Pregnant/Breastfeeding and needs retesting'


                                  WHEN
                                      (vlperfdate.VL_Sent_Date IS NOT NULL)
                                      THEN 'Annual Viral Load Test'

                                  ELSE 'Unassigned' End                           AS vl_status_final,
                              CASE

                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NULL
                                          AND f_case.follow_up_status = 'Restart medication')
                                      THEN 'client restarted ART'

                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NULL
                                          AND sub_switch_date.FollowupDate IS NOt NULL
                                          )
                                      THEN 'Regimen Change'


                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NULL
                                          AND f_case.pregnancy_status = 'Yes'
                                          AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                            CURDATE()) > 90)
                                      THEN 'First VL for Pregnant'

                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NULL
                                          AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                            CURDATE()) <= 180)
                                      THEN 'N/A'

                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NULL
                                          AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                            CURDATE()) > 180)
                                      THEN 'First VL'


                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NOT NULL
                                          AND vlperfdate.viral_load_perform_date < f_case.visit_date)
                                          AND (f_case.follow_up_status = 'Restart medication')
                                      THEN 'client restarted ART'

                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NOT NULL
                                          AND vlperfdate.viral_load_perform_date < sub_switch_date.FollowupDate
                                          AND sub_switch_date.FollowupDate IS NOT NULL
                                          )
                                      THEN 'Regimen Change'

                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NOT NULL
                                          AND vlperfdate.viral_load_status_inferred = 'U')
                                      THEN 'Repeat/Confirmatory Viral Load test'

                                  WHEN
                                      (vlperfdate.viral_load_status_inferred IS NOT NULL
                                          AND
                                       (f_case.pregnancy_status = 'Yes' OR
                                        f_case.currently_breastfeeding_child = 'Yes'))
                                      THEN 'Pregnant/Breastfeeding and needs retesting'


                                  WHEN
                                      (vlperfdate.viral_load_perform_date IS NOT NULL)
                                      THEN 'Annual Viral Load Test'

                                  ELSE 'Unassigned' End                           AS VL_STATUS_COVERAGE

                       FROM LatestFollowUp AS f_case
                                LEFT JOIN vl_performed as vlperfdate
                                          ON vlperfdate.client_id = f_case.client_id
                                LEFT JOIN Vl_Events as vlsentdate
                                          ON vlsentdate.client_id = f_case.client_id
                                              AND vlsentdate.rn_sent = 1
                                              AND vlsentdate.viral_load_sent_date IS NOT NULL
                                LEFT JOIN Vl_Events as sub_switch_date
                                          ON sub_switch_date.client_id = f_case.client_id
                                              AND sub_switch_date.rn_switch = 1
                                              AND sub_switch_date.regimen_change IS NOT NULL),

    -- 3. ART Start Date
    ARTStart AS (SELECT client_id as client_id, MIN(art_start_date) as start_date
                 FROM FollowUp
                 WHERE art_start_date IS NOT NULL
                 GROUP BY client_id),

    -- 4. Identifiers (PHRH, NCD)
    Identifiers AS (SELECT patient_id,
                           MAX(CASE WHEN pit.name = 'PHRH' THEN pi.identifier END) as phrh_code,
                           MAX(CASE WHEN pit.name = 'NCD' THEN pi.identifier END)  as ncd_code,
                           MAX(CASE WHEN pit.name = 'ICD' THEN pi.identifier END)  as icd_number
                    FROM mamba_dim_patient_identifier pi
                             JOIN mamba_dim_patient_identifier_type pit
                                  ON pi.identifier_type = pit.patient_identifier_type_id
                    GROUP BY patient_id),

    -- TPT Logic (Simplified from Provider View for brevity, but capturing key statuses)
    TPT_Events AS (SELECT client_id                                as client_id,
                          MAX(date_started_on_tuberculosis_prophy) as tpt_start_date,
                          MAX(date_completed_tuberculosis_prophyl) as tpt_completed_date,
                          MAX(date_discontinued_tuberculosis_prop) as tpt_discontinued_date,
                          MAX(CASE
                                  WHEN reason_not_eligible_for_tuberculosi = 'Contraindication' THEN 1
                                  ELSE 0 END)                      as is_contraindicated
                   FROM FollowUp
                   GROUP BY client_id),

    -- TB Treatment Dates
    TB_Events AS (SELECT client_id                                as client_id,
                         MAX(diagnosis_date)                      as active_tb_diagnosis_date,
                         MAX(tuberculosis_drug_treatment_start_d) as tb_treatment_start_date,
                         MAX(date_active_tbrx_completed)          as tb_treatment_completed_date,
                         MAX(date_discontinued_tuberculosis_prop) as tb_treatment_discontinued_date
                  FROM FollowUp
                  GROUP BY client_id),

-- ICT Screening Logic
    ICT_Events AS (SELECT client_id,
                          CASE
                              WHEN accepted = 'Yes' THEN 'Accepted'
                              WHEN accepted = 'No' THEN 'Refused'
                              ELSE 'Offered'
                              END                                                               as ict_status,
                          ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY offered_date DESC) as rn
                   FROM mamba_flat_encounter_ict_offer
                   WHERE offered_date IS NOT NULL)
        ,
    NCD_Screening_Events AS (SELECT client_id,
                                    baseline_diagnosis,
                                    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY screening_date DESC) as rn
                             FROM mamba_flat_encounter_ncd_screening
                             WHERE screening_date IS NOT NULL)


SELECT c.client_id,
       c.patient_uuid,
       c.mrn,
       c.patient_name,
       c.sex,
       c.date_of_birth,
       TIMESTAMPDIFF(YEAR, c.date_of_birth, CURDATE())               as age,

       -- Identifiers
       c.uan,
       id.phrh_code,
       id.ncd_code,
       id.icd_number,

       -- Registration
       NULL                                                          as registration_date,
       lfu.hiv_confirmed_date,
       NULL                                                          as transfer_in_date,
       TIMESTAMPDIFF(MONTH, art.start_date, CURDATE())               as months_on_art,

       -- Address & Completeness
       c.state_province                                              as region,
       c.county_district                                             as zone,
       c.city_village                                                as woreda,
       c.kebele,
       c.house_number,
       c.mobile_no                                                   as mobile_phone,
       CASE
           WHEN (c.patient_name IS NOT NULL AND c.state_province IS NOT NULL AND c.mobile_no IS NOT NULL) THEN 'GREEN'
           ELSE 'YELLOW'
           END                                                       as address_completeness,

       -- ART Clinical
       art.start_date,

       -- Status Logic
       CASE
           WHEN lfu.follow_up_status IN ('Dead', 'Died') THEN 'Dead'
           WHEN lfu.follow_up_status IN ('Transferred out', 'TO') THEN 'Transferred Out'
           WHEN lfu.treatment_end_date >= CURDATE() THEN 'Active'
           WHEN lfu.visit_date IS NULL THEN 'No Clinical Contact'
           ELSE lfu.follow_up_status
           END
                                                                     as current_status,

       lfu.regimen                                                   as current_regimen,
       lfu.regimen_dose,

       -- Regimen Line
       CASE
           WHEN lfu.regimen LIKE '1%' THEN 'First Line'
           WHEN lfu.regimen LIKE '2%' THEN 'Second Line'
           ELSE 'Other'
           END
                                                                     as regimen_line,

       lfu.treatment_end_date                                        as tx_curr_end_date,
       lfu.nutritional_status_of_adult                               as nutritional_status,
       lfu.pregnancy_status,
       CASE
           WHEN (lfu.pregnancy_status = 'Yes' OR lfu.currently_breastfeeding_child = 'Yes') THEN 'On PMTCT'
           ELSE 'Not Applicable'
           END                                                       as pmtct_status,
       lfu.method_of_family_planning,

       lfu.visit_date                                                as last_visit_date,
       lfu.next_visit_date,
       DATEDIFF(CURDATE(),
                COALESCE(lfu.next_visit_date, lfu.visit_date))       as days_overdue,

       -- Viral Load
       lab.viral_load_perform_date                                   as last_vl_date,
       lab.viral_load_indication                                     as last_vl_result,
       lab.viral_load_status_inferred                                as is_suppressed,

       lab.vl_status_final                                           as vl_status,

       lab.eligiblityDate                                            as vl_eligibility_date,

       CASE
           WHEN tpt.tpt_completed_date IS NOT NULL THEN 'Gold (TPT Completed)'
           WHEN tpt.tpt_start_date IS NOT NULL AND tpt.tpt_completed_date IS NULL AND tpt.tpt_discontinued_date IS NULL
               THEN 'On TPT'
           WHEN tpt.is_contraindicated = 1 THEN 'Contraindicated'
           ELSE 'Not Started'
           END
                                                                     as tpt_status,

       -- TB Treatment
       tb.active_tb_diagnosis_date,
       tb.tb_treatment_start_date,
       tb.tb_treatment_discontinued_date,
       tb.tb_treatment_completed_date,

       lfu.dsd_category,
       COALESCE(ict.ict_status, 'Not Screened')                      as ict_screening_status,

       COALESCE(ncd.baseline_diagnosis, CASE
                                            WHEN ncd.client_id IS NOT NULL THEN 'Screened'
                                            ELSE 'Not Screened' END) as ncd_screening_status,
       'Not Screened'                                                as cxca_screening_status,
       c.key_population                                              as target_population

FROM mamba_dim_client c
         LEFT JOIN LatestFollowUp lfu ON c.client_id = lfu.client_id AND lfu.rn = 1
         LEFT JOIN vl_eligibility lab ON c.client_id = lab.PatientId
         LEFT JOIN ARTStart art ON c.client_id = art.client_id
         LEFT JOIN Identifiers id ON c.client_id = id.patient_id
         LEFT JOIN TPT_Events tpt ON c.client_id = tpt.client_id
         LEFT JOIN TB_Events tb ON c.client_id = tb.client_id
         LEFT JOIN ICT_Events ict ON c.client_id = ict.client_id AND ict.rn = 1
         LEFT JOIN NCD_Screening_Events ncd ON c.client_id = ncd.client_id AND ncd.rn = 1;
-- $END
