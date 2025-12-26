-- $BEGIN
INSERT INTO mamba_fact_client
(client_id, patient_uuid, mrn,
 patient_name, sex, birthdate, age,
 uan, phrh_code, ncd_code, icd_number,
 registration_date, hiv_confirmed_date, transfer_in_date, months_on_art,
 region, zone, woreda, kebele, house_number, mobile_phone, address_completeness,
 art_start_date, current_status, current_regimen, regimen_dose, regimen_line,
 tx_curr_end_date, nutritional_status, pregnancy_status, pmtct_status, family_planning_method,
 last_visit_date, next_appointment_date, days_overdue,
 last_vl_date, last_vl_result, is_suppressed, vl_status, vl_eligibility_date,
 tpt_status,
 active_tb_diagnosis_date, tb_treatment_start_date, tb_treatment_discontinued_date, tb_treatment_completed_date,
 dsd_category, ict_screening_status, ncd_screening_status, cxca_screening_status, target_population)
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
                        pregnancy_status,
                        currently_breastfeeding_child,
                        method_of_family_planning,
                        dsd_category,
                        date_of_event,
                        transfer_in,
                        art_antiretroviral_start_date,
                        date_of_reported_hiv_viral_load,
                        date_viral_load_results_received,
                        hiv_viral_load,
                        viral_load_test_status,
                        date_started_on_tuberculosis_prophy,
                        date_completed_tuberculosis_prophyl,
                        date_discontinued_tuberculosis_prop,
                        reason_not_eligible_for_tuberculosi,
                        diagnosis_date,
                        tuberculosis_drug_treatment_start_d,
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
                          LEFT JOIN mamba_flat_encounter_follow_up_9 USING (encounter_id)),

    -- 1. Get Latest Clinical Follow-up with all flattened columns
    LatestFollowUp AS (SELECT client_id                                                                                              as client_id,
                              follow_up_date_followup_                                                                                 as visit_date,
                              next_visit_date,
                              follow_up_status,
                              regimen,
                              antiretroviral_art_dispensed_dose_i                                                                      as regimen_dose,
                              treatment_end_date,
                              nutritional_status_of_adult,
                              pregnancy_status,
                              currently_breastfeeding_child,
                              method_of_family_planning,
                              dsd_category,
                              date_of_event                                                                                            as hiv_confirmed_date,
                              transfer_in, -- Use as indicator for transfer in? Or need date
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) as rn
                       FROM FollowUp
                       WHERE follow_up_date_followup_ IS NOT NULL),

    -- 2. Latest Lab (Viral Load) - Need Sent and Received dates for Logic
    LatestVL AS (SELECT client_id                                                                                 as client_id,
                        date_of_reported_hiv_viral_load                                                             as vl_sent_date,
                        date_viral_load_results_received                                                            as vl_perform_date,
                        hiv_viral_load                                                                              as vl_result,
                        viral_load_test_status,
                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY date_of_reported_hiv_viral_load DESC)  as rn_sent,
                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY date_viral_load_results_received DESC) as rn_res
                 FROM FollowUp
                 WHERE date_of_reported_hiv_viral_load IS NOT NULL
                    OR date_viral_load_results_received IS NOT NULL),

    -- 3. ART Start Date
    ARTStart AS (SELECT client_id                        as client_id,
                        MIN(art_antiretroviral_start_date) as start_date
                 FROM FollowUp
                 WHERE art_antiretroviral_start_date IS NOT NULL
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
    TPT_Events AS (SELECT client_id                              as client_id,
                          MAX(date_started_on_tuberculosis_prophy) as tpt_start_date,
                          MAX(date_completed_tuberculosis_prophyl) as tpt_completed_date,
                          MAX(date_discontinued_tuberculosis_prop) as tpt_discontinued_date,
                          MAX(CASE
                                  WHEN reason_not_eligible_for_tuberculosi = 'Contraindication' THEN 1
                                  ELSE 0 END)                      as is_contraindicated
                   FROM FollowUp
                   GROUP BY client_id),

    -- TB Treatment Dates
    TB_Events AS (SELECT client_id                              as client_id,
                         MAX(diagnosis_date)                      as active_tb_diagnosis_date,
                         MAX(tuberculosis_drug_treatment_start_d) as tb_treatment_start_date,
                         MAX(date_active_tbrx_completed)          as tb_treatment_completed_date,
                         MAX(date_discontinued_tuberculosis_prop) as tb_treatment_discontinued_date -- Note: checking correct column mapping
                  FROM FollowUp
                  GROUP BY client_id)

SELECT c.client_id,
       c.patient_uuid,
       c.mrn,
       c.patient_name,
       c.sex,
       c.date_of_birth,
       TIMESTAMPDIFF(YEAR, c.date_of_birth, CURDATE())                    as age,

       -- Identifiers
       c.uan,
       id.phrh_code,
       id.ncd_code,
       id.icd_number,

       -- Registration
       NULL                                                               as registration_date,     -- Need to source enrollment date if not in flat_followup
       lfu.hiv_confirmed_date,
       NULL                                                               as transfer_in_date,      -- Need source
       TIMESTAMPDIFF(MONTH, art.start_date, CURDATE())                    as months_on_art,

       -- Address & Completeness
       c.state_province                                                   as region,
       c.county_district                                                  as zone,
       c.city_village                                                     as woreda,
       c.kebele,
       c.house_number,
       c.mobile_no                                                        as mobile_phone,
       CASE
           WHEN (c.patient_name IS NOT NULL AND c.state_province IS NOT NULL AND c.mobile_no IS NOT NULL) THEN 'GREEN'
           ELSE 'YELLOW' END                                              as address_completeness,

       -- ART Clinical
       art.start_date,

       -- Status Logic
       CASE
           WHEN lfu.follow_up_status IN ('Dead', 'Died') THEN 'Dead'
           WHEN lfu.follow_up_status IN ('Transferred out', 'TO') THEN 'Transferred Out'
           WHEN lfu.treatment_end_date >= CURDATE() THEN 'Active'
           WHEN lfu.visit_date IS NULL THEN 'No Clinical Contact'
           ELSE 'Lost to Follow-up'
           END                                                            as current_status,

       lfu.regimen                                                        as current_regimen,
       lfu.regimen_dose,

       -- Regimen Line
       CASE
           WHEN lfu.regimen LIKE '1%' THEN 'First Line'
           WHEN lfu.regimen LIKE '2%' THEN 'Second Line'
           ELSE 'Other'
           END                                                            as regimen_line,

       lfu.treatment_end_date                                             as tx_curr_end_date,
       lfu.nutritional_status_of_adult                                    as nutritional_status,
       lfu.pregnancy_status,
       CASE
           WHEN (lfu.pregnancy_status = 'Yes' OR lfu.currently_breastfeeding_child = 'Yes') THEN 'On PMTCT'
           ELSE 'Not Applicable' END                                      as pmtct_status,
       lfu.method_of_family_planning,

       lfu.visit_date                                                     as last_visit_date,
       lfu.next_visit_date,
       DATEDIFF(CURDATE(), COALESCE(lfu.next_visit_date, lfu.visit_date)) as days_overdue,

       -- Viral Load
       lab.vl_perform_date                                                as last_vl_date,
       lab.vl_result                                                      as last_vl_result,
       CASE WHEN lab.vl_result < 1000 THEN 1 ELSE 0 END                   as is_suppressed,

       -- VL Status Logic (Simplified)
       CASE
           WHEN lab.vl_perform_date IS NOT NULL AND lab.vl_result < 1000 THEN 'Suppressed'
           WHEN lab.vl_perform_date IS NOT NULL AND lab.vl_result >= 1000 THEN 'Unsuppressed'
           ELSE 'Unknown'
           END                                                            as vl_status,

       -- VL Eligibility (Simplified rule: 6mo after start, then yearly)
       CASE
           WHEN lab.vl_perform_date IS NULL THEN DATE_ADD(art.start_date, INTERVAL 6 MONTH)
           ELSE DATE_ADD(lab.vl_perform_date, INTERVAL 12 MONTH)
           END                                                            as vl_eligibility_date,

       -- TPT Status (Simplified Gold/Silver/Bronze)
       CASE
           WHEN tpt.tpt_completed_date IS NOT NULL THEN 'Gold (TPT Completed)'
           WHEN tpt.tpt_start_date IS NOT NULL AND tpt.tpt_completed_date IS NULL AND tpt.tpt_discontinued_date IS NULL
               THEN 'On TPT'
           WHEN tpt.is_contraindicated = 1 THEN 'Contraindicated'
           ELSE 'Not Started'
           END                                                            as tpt_status,

       -- TB Treatment
       tb.active_tb_diagnosis_date,
       tb.tb_treatment_start_date,
       tb.tb_treatment_discontinued_date,
       tb.tb_treatment_completed_date,

       lfu.dsd_category,
       'Not Screened'                                                     as ict_screening_status,  -- Placeholder, need ICT source query
       'Not Screened'                                                     as ncd_screening_status,  -- Placeholder
       'Not Screened'                                                     as cxca_screening_status, -- Placeholder
       'General Population'                                               as target_population      -- Placeholder

FROM mamba_dim_client c
         LEFT JOIN LatestFollowUp lfu ON c.client_id = lfu.client_id AND lfu.rn = 1
         LEFT JOIN LatestVL lab ON c.client_id = lab.client_id AND lab.rn_res = 1
         LEFT JOIN ARTStart art ON c.client_id = art.client_id
         LEFT JOIN Identifiers id ON c.client_id = id.patient_id
         LEFT JOIN TPT_Events tpt ON c.client_id = tpt.client_id
         LEFT JOIN TB_Events tb ON c.client_id = tb.client_id;
-- $END
