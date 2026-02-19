DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_ncd_screening_query;

CREATE PROCEDURE sp_fact_line_list_ncd_screening_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH art_history AS (SELECT client_id,
                                hiv_confirmed_date,
                                art_start_date,
                                art_followup_date,
                                art_followup_status,
                                art_regimen,
                                art_regimen_dose
                         FROM (SELECT mpf.client_id,
                                      mpf2.date_of_event                                                                                                AS hiv_confirmed_date,
                                      mpf7.art_antiretroviral_start_date                                                                                AS art_start_date,
                                      mpf6.follow_up_date_followup_                                                                                     AS art_followup_date,
                                      mpf4.follow_up_status                                                                                             AS art_followup_status,
                                      mpf1.regimen                                                                                                      AS art_regimen,
                                      mpf8.antiretroviral_art_dispensed_dose_i                                                                          AS art_regimen_dose,
                                      ROW_NUMBER() OVER (PARTITION BY mpf.client_id ORDER BY mpf6.follow_up_date_followup_ DESC, mpf.encounter_id DESC) AS rn
                               FROM mamba_flat_encounter_follow_up mpf
                                        LEFT JOIN mamba_flat_encounter_follow_up_1 mpf1
                                                  ON mpf.encounter_id = mpf1.encounter_id
                                        LEFT JOIN mamba_flat_encounter_follow_up_2 mpf2
                                                  ON mpf.encounter_id = mpf2.encounter_id
                                        LEFT JOIN mamba_flat_encounter_follow_up_4 mpf4
                                                  ON mpf.encounter_id = mpf4.encounter_id
                                        LEFT JOIN mamba_flat_encounter_follow_up_6 mpf6
                                                  ON mpf.encounter_id = mpf6.encounter_id
                                        LEFT JOIN mamba_flat_encounter_follow_up_7 mpf7
                                                  ON mpf.encounter_id = mpf7.encounter_id
                                        LEFT JOIN mamba_flat_encounter_follow_up_8 mpf8
                                                  ON mpf.encounter_id = mpf8.encounter_id) sub
                         WHERE rn = 1),

         -- 2. Get NCD Screening Events in the date range
         ncd_screening AS (SELECT mpn.client_id,
                                  mpn.screening_date,
                                  risk_factors_present,
                                  past_complication,
                                  past_medication,
                                  weight_text_,
                                  bmi_text_,
                                  diastolic_blood_pressure,
                                  fasting_blood_glucose_measurement_m,
                                  baseline_diagnosis,
                                  diagnosed_previously,
                                  'Screening' AS visit_type
                           FROM mamba_flat_encounter_ncd_screening mpn
                           WHERE (REPORT_START_DATE IS NULL OR mpn.screening_date >= REPORT_START_DATE)
                             AND (REPORT_END_DATE IS NULL OR mpn.screening_date <= REPORT_END_DATE))

    -- 5. Final Select with Client and ART details
    SELECT mpc.patient_name                                  AS 'Full Name',
           mpc.uan                                           AS 'UAN',
           mpc.mrn                                           AS 'MRN',
           TIMESTAMPDIFF(YEAR, mpc.date_of_birth, CURDATE()) AS 'Age',
           mpc.sex                                           AS 'Sex',
           art.hiv_confirmed_date                            AS 'HIV Confirmed Date EC.',
           art.hiv_confirmed_date                            AS 'HIV Confirmed Date (GC)',
           art.art_start_date                                AS 'ART Start Date EC.',
           art.art_start_date                                AS 'ART Start Date (GC)',

           art_followup_status                               as `Last Follow-Up Status`,
           e.screening_date                                  AS 'NCD Screening Date ',
           risk_factors_present                                 `Risk Factors`,
           diagnosed_previously                              as `Past Diagnosis`,
           past_complication                                 as `Past Complication`,
           past_medication                                   as `Past Medication`,
           weight_text_                                      as `Weight`,
           bmi_text_                                         as `BMI`,
           diastolic_blood_pressure                          as `Blood Pressure`,
           fasting_blood_glucose_measurement_m               as `FBS (mg/dl)`,
           baseline_diagnosis                                as `Baseline Diagnosis`,
# UNCD #


           art.art_followup_date                             AS 'Follow-Up Date (ART)',
           mpc.state_province                                AS 'Address: Region',
           mpc.county_district                               AS 'Address: Zone',
           mpc.city_village                                  AS 'Address: Woreda',
           mpc.mobile_no                                     AS 'Address: Phone Number (Mobile)',

           art.art_followup_status                           AS 'Follow-Up Status (ART)',

           art.art_regimen                                   AS 'ART Regimen',
           art.art_regimen_dose                              AS 'ART Regimen Dose',
           e.visit_type                                      AS 'Visit Type'
    FROM ncd_screening e
             INNER JOIN mamba_dim_client mpc ON e.client_id = mpc.client_id
             LEFT JOIN art_history art ON e.client_id = art.client_id;

END //

DELIMITER ;
