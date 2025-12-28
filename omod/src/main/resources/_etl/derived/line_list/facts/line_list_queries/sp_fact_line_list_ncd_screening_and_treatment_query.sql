DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_ncd_screening_and_treatment_query;

CREATE PROCEDURE sp_fact_line_list_ncd_screening_and_treatment_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    -- 1. Get the latest ART/HIV history for each client
    WITH art_history AS (
        SELECT
            client_id,
            hiv_confirmed_date,
            art_start_date,
            art_followup_date,
            art_followup_status,
            art_regimen,
            art_regimen_dose
        FROM (
                 SELECT
                     mpf.client_id,
                     mpf2.date_of_event AS hiv_confirmed_date,
                     mpf7.art_antiretroviral_start_date AS art_start_date,
                     mpf6.follow_up_date_followup_ AS art_followup_date,
                     mpf4.follow_up_status AS art_followup_status,
                     mpf1.regimen AS art_regimen,
                     mpf8.antiretroviral_art_dispensed_dose_i AS art_regimen_dose,
                     ROW_NUMBER() OVER (PARTITION BY mpf.client_id ORDER BY mpf6.follow_up_date_followup_ DESC, mpf.encounter_id DESC) AS rn
                 FROM mamba_flat_encounter_follow_up mpf
                          LEFT JOIN mamba_flat_encounter_follow_up_1 mpf1 ON mpf.encounter_id = mpf1.encounter_id
                          LEFT JOIN mamba_flat_encounter_follow_up_2 mpf2 ON mpf.encounter_id = mpf2.encounter_id
                          LEFT JOIN mamba_flat_encounter_follow_up_4 mpf4 ON mpf.encounter_id = mpf4.encounter_id
                          LEFT JOIN mamba_flat_encounter_follow_up_6 mpf6 ON mpf.encounter_id = mpf6.encounter_id
                          LEFT JOIN mamba_flat_encounter_follow_up_7 mpf7 ON mpf.encounter_id = mpf7.encounter_id
                          LEFT JOIN mamba_flat_encounter_follow_up_8 mpf8 ON mpf.encounter_id = mpf8.encounter_id
             ) sub
        WHERE rn = 1
    ),

         -- 2. Get NCD Screening Events in the date range
         ncd_screening AS (
             SELECT
                 mpn.client_id,
                 mpn.screening_date,
                 'Screening' AS visit_type,
                 NULL AS diagnosis_type,
                 NULL AS diagnosis,
                 NULL AS outcome,
                 NULL AS next_appointment_date,
                 NULL AS ncd_follow_up_date,
                 NULL AS risk_factors,
                 NULL AS dm_symptoms,
                 NULL AS hypertension_symptoms,
                 NULL AS bp1,
                 NULL AS bp2,
                 NULL AS bmi,
                 NULL AS fbs,
                 NULL AS fundoscopy,
                 NULL AS dm_complications,
                 NULL AS htn_complications,
                 NULL AS adherence,
                 NULL AS lifestyle_changes
             FROM mamba_flat_encounter_ncd_screening mpn
             WHERE (REPORT_START_DATE IS NULL OR mpn.screening_date >= REPORT_START_DATE)
               AND (REPORT_END_DATE IS NULL OR mpn.screening_date <= REPORT_END_DATE)
         ),

         -- 3. Get NCD Follow-up Events in the date range
         ncd_followup AS (
             SELECT
                 mpnf.client_id,
                 NULL AS screening_date,
                 mpnf.visit_type,
                 COALESCE(mpnf1.diabetes_mellitus_diagnose_type, mpnf1.hypertension_diagnose_type) AS diagnosis_type,
                 mpnf1.baseline_diagnosis AS diagnosis,
                 mpnf1.ncd_clinical_status AS outcome,
                 mpnf1.return_visit_date AS next_appointment_date,
                 mpnf.follow_up_date AS ncd_follow_up_date,
                 CONCAT_WS(',',
                           mpnf.use_of_tobacco,
                           mpnf.excessive_alcohol,
                           mpnf1.physical_inactivity,
                           mpnf1.high_salt_consumption,
                           mpnf1.high_sugar_consumption,
                           mpnf1.high_fat_consumption,
                           mpnf1.low_vegetable_fruit_consumption,
                           mpnf1.family_history_of_dm_htn_cvd,
                           mpnf.khat_consumption
                 ) AS risk_factors,
                 CONCAT_WS(',',
                           mpnf.polyuria,
                           mpnf.nocturia,
                           mpnf.polydipsia,
                           mpnf.fatigue,
                           mpnf.nausea,
                           mpnf.vomiting,
                           mpnf.deep_breathing,
                           mpnf.fast_breathing,
                           mpnf.weakness,
                           mpnf.abdominal_pain,
                           mpnf.hunger,
                           mpnf.palpitations,
                           mpnf.lightheadedness,
                           mpnf.dizziness,
                           mpnf.sweating,
                           mpnf.extreme_fatigue,
                           mpnf.confusion,
                           mpnf.bad_dreams,
                           mpnf.fever,
                           mpnf.cough,
                           mpnf.skin_lesions,
                           mpnf.dysuria,
                           mpnf1.frequent_urination,
                           mpnf1.diabetic_foot_ulcer,
                           mpnf.tingling,
                           mpnf.numbness,
                           mpnf.burning_sensation,
                           mpnf1.postural_dizziness,
                           mpnf1.sexual_dysfunction,
                           mpnf.visual_blurring
                 ) AS dm_symptoms,
                 CONCAT_WS(',',
                           mpnf.dyspnea,
                           mpnf.orthopnea,
                           mpnf1.paroxysmal_nocturnal_dyspnea,
                           mpnf.leg_swelling,
                           mpnf.angina,
                           mpnf1.intermittent_claudication,
                           mpnf1.hemiparesis_hemiplegia,
                           mpnf1.speech_abnormality,
                           mpnf.seizure,
                           mpnf.headache,
                           mpnf.photophobia,
                           mpnf.neck_pain
                 ) AS hypertension_symptoms,
                 mpnf1.systolic_blood_pressure AS bp1,
                 mpnf1.diastolic_blood_pressure AS bp2,
                 mpnf.bmi_text_ AS bmi,
                 mpnf1.fasting_blood_glucose_measurement_m AS fbs,
                 mpnf1.fundoscopic_exam_finding AS fundoscopy,
                 CONCAT_WS(',',
                           mpnf1.chronic_kidney_disease,
                           mpnf1.transient_ischemic_attack,
                           mpnf1.hypertensive_heart_disease,
                           mpnf1.peripheral_arterial_disease,
                           mpnf1.diabetic_retinopathy,
                           mpnf1.diabetic_neuropathy,
                           mpnf1.diabetic_nephropathy,
                           mpnf1.coronary_artery_disease,
                           mpnf1.diabetic_foot_ulcer,
                           mpnf1.diabetes_ketoacidosis
                 ) AS dm_complications,
                 CONCAT_WS(',',
                           mpnf1.chronic_kidney_disease,
                           mpnf1.transient_ischemic_attack,
                           mpnf1.hypertensive_heart_disease,
                           mpnf1.peripheral_arterial_disease,
                           mpnf1.coronary_artery_disease
                 ) AS htn_complications,
                 mpnf1.overall_treatment_adherence AS adherence,
                 CONCAT_WS(',',
                           mpnf1.cessation_of_smoking,
                           mpnf1.exercise_150min_week,
                           mpnf.weight_reduction,
                           mpnf.healthy_diet,
                           mpnf1.medication_adherence_side_effect,
                           mpnf1.daily_foot_inspection,
                           mpnf.family_screening,
                           mpnf1.injection_techniques
                 ) AS lifestyle_changes
             FROM mamba_flat_encounter_ncd_followup mpnf
                      LEFT JOIN mamba_flat_encounter_ncd_followup_1 mpnf1 ON mpnf.encounter_id = mpnf1.encounter_id
             WHERE (REPORT_START_DATE IS NULL OR mpnf.follow_up_date >= REPORT_START_DATE)
               AND (REPORT_END_DATE IS NULL OR mpnf.follow_up_date <= REPORT_END_DATE)
         ),

         -- 4. Combine Screening and Follow-up
         combined_events AS (
             SELECT * FROM ncd_screening
             UNION ALL
             SELECT * FROM ncd_followup
         )

    -- 5. Final Select with Client and ART details
    SELECT
        mpc.mrn AS 'MRN',
        mpc.uan AS 'UAN',
        mpc.patient_name AS 'Full Name',
        mpc.sex AS 'Sex',
        art.hiv_confirmed_date AS 'HIV Confirmed Date EC.',
        art.hiv_confirmed_date AS 'HIV Confirmed Date (GC)',
        art.art_start_date AS 'ART Start Date EC.',
        art.art_start_date AS 'ART Start Date (GC)',
        e.screening_date AS 'Screening Date',
        art.art_followup_date AS 'Follow-Up Date (ART)',
        mpc.state_province AS 'Address: Region',
        mpc.county_district AS 'Address: Zone',
        mpc.city_village AS 'Address: Woreda',
        mpc.mobile_no AS 'Address: Phone Number (Mobile)',
        e.diagnosis_type AS 'Diagnosis Type',
        e.diagnosis AS 'Diagnosis',
        e.outcome AS 'Outcome',
        e.next_appointment_date AS 'Next Appointment Date (NCD) EC.',
        e.next_appointment_date AS 'Next Appointment Date (NCD) (GC)',
        e.ncd_follow_up_date AS 'Follow-Up Date NCD  EC.',
        e.ncd_follow_up_date AS 'Follow-Up Date NCD  (GC)',
        art.art_followup_status AS 'Follow-Up Status (ART)',
        TIMESTAMPDIFF(YEAR, mpc.date_of_birth, CURDATE()) AS 'Age',
        art.art_regimen AS 'ART Regimen',
        art.art_regimen_dose AS 'ART Regimen Dose',
        e.visit_type AS 'Visit Type',
        e.risk_factors AS 'Risk Factors',
        e.dm_symptoms AS 'Diabetes Melitus (DM) Symptoms',
        e.hypertension_symptoms AS 'Hypertension (HTN) Symptoms',
        e.bp1 AS 'BP1',
        e.bp2 AS 'BP2',
        e.bmi AS 'BMI',
        e.fbs AS 'FBS',
        e.fundoscopy AS 'Fundoscopy',
        e.dm_complications AS 'DM Complications',
        e.htn_complications AS 'HTN Complications',
        e.adherence AS 'Adherence',
        e.lifestyle_changes AS 'Lifestyle Changes'
    FROM combined_events e
             INNER JOIN mamba_dim_client mpc ON e.client_id = mpc.client_id
             LEFT JOIN art_history art ON e.client_id = art.client_id;

END //

DELIMITER ;
