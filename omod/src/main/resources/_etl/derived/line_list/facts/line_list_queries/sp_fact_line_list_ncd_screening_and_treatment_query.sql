DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_ncd_screening_and_treatment_query;

CREATE PROCEDURE sp_fact_line_list_ncd_screening_and_treatment_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)

BEGIN

	WITH screening AS (
        SELECT
			mpn.client_id,
			mpc.mrn,
			mpc.uan,
			mpc.patient_name AS full_name,
			mpc.sex AS gender,
			TIMESTAMPDIFF(YEAR, mpc.date_of_birth, CURDATE()) AS age,
			mpc.state_province AS region,
			mpc.county_district AS zone,
			mpc.city_village AS woreda,
			mpc.mobile_no AS phone_number,
            mpn.screening_date
		FROM mamba_flat_encounter_ncd_screening mpn
        INNER JOIN mamba_dim_client mpc
			ON mpn.client_id = mpc.client_id
    ),
	ncd_followup AS (
		SELECT mpn.*,
			ROW_NUMBER() OVER (PARTITION BY mpn.client_id ORDER BY mpnf.follow_up_date DESC, mpnf.encounter_id DESC) AS ncd_followup_row_num,
			ROW_NUMBER() OVER (PARTITION BY mpn.client_id ORDER BY mpf6.follow_up_date_followup_ DESC, mpf6.encounter_id DESC) AS art_followup_row_num,
            mpf2.date_of_event AS hiv_confirmed_date_ec,
			mpf2.date_of_event AS hiv_confirmed_date,
			mpf7.art_antiretroviral_start_date AS art_start_date_ec,
			mpf7.art_antiretroviral_start_date AS art_start_date,
			mpf6.follow_up_date_followup_ AS art_followup_date,
			COALESCE(mpnf1.diabetes_mellitus_diagnose_type, mpnf1.hypertension_diagnose_type) AS diagnosis_type,
            mpnf1.baseline_diagnosis AS diagnosis,
			mpnf1.ncd_clinical_status AS outcome,
			mpnf1.return_visit_date AS next_appointment_date_ec,
			mpnf1.return_visit_date AS next_appointment_date,
			mpnf.follow_up_date AS ncd_follow_up_date_ec,
			mpnf.follow_up_date AS ncd_follow_up_date,
			mpf4.follow_up_status AS art_followup_status,
			mpf1.regimen AS art_regimen,
			mpf8.antiretroviral_art_dispensed_dose_i AS art_regimen_dose,
			mpnf.visit_type,
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
	FROM screening mpn
		LEFT JOIN mamba_flat_encounter_ncd_followup mpnf
			ON mpn.client_id = mpnf.client_id
		LEFT JOIN mamba_flat_encounter_ncd_followup_1 mpnf1
			ON mpnf.encounter_id = mpnf1.encounter_id
		LEFT JOIN mamba_flat_encounter_follow_up mpf
			ON mpn.client_id = mpf.client_id
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
			ON mpf.encounter_id = mpf8.encounter_id
		WHERE REPORT_START_DATE is not null and REPORT_END_DATE is not null and (mpnf.follow_up_date BETWEEN REPORT_START_DATE and REPORT_END_DATE) or
			(REPORT_START_DATE is not null and REPORT_END_DATE is null and (mpnf.follow_up_date >= REPORT_START_DATE)) or
			(REPORT_START_DATE is null and REPORT_END_DATE is not null and (mpnf.follow_up_date <= REPORT_END_DATE)) or
            (REPORT_START_DATE is null and REPORT_END_DATE is null)
    )

    SELECT
		f.mrn AS 'MRN',
        f.uan AS 'UAN',
        f.full_name AS 'Full Name',
        f.gender AS 'Sex',
        f.hiv_confirmed_date_ec AS 'HIV Confirmed Date EC.',
        f.hiv_confirmed_date AS 'HIV Confirmed Date (GC)',
        f.art_start_date_ec AS 'ART Start Date EC.',
        f.art_start_date AS 'ART Start Date (GC)',
        f.screening_date AS 'Screening Date',
        f.art_followup_date AS 'Follow-Up Date (ART)',
        f.region AS 'Address: Region',
        f.zone AS 'Address: Zone',
        f.woreda AS 'Address: Woreda',
        f.phone_number AS 'Address: Phone Number (Mobile)',
        f.diagnosis_type AS 'Diagnosis Type',
        f.diagnosis AS 'Diagnosis',
        f.outcome AS 'Outcome',
        f.next_appointment_date_ec AS 'Next Appointment Date (NCD) EC.',
        f.next_appointment_date AS 'Next Appointment Date (NCD) (GC)',
        f.ncd_follow_up_date_ec AS 'Follow-Up Date NCD  EC.',
        f.ncd_follow_up_date AS 'Follow-Up Date NCD  (GC)',
        f.art_followup_status AS 'Follow-Up Status (ART)',
        f.age AS 'Age',
        f.art_regimen AS 'ART Regimen',
        f.art_regimen_dose AS 'ART Regimen Dose',
        f.visit_type AS 'Visit Type',
        f.risk_factors AS 'Risk Factors',
        f.dm_symptoms AS 'Diabetes Melitus (DM) Symptoms',
        f.hypertension_symptoms AS 'Hypertension (HTN) Symptoms',
        f.bp1 AS 'BP1',
        f.bp2 AS 'BP2',
        f.bmi AS 'BMI',
        f.fbs AS 'FBS',
        f.fundoscopy AS 'Fundoscopy',
        f.dm_complications AS 'DM Complications',
        f.htn_complications AS 'HTN Complications',
        f.adherence AS 'Adherence',
        f.lifestyle_changes AS 'Lifestyle Changes'
	FROM ncd_followup f
	WHERE f.ncd_followup_row_num = 1;
END //

DELIMITER ;