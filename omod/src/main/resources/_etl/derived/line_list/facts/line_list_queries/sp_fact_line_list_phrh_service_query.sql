DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_phrh_service_query;

CREATE PROCEDURE sp_fact_line_list_phrh_service_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE, IN TARGET_GROUP VARCHAR(255))
BEGIN
    WITH enrollment AS (
        SELECT
        mpe.client_id AS client_id,
        ROW_NUMBER() OVER (PARTITION BY mpe.client_id ORDER BY mpf.followup_date DESC, mpf.encounter_id DESC) AS row_num,
        mpi.identifier AS mrn,
        mp.person_name_short AS full_name,
        mp.gender AS gender,
        TIMESTAMPDIFF(YEAR, mp.birthdate, COALESCE(REPORT_END_DATE, CURDATE())) AS age,
        mpe.followup_date AS phrh_enrollment_date,
        mpe.previously_tested_for_hiv AS previously_tested_for_hiv,
        followup_date AS phrh_followup_date_ec,
        mpf.followup_date AS phrh_followup_date,
        CONCAT_WS(',',
			mpf.history_of_sti,
            mpf.multiple_partners_exchange,
            mpf.inconsistent_condoms,
            mpf.consistent_condoms,
            mpf.drug_use,
            mpf.iv_drug_use,
            mpf.declined_to_disclose) AS risk_behaviors,
		CASE
			WHEN mpf.target_population = 'Female sex worker' THEN 'FSW'
			WHEN mpf.target_population = 'People who inject drug' THEN 'PWID'
			WHEN mpf.target_population = 'Late adolescent/young adulthood period' THEN 'High Risk AGYW'
			WHEN mpf.target_population = 'Other' THEN 'Other KPP'
			ELSE 'General Population'
		END
		AS target_population,
        mpf.modality_used_to_reach AS modality,
        mpf.hiv_self_test_performed AS hiv_self_test_kit,
        mpf.hivst_result AS self_test_result,
        mpf.final_hiv_test_result AS conventional_result,
        mpf.on_antiretroviral_therapy AS art_started,
        mpf.unique_art_number AS unique_art_number,
        mpf.pmtct_linkage_date AS date_linked_to_pmtct,
        mpf1.prep_pre_exposure_prophylaxis_eligi AS eligible_for_prep,
        mpf.discharge_date AS prep_discharge_date_ec,
        mpf.discharge_date as prep_discharge_date,
        mpf.prep_started AS prep_started,
        mpf.next_visit_date AS next_visit_date_ec,
        mpf.next_visit_date AS next_visit_date,
        mpf.screen_result_for_sti AS sti_diagnosis,
        mpf.tb_diagnostic_test_result AS tb_screening_result,
        mpf.substance_use AS mhi_identified,
        mpf.hepatitis_b_test_qualitative AS hepatitis_b_result,
        mpf.hepatitis_c_test_qualitative AS hepatitis_c_result,
        mpf.hepatitis_b_vaccination AS hepatitis_b_vaccination,
        mpf.family_planning_counseling AS fp_counseling,
        mpf1.ready_for_cervical_cancer_screening AS eligible_for_cxca_screen,
        mpf1.linked_to_cervical_cancer_screening AS counseled_and_linked_cxca,
        mpf.last_follow_up_outcome AS last_follow_up_outcome,
        mpf.decision AS final_decision
        FROM mamba_flat_encounter_phrh_enrollment mpe
        LEFT JOIN mamba_flat_encounter_phrh_followup mpf
			ON mpe.client_id = mpf.client_id
		INNER JOIN mamba_flat_encounter_phrh_followup_1 mpf1
			ON mpf.encounter_id = mpf1.encounter_id
        INNER JOIN mamba_dim_patient_identifier mpi
            ON mpe.client_id = mpi.patient_id
        INNER JOIN mamba_dim_patient_identifier_type mpit
            ON mpi.identifier_type = mpit.patient_identifier_type_id and mpit.name = 'MRN'
		INNER JOIN mamba_dim_person mp on mpe.client_id = mp.person_id
        WHERE REPORT_START_DATE is not null and REPORT_END_DATE is not null and (mpf.followup_date BETWEEN REPORT_START_DATE and REPORT_END_DATE) or
			(REPORT_START_DATE is not null and REPORT_END_DATE is null and (mpf.followup_date >= REPORT_START_DATE)) or
			(REPORT_START_DATE is null and REPORT_END_DATE is not null and (mpf.followup_date <= REPORT_END_DATE)) or
            (REPORT_START_DATE is null and REPORT_END_DATE is null)
    ),

    phrhs AS (
        SELECT
        mpe.client_id AS client_id,
        mpi.identifier AS phrh
        FROM mamba_flat_encounter_phrh_enrollment mpe
        INNER JOIN mamba_dim_patient_identifier mpi
            ON mpe.client_id = mpi.patient_id
        INNER JOIN mamba_dim_patient_identifier_type mpit
            ON mpi.identifier_type = mpit.patient_identifier_type_id
		WHERE mpit.name = 'PHRH'
    )

    SELECT
		en.mrn AS 'MRN',
        p.phrh AS 'PHRH Code',
        en.full_name AS 'Name',
        en.gender AS 'Sex',
        en.age AS 'Age',
        en.phrh_enrollment_date AS 'PHRH Enrollment Date',
        en.previously_tested_for_hiv AS 'Previously Tested for HIV',
        en.phrh_followup_date_ec AS 'FU Date (PHRH) EC.',
        en.phrh_followup_date AS 'FU Date (PHRH) (GC)',
        en.risk_behaviors AS 'Risk Behaviors',
        en.target_population AS 'Target group',
        en.modality AS 'Modality',
        en.hiv_self_test_kit AS 'Self-test kit',
        en.self_test_result AS 'Self-testing result',
        en.conventional_result AS 'Conventional Result',
        en.art_started AS 'Start ART',
        en.unique_art_number AS 'Unique ART #',
        en.date_linked_to_pmtct AS 'Date Linked to PMTCT',
        en.eligible_for_prep AS 'Eligible for PrEP',
        en.prep_started AS 'PrEP Started',
        en.prep_discharge_date_ec AS 'Date Discharged PrEP EC.',
        en.prep_discharge_date AS 'Date Discharged PrEP(GC)',
        en.next_visit_date_ec AS 'Next Appointment Date (PHRH) EC.',
        en.next_visit_date AS 'Next Appointment Date (PHRH) (GC)',
        en.sti_diagnosis AS 'STI Diagnosis',
        en.tb_screening_result AS 'TB Screening Result',
        en.mhi_identified AS 'Is MHI Identified?',
        en.hepatitis_b_result AS 'Hepatitis B Result',
        en.hepatitis_c_result AS 'Hepatitis C Result',
        en.hepatitis_b_vaccination AS 'Hepatitis B vaccination (PWID)',
        en.fp_counseling AS 'FP Counseling',
        en.eligible_for_cxca_screen AS 'Eligible for CXCA Screen',
        en.counseled_and_linked_cxca AS 'Counseled & linked CXCA',
        en.last_follow_up_outcome AS 'Last FU Outcome',
        en.final_decision AS 'Final Decision'
	FROM enrollment en JOIN phrhs p on en.client_id = p.client_id
    WHERE  en.row_num = 1 and ((TARGET_GROUP != 'ALL' and en.target_population = TARGET_GROUP) or TARGET_GROUP = 'ALL');
END //

DELIMITER ;