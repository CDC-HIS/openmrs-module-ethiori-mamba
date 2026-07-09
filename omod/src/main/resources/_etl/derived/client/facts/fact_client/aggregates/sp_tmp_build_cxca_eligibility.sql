DELIMITER //

DROP PROCEDURE IF EXISTS sp_tmp_build_cxca_eligibility;

-- Builds a session-scoped temp table with each client's cervical cancer screening
-- eligibility status, in one self-contained statement queried directly off
-- mamba_fact_follow_up and mamba_dim_client.
-- Replaces the previous `LEFT JOIN LATERAL (...) ON TRUE` in sp_fact_client_insert,
-- which re-evaluated a correlated subquery once per row of mamba_dim_client -- this
-- version computes the same result once and is joined normally by client_id.
-- p_as_of_date = NULL means "no cutoff" (matches the ETL's full-history behavior);
-- v_end (defaulting to CURDATE() when p_as_of_date is NULL) is the reference point
-- used for all "days since" date arithmetic and the age calculation, matching the
-- ETL's use of CURDATE() today.
CREATE PROCEDURE sp_tmp_build_cxca_eligibility(IN p_as_of_date DATE)
BEGIN
    DECLARE v_end DATE;
    SET v_end = COALESCE(p_as_of_date, CURDATE());

    DROP TEMPORARY TABLE IF EXISTS mamba_temp_cxca_eligibility;

    CREATE TEMPORARY TABLE mamba_temp_cxca_eligibility AS
    WITH LatestFollowUp AS (SELECT *
                             FROM (SELECT client_id,
                                          follow_up_status                                                                                     AS final_follow_up_status,
                                          art_antiretroviral_start_date                                                                        AS art_start_date,
                                          cervical_cancer_screening_status                                                                     AS screening_status,
                                          eligible_for_cxca_screening,
                                          reason_for_not_being_eligible,
                                          other_reason_for_not_being_eligible_for_cxca,
                                          ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS rn
                                   FROM mamba_fact_follow_up
                                   WHERE follow_up_date_followup_ IS NOT NULL
                                     AND (p_as_of_date IS NULL OR follow_up_date_followup_ <= p_as_of_date)) ranked
                             WHERE rn = 1),

         CXCA_PrevScreening AS (SELECT *
                                 FROM (SELECT client_id,
                                              via_screening_result                                                                                 AS ccs_via_result,
                                              date_visual_inspection_of_the_cervi                                                                   AS via_date,
                                              treatment_start_date                                                                                  AS ccs_treat_received_date,
                                              date_patient_referred_out,
                                              biopsy_result,
                                              hpv_dna_screening_result                                                                              AS ccs_hpv_result,
                                              hpv_dna_result_received_date,
                                              cytology_result,
                                              date_cytology_result_received                                                                        AS cytology_result_date,
                                              colposcopy_of_cervix_findings                                                                         AS colposcopy_exam_finding,
                                              biopsy_result_received_date,
                                              next_follow_up_screening_date,
                                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS rn
                                       FROM mamba_fact_follow_up
                                       WHERE cervical_cancer_screening_status = 'Cervical cancer screening performed'
                                         AND (p_as_of_date IS NULL OR follow_up_date_followup_ <= p_as_of_date)) ranked
                                 WHERE rn = 1),

         CXCA_Eligibility AS (SELECT lf.client_id,
                                      lf.final_follow_up_status,
                                      lf.art_start_date,
                                      ps.next_follow_up_screening_date,
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
                                          WHEN (TIMESTAMPDIFF(DAY, ps.via_date, v_end)) > 730 AND
                                               ps.ccs_via_result = 'VIA negative'
                                              THEN 'Eligible Needs Re-Screening'
                                          WHEN ps.ccs_via_result = 'VIA positive: eligible for cryo/thermo-coagula'
                                              THEN CASE
                                                       WHEN
                                                           ((TIMESTAMPDIFF(DAY, ps.ccs_treat_received_date, v_end)) >
                                                            365 OR
                                                            (TIMESTAMPDIFF(DAY, ps.date_patient_referred_out, v_end)) >
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
                                               TIMESTAMPDIFF(DAY, ps.hpv_dna_result_received_date, v_end) > 1095
                                              THEN 'Eligible Needs Re-Screening'
                                          WHEN ps.ccs_via_result IS NULL AND ps.ccs_hpv_result = 'Positive'
                                              THEN 'Eligible Needs VIA Triage'
                                          WHEN ps.cytology_result = 'Negative result' AND
                                               TIMESTAMPDIFF(DAY, ps.cytology_result_date, v_end) > 1095
                                              THEN 'Eligible Needs Re-Screening'
                                          WHEN (ps.cytology_result =
                                                'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear' OR
                                                ps.cytology_result = '> Ascus')
                                              THEN CASE
                                                       WHEN ps.hpv_dna_result_received_date IS NULL
                                                           THEN 'Eligible Needs HPV Triage'
                                                       WHEN ps.ccs_hpv_result = 'Negative result' AND
                                                            TIMESTAMPDIFF(DAY, ps.hpv_dna_result_received_date, v_end) >
                                                            730
                                                           THEN 'Eligible Needs Re-Screening'
                                                       WHEN ps.ccs_hpv_result = 'Positive' AND ps.colposcopy_exam_finding IS NULL
                                                           THEN 'Eligible Needs Colposcopy Test'
                                                       ELSE 'Not Eligible (Screening Up-to-Date)'
                                                  END
                                          WHEN (ps.biopsy_result = 'CIN (1-3)' OR ps.biopsy_result = 'CIN-2') AND
                                               TIMESTAMPDIFF(DAY, ps.biopsy_result_received_date, v_end) > 365
                                              THEN 'Eligible Needs Re-Screening'
                                          ELSE 'Not Eligible (Screening Up-to-Date)'
                                          END                                         AS EligibilityStatus
                               FROM LatestFollowUp lf
                                        LEFT JOIN CXCA_PrevScreening ps ON lf.client_id = ps.client_id)

    SELECT c.client_id,
           CASE
               WHEN ce.client_id IS NULL THEN NULL
               WHEN c.sex = 'Male' OR TIMESTAMPDIFF(YEAR, c.date_of_birth, v_end) < 25 OR
                    TIMESTAMPDIFF(YEAR, c.date_of_birth, v_end) > 65 OR
                    ce.final_follow_up_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away')
                   THEN 'Not Applicable (Blue)'
               WHEN ce.art_start_date IS NULL THEN 'ART Not Started (Black)'
               WHEN ce.EligibilityStatus = 'Not Eligible Confirmed Cirvical Cancer' THEN 'Confirmed CXCA (RED)'
               WHEN ce.EligibilityStatus = 'Not Eligible (Screening Up-to-Date)' THEN 'Previously Screened (Green)'
               WHEN ce.EligibilityStatus LIKE 'Eligible%' THEN 'Eligible (Yellow)'
               WHEN ce.EligibilityStatus LIKE 'Not Eligible%' THEN 'Not Eligible (White)'
               ELSE 'Unknown Status'
               END AS cxca_screening_status,
           ce.next_follow_up_screening_date
    FROM mamba_dim_client c
             LEFT JOIN CXCA_Eligibility ce ON c.client_id = ce.client_id;

    ALTER TABLE mamba_temp_cxca_eligibility ADD PRIMARY KEY (client_id);
END //

DELIMITER ;
