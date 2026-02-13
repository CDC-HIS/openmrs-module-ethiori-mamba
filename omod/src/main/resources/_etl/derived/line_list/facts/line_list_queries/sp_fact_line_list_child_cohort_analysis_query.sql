DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_child_cohort_analysis_query;

CREATE PROCEDURE sp_fact_line_list_child_cohort_analysis_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

WITH HeiEnrolled AS (SELECT h.encounter_id                         as hei_encounter_id,
                            pm.encounter_id                        as mother_encounter_id,
                            client.identifier                      as mother_mrn,
                            h.client_id                            as hei_client_id,
                            pm.client_id                           as mother_client_id,
                            pm.date_of_enrollment_or_booking       as date_of_erollement,
                            h.date_enrolled_in_care                as hei_enrollment_date,
                            h.infant_referred                      as infant_referred,
                            ho.hei_pmtct_final_outcome             as final_out_come,
                            followup_date_followup
                     FROM mamba_flat_encounter_hei_enrollment as h
                              LEFT JOIN mamba_dim_patient_identifier as client
                                        on h.service_delivery_point_number = client.identifier
                                            and client.identifier_type = 'MRN'
                              LEFT JOIN mamba_flat_encounter_pmtct_enrollment as pm
                                        on pm.client_id = client.patient_id
                              LEFT JOIN mamba_flat_encounter_hei_final_outcome as ho
                                        on h.client_id = ho.client_id
                     WHERE (client.identifier is null and (h.date_enrolled_in_care between REPORT_START_DATE
                         and REPORT_END_DATE))
                        or (client.identifier is not null
                         and (pm.date_of_enrollment_or_booking between
                             REPORT_START_DATE and REPORT_END_DATE)
                         and
                            h.date_enrolled_in_care between REPORT_START_DATE and DATE_ADD(REPORT_START_DATE, INTERVAL 12 MONTH))),

     -- Deduplicate HEI in Cohort
     HIEInCohortFiltered AS (SELECT *
                             FROM (SELECT a.*,
                                          ROW_NUMBER() OVER (PARTITION BY a.hei_client_id ORDER BY a.hei_enrollment_date DESC) AS row_num
                                   FROM HeiEnrolled a
                                   WHERE hei_enrollment_date is not null) t
                             WHERE row_num = 1),

     -- 2. Define Intervals
     IntervalsDef AS (SELECT 12 AS interval_month
                      UNION ALL
                      SELECT 18
                      UNION ALL
                      SELECT 24
                      UNION ALL
                      SELECT 30),

     -- 3. Calculate Interval Dates per Patient
     PatientIntervals AS (SELECT h.hei_client_id,
                                 h.hei_enrollment_date,
                                 i.interval_month,
                                 DATE_ADD(h.hei_enrollment_date, INTERVAL i.interval_month MONTH) as interval_end_date
                          FROM HIEInCohortFiltered h
                                   CROSS JOIN IntervalsDef i),

     -- 4. Unified Event Stream (Mother + HEI + Final Outcomes)
     AllEvents AS (
         -- Mother Events (Mother's status affects Child)
         SELECT h.hei_client_id,
                f.follow_up_date_followup_ as event_date,
                CASE
                    WHEN f.follow_up_status IN ('Dead', 'Died') THEN 'Dead'
                    WHEN f.follow_up_status IN ('Transferred out', 'TO') THEN 'Transferred out'
                    WHEN f.follow_up_status IN ('Stop all', 'Loss to follow-up (LTFU)', 'Ran away') THEN 'Lost to follow-up'
                    ELSE 'Active'
                    END as status,
                1 as priority -- Lowest priority
         FROM HIEInCohortFiltered h
                  JOIN mamba_flat_encounter_follow_up f ON h.mother_client_id = f.client_id
         WHERE f.follow_up_date_followup_ IS NOT NULL

         UNION ALL

         -- HEI Followup Events
         SELECT h.hei_client_id,
                hf.followup_date_followup as event_date,
                CASE
                    WHEN hf.decision IN ('Dead', 'Died') THEN 'Dead'
                    WHEN hf.decision IN ('TO', 'Transferred out') THEN 'Transferred out'
                    WHEN hf.decision IN ('Lost to Follow-up') THEN 'Lost to follow-up'
                    ELSE 'Active'
                    END as status,
                2 as priority -- Medium priority
         FROM HIEInCohortFiltered h
                  JOIN mamba_flat_encounter_hei_followup hf ON h.hei_client_id = hf.client_id
         WHERE hf.followup_date_followup IS NOT NULL

         UNION ALL

         -- HEI Final Outcome Events
         SELECT h.hei_client_id,
                ho.date_when_final_outcome_was_known as event_date,
                ho.hei_pmtct_final_outcome as status,
                3 as priority -- Highest priority
         FROM HIEInCohortFiltered h
                  JOIN mamba_flat_encounter_hei_final_outcome ho ON h.hei_client_id = ho.client_id
         WHERE ho.date_when_final_outcome_was_known IS NOT NULL
     ),

     -- 5. Determine Snapshot Status at each Interval
     LatestStatus AS (
         SELECT pi.hei_client_id,
                pi.interval_month,
                ae.status,
                ae.event_date,
                ROW_NUMBER() OVER (PARTITION BY pi.hei_client_id, pi.interval_month ORDER BY ae.event_date DESC, ae.priority DESC) as rn
         FROM PatientIntervals pi
                  LEFT JOIN AllEvents ae ON pi.hei_client_id = ae.hei_client_id
             AND ae.event_date <= pi.interval_end_date
     ),

     CohortSnapshot AS (
         SELECT hei_client_id, interval_month, COALESCE(status, 'Lost to follow-up') as status, event_date
         FROM LatestStatus
         WHERE rn = 1
     ),

     -- 6. PCR Information (Static based on age)
     PCRInfo AS (
         SELECT h.hei_client_id,
                -- PCR < 2 Months
                MAX(CASE WHEN TIMESTAMPDIFF(MONTH, c.date_of_birth, COALESCE(hf.dna_pcr_sample_collection_date, hf.date_dbs_result_received, hf.hiv_test_date)) < 2
                             THEN hf.hiv_test_date END) as pcr_date_lt_2m,
                MAX(CASE WHEN TIMESTAMPDIFF(MONTH, c.date_of_birth, COALESCE(hf.dna_pcr_sample_collection_date, hf.date_dbs_result_received, hf.hiv_test_date)) < 2
                             THEN hf.hiv_test_result END) as pcr_result_lt_2m,
                -- PCR 2-12 Months
                MAX(CASE WHEN TIMESTAMPDIFF(MONTH, c.date_of_birth, COALESCE(hf.dna_pcr_sample_collection_date, hf.date_dbs_result_received, hf.hiv_test_date)) BETWEEN 2 AND 12
                             THEN hf.hiv_test_date END) as pcr_date_2_12m,
                MAX(CASE WHEN TIMESTAMPDIFF(MONTH, c.date_of_birth, COALESCE(hf.dna_pcr_sample_collection_date, hf.date_dbs_result_received, hf.hiv_test_date)) BETWEEN 2 AND 12
                             THEN hf.hiv_test_result END) as pcr_result_2_12m
         FROM HIEInCohortFiltered h
                  JOIN mamba_dim_client c ON h.hei_client_id = c.client_id
                  JOIN mamba_flat_encounter_hei_hiv_test hf ON h.hei_client_id = hf.client_id
         WHERE hf.hiv_test_performed IS NOT NULL
         GROUP BY h.hei_client_id
     )

-- Final Select: Pivot Data
SELECT
    c.patient_name                                                                    AS 'Patient Name',
    c.patient_uuid                                                                    AS 'UUID',
    CAST(c.mrn AS CHAR(20))                                                           AS 'MRN',
    h.mother_mrn                                                                      AS 'Mother MRN',
    c.Sex                                                                             AS 'Sex',
    c.date_of_birth                                                                   AS 'Date of Birth',
    c.date_of_birth                                                                   AS 'Date of Birth EC.',
    h.hei_enrollment_date                                                             AS 'Enrollment Date',
    h.hei_enrollment_date                                                             AS 'Enrollment Date EC.',

        -- PCR Details
    pcr.pcr_date_lt_2m                                                                AS 'PCR Date (< 2 Months)',
    pcr.pcr_date_lt_2m                                                                AS 'PCR Date (< 2 Months) EC.',
    pcr.pcr_result_lt_2m                                                              AS 'PCR Result (< 2 Months)',
    pcr.pcr_date_2_12m                                                                AS 'PCR Date (2-12 Months)',
    pcr.pcr_date_2_12m                                                                AS 'PCR Date (2-12 Months) EC.',
    pcr.pcr_result_2_12m                                                              AS 'PCR Result (2-12 Months)',

        -- 12 Months
    MAX(CASE WHEN cs.interval_month = 12 THEN cs.status ELSE NULL END)                AS 'Status at 12 Months',
    MAX(CASE WHEN cs.interval_month = 12 THEN cs.event_date ELSE NULL END)            AS 'Latest Event Date at 12 Months',
    MAX(CASE WHEN cs.interval_month = 12 THEN cs.event_date ELSE NULL END)            AS 'Latest Event Date at 12 Months EC.',

        -- 18 Months
    MAX(CASE WHEN cs.interval_month = 18 THEN cs.status ELSE NULL END)                AS 'Status at 18 Months',
    MAX(CASE WHEN cs.interval_month = 18 THEN cs.event_date ELSE NULL END)            AS 'Latest Event Date at 18 Months',
    MAX(CASE WHEN cs.interval_month = 18 THEN cs.event_date ELSE NULL END)            AS 'Latest Event Date at 18 Months EC.',

        -- 24 Months
    MAX(CASE WHEN cs.interval_month = 24 THEN cs.status ELSE NULL END)                AS 'Status at 24 Months',
    MAX(CASE WHEN cs.interval_month = 24 THEN cs.event_date ELSE NULL END)            AS 'Latest Event Date at 24 Months',
    MAX(CASE WHEN cs.interval_month = 24 THEN cs.event_date ELSE NULL END)            AS 'Latest Event Date at 24 Months EC.',

        -- 30 Months
    MAX(CASE WHEN cs.interval_month = 30 THEN cs.status ELSE NULL END)                AS 'Status at 30 Months',
    MAX(CASE WHEN cs.interval_month = 30 THEN cs.event_date ELSE NULL END)            AS 'Latest Event Date at 30 Months',
    MAX(CASE WHEN cs.interval_month = 30 THEN cs.event_date ELSE NULL END)            AS 'Latest Event Date at 30 Months EC.'

FROM HIEInCohortFiltered h
         JOIN mamba_dim_client c ON h.hei_client_id = c.client_id
         LEFT JOIN CohortSnapshot cs ON h.hei_client_id = cs.hei_client_id
         LEFT JOIN PCRInfo pcr ON h.hei_client_id = pcr.hei_client_id
GROUP BY h.hei_client_id, c.patient_name, c.patient_uuid, c.mrn, h.mother_mrn, c.Sex, c.date_of_birth, h.hei_enrollment_date
ORDER BY h.hei_enrollment_date DESC;

END //

DELIMITER ;
