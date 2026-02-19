DELIMITER //
DROP PROCEDURE IF EXISTS sp_fact_line_list_child_cohort_analysis_summary_query;

CREATE PROCEDURE sp_fact_line_list_child_cohort_analysis_summary_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE
    DATE)
BEGIN

    WITH IntervalsDef AS (SELECT 12 AS interval_month
                          UNION ALL
                          SELECT 18
                          UNION ALL
                          SELECT 24
                          UNION ALL
                          SELECT 30),

         HIEInCohort AS (SELECT hei_enrollment.encounter_id                                                                                  as hei_encounter_id,
                                maternal_enrollment.encounter_id                                                                             as mother_encounter_id,
                                linked_mother.identifier                                                                                     as mother_mrn,
                                hei_enrollment.client_id                                                                                     as hei_client_id,
                                maternal_enrollment.client_id                                                                                as mother_client_id,

                                CASE
                                    WHEN maternal_enrollment.date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                                        THEN maternal_enrollment.date_of_enrollment_or_booking
                                    ELSE hei_enrollment.date_enrolled_in_care
                                    END                                                                                                      AS effective_enrollment_date,

                                hei_enrollment.date_enrolled_in_care                                                                         as hei_enrollment_date,


                                ROW_NUMBER() OVER (PARTITION BY hei_enrollment.client_id ORDER BY hei_enrollment.date_enrolled_in_care DESC) AS row_num

                         FROM mamba_flat_encounter_hei_enrollment as hei_enrollment
                                  LEFT JOIN mamba_dim_patient_identifier as linked_mother
                                            on hei_enrollment.service_delivery_point_number = linked_mother.identifier
                                                and linked_mother.identifier_type = 'MRN'
                                  LEFT JOIN mamba_flat_encounter_pmtct_enrollment as maternal_enrollment
                                            on maternal_enrollment.client_id = linked_mother.patient_id
                                  LEFT JOIN mamba_flat_encounter_hei_final_outcome as final_outcome
                                            on hei_enrollment.client_id = final_outcome.client_id
                         WHERE (maternal_enrollment.date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE)
                            OR (hei_enrollment.date_enrolled_in_care BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                             AND (maternal_enrollment.date_of_enrollment_or_booking IS NULL
                                 OR
                                  maternal_enrollment.date_of_enrollment_or_booking NOT BETWEEN REPORT_START_DATE AND REPORT_END_DATE))),

         HIEInCohortFiltered AS (SELECT * FROM HIEInCohort WHERE row_num = 1),

         HeiIntervals AS (SELECT h.hei_client_id,
                                 h.mother_client_id,
                                 h.effective_enrollment_date,
                                 h.hei_enrollment_date,
                                 i.interval_month,
                                 DATE_ADD(h.effective_enrollment_date, INTERVAL i.interval_month
                                          MONTH) as interval_end_date
                          FROM HIEInCohortFiltered h
                                   CROSS JOIN IntervalsDef i),

         CohortHeaderCalc AS (SELECT interval_month,
                                     CASE CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(
                                                                       fn_gregorian_to_ethiopian_calendar(MAX(interval_end_date), 'Y-M-D'),
                                                                       '-', 2), '-', -1) AS UNSIGNED)
                                         WHEN 1 THEN 'Meskerem'
                                         WHEN 2 THEN 'Tikimt'
                                         WHEN 3 THEN 'Hidar'
                                         WHEN 4 THEN 'Tahsas'
                                         WHEN 5 THEN 'Tir'
                                         WHEN 6 THEN 'Yekatit'
                                         WHEN 7 THEN 'Megabit'
                                         WHEN 8 THEN 'Miyazia'
                                         WHEN 9 THEN 'Ginbot'
                                         WHEN 10 THEN 'Sene'
                                         WHEN 11 THEN 'Hamle'
                                         WHEN 12 THEN 'Nehase'
                                         WHEN 13 THEN 'Pagume'
                                         END             AS et_month,
                                     CAST(SUBSTRING_INDEX(
                                             fn_gregorian_to_ethiopian_calendar(MAX(interval_end_date), 'Y-M-D'), '-',
                                             1) AS CHAR) as et_year
                              FROM HeiIntervals
                              GROUP BY interval_month),

         HeiCohortEnriched AS (SELECT hi.hei_client_id,
                                      hi.interval_month,

                                      fn_get_ti_status(hi.mother_client_id, hi.effective_enrollment_date,
                                                       hi.interval_end_date)    as ti_status,

                                      CASE
                                          WHEN EXISTS (SELECT 1
                                                       FROM mamba_flat_encounter_follow_up_4 f4
                                                                JOIN mamba_flat_encounter_follow_up_6 f6 ON f4.encounter_id = f6.encounter_id
                                                       WHERE f4.client_id = hi.mother_client_id
                                                         AND f4.follow_up_status IN
                                                             ('Dead', 'Transferred out', 'Stop all',
                                                              'Loss to follow-up (LTFU)', 'Ran away')
                                                         AND f6.follow_up_date_followup_ BETWEEN hi.hei_enrollment_date AND hi.interval_end_date) OR
                                               EXISTS (SELECT 1
                                                       FROM mamba_flat_encounter_hei_followup hf
                                                       WHERE hf.client_id = hi.hei_client_id
                                                         AND hf.decision IN ('Lost to Follow-up', 'TO', 'Dead', 'Died')
                                                         AND hf.followup_date_followup BETWEEN hi.hei_enrollment_date AND hi.interval_end_date)
                                              THEN 1
                                          ELSE 0
                                          END                                   AS is_ltfu,

                                      (SELECT MIN(TIMESTAMPDIFF(MONTH, c.date_of_birth,
                                                                COALESCE(hf.dna_pcr_sample_collection_date,
                                                                         hf.date_dbs_result_received,
                                                                         hf.hiv_test_date)))
                                       FROM mamba_flat_encounter_hei_hiv_test hf
                                                JOIN mamba_dim_client c ON c.client_id = hi.hei_client_id
                                       WHERE hf.client_id = hi.hei_client_id
                                         AND hf.hiv_test_performed IS NOT NULL) as age_at_pcr_test,

                                      (SELECT ho.hei_pmtct_final_outcome
                                       FROM mamba_flat_encounter_hei_final_outcome ho
                                       WHERE ho.client_id = hi.hei_client_id
                                         AND ho.date_when_final_outcome_was_known <= hi.interval_end_date
                                       ORDER BY ho.date_when_final_outcome_was_known DESC
                                       LIMIT 1)                                 as final_outcome_val

                               FROM HeiIntervals hi),

         MonthlyStats AS (SELECT interval_month,

                                 SUM(CASE WHEN ti_status != 'TI' OR ti_status IS NULL THEN 1 ELSE 0 END)    as count_base,

                                 SUM(CASE WHEN ti_status = 'TI' THEN 1 ELSE 0 END)                          as count_ti,

                                 SUM(is_ltfu)                                                               as count_ltfu,

                                 SUM(CASE WHEN age_at_pcr_test < 2 THEN 1 ELSE 0 END)                       as count_pcr_lt_2,

                                 SUM(CASE WHEN age_at_pcr_test BETWEEN 2 AND 12 THEN 1 ELSE 0 END)          as count_pcr_2_12,

                                 SUM(CASE WHEN final_outcome_val = 'Discharged Negative' THEN 1 ELSE 0 END) as count_out_neg,
                                 SUM(CASE WHEN final_outcome_val = 'Positive' THEN 1 ELSE 0 END)            as count_out_pos,
                                 SUM(CASE WHEN final_outcome_val = 'Lost to Follow-up' THEN 1 ELSE 0 END)   as count_out_ltfu,
                                 SUM(CASE WHEN final_outcome_val = 'Still on BF/Exposed' THEN 1 ELSE 0 END) as count_out_bf,
                                 SUM(CASE WHEN final_outcome_val IN ('Died', 'Dead') THEN 1 ELSE 0 END)     as count_out_died,
                                 SUM(CASE WHEN final_outcome_val = 'TO' THEN 1 ELSE 0 END)                  as count_out_to

                          FROM HeiCohortEnriched
                          GROUP BY interval_month)

    SELECT 'Maternal Cohort Intervals (Ethiopian Calendar)'                                   AS Name,
           MAX(CASE WHEN interval_month = 12 THEN CONCAT(et_month, ' ', et_year) ELSE '' END) AS 'Month 12',
           MAX(CASE WHEN interval_month = 18 THEN CONCAT(et_month, ' ', et_year) ELSE '' END) AS 'Month 18',
           MAX(CASE WHEN interval_month = 24 THEN CONCAT(et_month, ' ', et_year) ELSE '' END) AS 'Month 24',
           MAX(CASE WHEN interval_month = 30 THEN CONCAT(et_month, ' ', et_year) ELSE '' END) AS 'Month 30'
    FROM CohortHeaderCalc

    UNION ALL

    SELECT 'A. Number of HEI born to HIV+mothers who enrolled in PMTCT',
           CAST(IFNULL(MAX(CASE WHEN interval_month = 12 THEN count_base END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 18 THEN count_base END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 24 THEN count_base END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 30 THEN count_base END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'B. Total number of HEI Transfer in (TI) since Month 0',
           CAST(IFNULL(MAX(CASE WHEN interval_month = 12 THEN count_ti END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 18 THEN count_ti END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 24 THEN count_ti END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 30 THEN count_ti END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'C. Number of HEI in the current cohort (A+B)',
           CAST(IFNULL(MAX(CASE WHEN interval_month = 12 THEN count_base + count_ti END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 18 THEN count_base + count_ti END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 24 THEN count_base + count_ti END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 30 THEN count_base + count_ti END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'D. HEI Lost to F/U (not seen > 1 month after scheduled appointment)',
           CAST(IFNULL(MAX(CASE WHEN interval_month = 12 THEN count_ltfu END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 18 THEN count_ltfu END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 24 THEN count_ltfu END), 0) AS SIGNED),
           0
    FROM MonthlyStats

    UNION ALL

    SELECT 'E. HEI with DNA PCR collected by 2 months of age',
           CAST(IFNULL(MAX(CASE WHEN interval_month = 12 THEN count_pcr_lt_2 END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 18 THEN count_pcr_lt_2 END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 24 THEN count_pcr_lt_2 END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 30 THEN count_pcr_lt_2 END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL
    SELECT 'E. HEI with DNA PCR collected by 2 months of age Percentage' AS Category,
           CONCAT(CAST(IFNULL(MAX(CASE WHEN interval_month = 12 THEN count_pcr_lt_2 END), 0) AS SIGNED),
                  ' (', ROUND(IFNULL(MAX(CASE WHEN interval_month = 12 THEN (count_pcr_lt_2 /(count_base + count_ti)) * 100 END), 0), 1), '%)') AS '12_Months',
            CONCAT(CAST(IFNULL(MAX(CASE WHEN interval_month = 18 THEN count_pcr_lt_2 END), 0) AS SIGNED),
                ' (', ROUND(IFNULL(MAX(CASE WHEN interval_month = 18 THEN (count_pcr_lt_2 / (count_base + count_ti)) * 100 END), 0), 1), '%)') AS '18_Months',
            CONCAT(CAST(IFNULL(MAX(CASE WHEN interval_month = 24 THEN count_pcr_lt_2 END), 0) AS SIGNED),
                ' (', ROUND(IFNULL(MAX(CASE WHEN interval_month = 24 THEN (count_pcr_lt_2 / (count_base + count_ti)) * 100 END), 0), 1), '%)') AS '24_Months',
            CONCAT(CAST(IFNULL(MAX(CASE WHEN interval_month = 30 THEN count_pcr_lt_2 END), 0) AS SIGNED),
                ' (', ROUND(IFNULL(MAX(CASE WHEN interval_month = 30 THEN (count_pcr_lt_2 / (count_base + count_ti)) * 100 END), 0), 1), '%)') AS '30_Months'
    FROM MonthlyStats

    UNION ALL

    SELECT 'F. HEI with DNA PCR collected between 2 and 12 months of age',
           CAST(IFNULL(MAX(CASE WHEN interval_month = 12 THEN count_pcr_2_12 END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 18 THEN count_pcr_2_12 END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 24 THEN count_pcr_2_12 END), 0) AS SIGNED),
           CAST(IFNULL(MAX(CASE WHEN interval_month = 30 THEN count_pcr_2_12 END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL
    SELECT 'F. HEI with DNA PCR collected between 2 and 12 months of age Percentage',
           -- 12 Months
           CONCAT(CAST(IFNULL(MAX(CASE WHEN interval_month = 12 THEN count_pcr_2_12 END), 0) AS SIGNED),
                  ' (', ROUND(IFNULL(MAX(CASE WHEN interval_month = 12 THEN (count_pcr_2_12 / (count_base + count_ti)) * 100 END), 0), 1), '%)'),
           -- 18 Months
           CONCAT(CAST(IFNULL(MAX(CASE WHEN interval_month = 18 THEN count_pcr_2_12 END), 0) AS SIGNED),
                  ' (', ROUND(IFNULL(MAX(CASE WHEN interval_month = 18 THEN (count_pcr_2_12 / (count_base + count_ti)) * 100 END), 0), 1), '%)'),
           -- 24 Months
           CONCAT(CAST(IFNULL(MAX(CASE WHEN interval_month = 24 THEN count_pcr_2_12 END), 0) AS SIGNED),
                  ' (', ROUND(IFNULL(MAX(CASE WHEN interval_month = 24 THEN (count_pcr_2_12 / (count_base + count_ti)) * 100 END), 0), 1), '%)'),
           -- 30 Months
           CONCAT(CAST(IFNULL(MAX(CASE WHEN interval_month = 30 THEN count_pcr_2_12 END), 0) AS SIGNED),
                  ' (', ROUND(IFNULL(MAX(CASE WHEN interval_month = 30 THEN (count_pcr_2_12 / (count_base + count_ti)) * 100 END), 0), 1), '%)')
    FROM MonthlyStats

    UNION ALL

    SELECT 'G. HEI discharged negative (DN)',
           0,
           0,
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 30 THEN count_out_neg END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'H. HEI diagnosed positive (p)',
           0,
           0,
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 30 THEN count_out_pos END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'I. Final HEI Lost to F/U',
           0,
           0,
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 30 THEN count_out_ltfu END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'J. HEI still exposed /breastfeeding (CPT)',
           0,
           0,
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 30 THEN count_out_bf END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'K. HEI known dead',
           0,
           0,
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 30 THEN count_out_died END), 0) AS SIGNED)
    FROM MonthlyStats

    UNION ALL

    SELECT 'L. HEI transferred out (TO to another facility)',
           0,
           0,
           0,
           CAST(IFNULL(MAX(CASE WHEN interval_month = 30 THEN count_out_to END), 0) AS SIGNED)
    FROM MonthlyStats;

END //
DELIMITER ;
