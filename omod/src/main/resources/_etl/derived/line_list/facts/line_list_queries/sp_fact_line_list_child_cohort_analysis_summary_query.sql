DELIMITER //
DROP PROCEDURE IF EXISTS sp_fact_line_list_child_cohort_analysis_summary_query;

CREATE PROCEDURE sp_fact_line_list_child_cohort_analysis_summary_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE
    DATE)
BEGIN

    -- 1. Base Cohort: HEI born to HIV+ mothers enrolled in PMTCT
    WITH HeiEnrolled AS (SELECT hei_enrollment.encounter_id                         as hei_encounter_id,
                                maternal_enrollment.encounter_id                        as mother_encounter_id,
                                linked_mother.identifier                      as mother_mrn,
                                hei_enrollment.client_id                            as hei_client_id,
                                maternal_enrollment.client_id                           as mother_client_id,
                                maternal_enrollment.date_of_enrollment_or_booking       as date_of_erollement,
                                hei_enrollment.date_enrolled_in_care                as hei_enrollment_date,
                                hei_enrollment.infant_referred                      as infant_referred,
                                final_outcome.hei_pmtct_final_outcome             as final_out_come
                         FROM mamba_flat_encounter_hei_enrollment as hei_enrollment
                                  LEFT JOIN mamba_dim_patient_identifier as linked_mother
                                            on hei_enrollment.service_delivery_point_number = linked_mother.identifier
                                                and linked_mother.identifier_type = 'MRN'
                                  LEFT JOIN mamba_flat_encounter_pmtct_enrollment as maternal_enrollment
                                            on maternal_enrollment.client_id = linked_mother.patient_id
                                  LEFT JOIN mamba_flat_encounter_hei_final_outcome as final_outcome
                                            on hei_enrollment.client_id = final_outcome.client_id
                         WHERE (linked_mother.identifier is null and (hei_enrollment.date_enrolled_in_care between REPORT_START_DATE
                             and REPORT_END_DATE))
                            or (linked_mother.identifier is not null
                             and (maternal_enrollment.date_of_enrollment_or_booking between
                                 REPORT_START_DATE and REPORT_END_DATE)
                             and
                                hei_enrollment.date_enrolled_in_care between REPORT_START_DATE and DATE_ADD(REPORT_START_DATE, INTERVAL 12 MONTH))),

         -- Deduplicate HEI in Cohort
         HIEInCohort AS (SELECT a.*
                              , ROW_NUMBER() OVER (PARTITION BY a.hei_client_id ORDER BY a.hei_enrollment_date DESC) AS row_num
                         FROM HeiEnrolled a
                         WHERE hei_enrollment_date is not null),

         HIEInCohortFiltered AS (SELECT * FROM HIEInCohort WHERE row_num = 1),

         -- 2. Define Intervals (12, 18, 24, 30 Months)
         IntervalsDef AS (SELECT 12 AS interval_month
                          UNION ALL
                          SELECT 18
                          UNION ALL
                          SELECT 24
                          UNION ALL
                          SELECT 30),

         -- 3. Transfer In (TI) Logic (Relative to HEI Enrollment)
         TransferedInHEI AS (SELECT a.hei_client_id,
                                    a.date_of_erollement,
                                    i.interval_month,
                                    fn_get_ti_status(a.mother_client_id, a.date_of_erollement,
                                                     DATE_ADD(a.hei_enrollment_date,
                                                              INTERVAL i.interval_month MONTH)
                                    )                                   as ti_status,
                                    DATE_ADD(a.hei_enrollment_date,
                                             INTERVAL i
                                                 .interval_month MONTH) as interval_end_date
                             FROM HIEInCohortFiltered a
                                      CROSS JOIN IntervalsDef i),

         -- 4. Lost to Follow Up Logic (Relative to HEI Enrollment)
         LostToFollowUp AS (select h.hei_client_id, h.mother_client_id, i.interval_month
                            from mamba_flat_encounter_follow_up_4 as f
                                     inner join HIEInCohortFiltered as h on f.client_id = h.mother_client_id
                                     inner join mamba_flat_encounter_follow_up_6 as f6
                                                on f6.encounter_id = f.encounter_id
                                     cross join IntervalsDef as i
                            where f.follow_up_status in ('Dead', 'Transferred out',
                                                         'Stop all', 'Loss to follow-up (LTFU)',
                                                         'Ran away')
                              and f6.follow_up_date_followup_ between
                                h.hei_enrollment_date and DATE_ADD(h.hei_enrollment_date,
                                                                   INTERVAL i.interval_month MONTH)

                            union all

                            SELECT h.hei_client_id, h.mother_client_id, i.interval_month
                            from mamba_flat_encounter_hei_followup
                                     as hf
                                     inner join HIEInCohortFiltered as h on h.hei_client_id = hf.client_id
                                     cross join IntervalsDef as i
                            where hf.decision in ('Lost to Follow-up', 'TO', 'Dead', 'Died')
                              and hf.followup_date_followup between
                                h.hei_enrollment_date and DATE_ADD(h.hei_enrollment_date,
                                                                   INTERVAL i.interval_month MONTH)),

         -- 5. PCR Logic: Age < 2 months
         PCRTestBelowTwoYear as (select distinct h.hei_client_id
                                 from mamba_flat_encounter_hei_hiv_test
                                          as hf
                                          inner join HIEInCohortFiltered as h on h.hei_client_id = hf.client_id
                                          inner join mamba_dim_client as c on c.client_id = h.hei_client_id
                                 where hf.hiv_test_performed is not null
                                   and TIMESTAMPDIFF(MONTH, c.date_of_birth,
                                                     COALESCE(hf.dna_pcr_sample_collection_date, hf.date_dbs_result_received, hf.hiv_test_date)) < 2),

         -- 6. PCR Logic: Age between 2 and 12 months
         PCRTestAboveTwoYear as (select distinct h.hei_client_id
                                 from mamba_flat_encounter_hei_hiv_test
                                          as hf
                                          inner join HIEInCohortFiltered as h on h.hei_client_id = hf.client_id
                                          inner join mamba_dim_client as c on c.client_id = h.hei_client_id
                                 where hf.hiv_test_performed is not null
                                   and TIMESTAMPDIFF(MONTH, c.date_of_birth,
                                                     COALESCE(hf.dna_pcr_sample_collection_date, hf.date_dbs_result_received, hf.hiv_test_date)) BETWEEN 2 AND 12),

         -- 7. Final Outcomes with Interval Logic
         FinalOutcomeIntervals AS (
             SELECT h.hei_client_id, ho.hei_pmtct_final_outcome, i.interval_month
             FROM HIEInCohortFiltered h
                      JOIN mamba_flat_encounter_hei_final_outcome ho ON h.hei_client_id = ho.client_id
                      CROSS JOIN IntervalsDef i
             WHERE ho.hei_pmtct_final_outcome IS NOT NULL
               AND ho.date_when_final_outcome_was_known <= DATE_ADD(h.hei_enrollment_date, INTERVAL i.interval_month MONTH)
         )


    -- A. Number of HEI born to HIV+mothers who enrolled in PMTCT
    select 'A. Number of HEI born to HIV+mothers who enrolled in PMTCT during this cohort month/year:' as name,
           CAST(COALESCE(count(h.hei_client_id), 0) AS SIGNED)                                         as ' Maternal Cohort Month 12',
           CAST(COALESCE(count(h.hei_client_id), 0) AS SIGNED)                                         AS 'Maternal Cohort Month18',
           CAST(COALESCE(count(h.hei_client_id), 0) AS SIGNED)                                         AS 'Maternal Cohort Month 24',
           CAST(COALESCE(count(h.hei_client_id), 0) AS SIGNED)                                         AS 'Maternal Cohort Month 30'
    from HIEInCohortFiltered as h

    union all

    -- B. Transfer In
    select 'B. Total number of HEI Transfer in (TI) since Month 0:' as name,
           CAST(COALESCE(SUM(CASE WHEN h.interval_month = 12 and h.ti_status = 'TI' THEN 1 ELSE 0 END), 0) AS SIGNED) as ' Maternal Cohort Month 12',
           CAST(COALESCE(SUM(CASE WHEN h.interval_month = 18 and h.ti_status = 'TI' THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Maternal Cohort Month  18',
           CAST(COALESCE(SUM(CASE WHEN h.interval_month = 24 and h.ti_status = 'TI' THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Maternal Cohort Month 24',
           CAST(COALESCE(SUM(CASE WHEN h.interval_month = 30 and h.ti_status = 'TI' THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Maternal Cohort Month 30'
    from TransferedInHEI as h

    UNION all

    -- C. Current Cohort (A+B)
    select 'C. Number of HEI in the current cohort (A+B):'     as name,
           CAST(COALESCE(SUM(CASE WHEN h.interval_month = 12 and h.ti_status = 'TI' THEN 1 ELSE 0 END), 0) AS SIGNED) + (SELECT COUNT(*) FROM HIEInCohortFiltered) as ' Maternal Cohort Month 12',
           CAST(COALESCE(SUM(CASE WHEN h.interval_month = 18 and h.ti_status = 'TI' THEN 1 ELSE 0 END), 0) AS SIGNED) + (SELECT COUNT(*) FROM HIEInCohortFiltered) AS 'Maternal Cohort Month  18',
           CAST(COALESCE(SUM(CASE WHEN h.interval_month = 24 and h.ti_status = 'TI' THEN 1 ELSE 0 END), 0) AS SIGNED) + (SELECT COUNT(*) FROM HIEInCohortFiltered) AS 'Maternal Cohort Month 24',
           CAST(COALESCE(SUM(CASE WHEN h.interval_month = 30 and h.ti_status = 'TI' THEN 1 ELSE 0 END), 0) AS SIGNED) + (SELECT COUNT(*) FROM HIEInCohortFiltered) AS 'Maternal Cohort Month 30'
    from TransferedInHEI as h

    union all

    -- D. Lost to F/U (Using COUNT DISTINCT to avoid double counting from UNION ALL)
    select 'D. HEI Lost to F/U (not seen > 1 month after scheduled appointment)' as name,
           CAST(COALESCE(COUNT(DISTINCT CASE WHEN h.interval_month = 12 THEN h.hei_client_id END), 0) AS SIGNED) as 'Maternal Cohort Month 12',
           CAST(COALESCE(COUNT(DISTINCT CASE WHEN h.interval_month = 18 THEN h.hei_client_id END), 0) AS SIGNED) AS 'Maternal Cohort Month  18',
           CAST(COALESCE(COUNT(DISTINCT CASE WHEN h.interval_month = 24 THEN h.hei_client_id END), 0) AS SIGNED) AS 'Maternal Cohort Month 24',
           0 AS 'Maternal Cohort Month 30'
    from LostToFollowUp as h

    union all

    -- E. PCR < 2 Months
    select 'E. HEI with DNA PCR collected by 2 months of age' as name,
           CAST(COALESCE(count(h.hei_client_id), 0) AS SIGNED)                as 'Maternal Cohort Month 12',
           CAST(COALESCE(count(h.hei_client_id), 0) AS SIGNED)                AS 'Maternal Cohort Month 18',
           CAST(COALESCE(count(h.hei_client_id), 0) AS SIGNED)                AS 'Maternal Cohort Month 24',
           CAST(COALESCE(count(h.hei_client_id), 0) AS SIGNED)                AS 'Maternal Cohort Month 30'
    from PCRTestBelowTwoYear as h

    union all

    -- F. PCR 2-12 Months
    select 'F. HEI with DNA PCR collected between 2 and 12 months of age' as name,
           CAST(COALESCE(count(h.hei_client_id), 0) AS SIGNED)                as 'Maternal Cohort Month 12',
           CAST(COALESCE(count(h.hei_client_id), 0) AS SIGNED)                AS 'Maternal Cohort Month 18',
           CAST(COALESCE(count(h.hei_client_id), 0) AS SIGNED)                AS 'Maternal Cohort Month 24',
           CAST(COALESCE(count(h.hei_client_id), 0) AS SIGNED)                AS 'Maternal Cohort Month 30'
    from PCRTestAboveTwoYear as h

    union all

    -- G. Discharged Negative
    select 'G. HEI discharged negative (DN)' as name,
           0,
           0,
           0,
           CAST(COALESCE(SUM(CASE WHEN h.interval_month = 30 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Maternal Cohort Month 30'
    from FinalOutcomeIntervals as h
    where h.hei_pmtct_final_outcome = 'Discharged Negative'

    union all

    -- H. Diagnosed Positive
    select 'H. HEI diagnosed positive (p)' as name,
           0,
           0,
           0,
           CAST(COALESCE(SUM(CASE WHEN h.interval_month = 30 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Maternal Cohort Month 30'
    from FinalOutcomeIntervals as h
    where h.hei_pmtct_final_outcome = 'Positive'

    union all

    -- I. Final Lost to F/U
    select 'I. Final HEI Lost to F/U' as name,
           0,
           0,
           0,
           CAST(COALESCE(SUM(CASE WHEN h.interval_month = 30 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Maternal Cohort Month 30'
    from FinalOutcomeIntervals as h
    where h.hei_pmtct_final_outcome = 'Lost to Follow-up'

    union all

    -- J. Still Exposed/Breastfeeding
    select 'J. HEI still exposed /breastfeeding (CPT)' as name,
           0,
           0,
           0,
           CAST(COALESCE(SUM(CASE WHEN h.interval_month = 30 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Maternal Cohort Month 30'
    from FinalOutcomeIntervals as h
    where h.hei_pmtct_final_outcome = 'Still on BF/Exposed'

    union all

    -- K. Known Dead
    select 'k. HEI known dead' as name,
           0,
           0,
           0,
           CAST(COALESCE(SUM(CASE WHEN h.interval_month = 30 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Maternal Cohort Month 30'
    from FinalOutcomeIntervals as h
    where h.hei_pmtct_final_outcome = 'Died'

    union all

    -- L. Transferred Out
    select 'L. HEI transferred out (TO to another facility-NOT to ART clinic) ' as name,
           0,
           0,
           0,
           CAST(COALESCE(SUM(CASE WHEN h.interval_month = 30 THEN 1 ELSE 0 END), 0) AS SIGNED) AS 'Maternal Cohort Month 30'
    from FinalOutcomeIntervals as h
    where h.hei_pmtct_final_outcome = 'TO';

END //
DELIMITER ;
