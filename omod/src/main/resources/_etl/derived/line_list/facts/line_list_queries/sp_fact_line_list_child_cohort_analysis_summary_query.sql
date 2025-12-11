DELIMITER //
DROP PROCEDURE IF EXISTS sp_fact_line_list_child_cohort_analysis_summary_query;

CREATE PROCEDURE sp_fact_line_list_child_cohort_analysis_summary_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE
    DATE)
BEGIN

    WITH HeiEnrolled AS (SELECT h.encounter_id                         as hei_encounter_id,
                                pm.encounter_id                        as mother_encounter_id,
                                client.identifier                      as mother_mrn,
                                h.client_id                            as hei_client_id,
                                pm.client_id                           as mother_client_id,
                                pm.date_of_enrollment_or_booking as date_of_erollement,
                                h.date_enrolled_in_care                as hei_enrollment_date,

                                h.infant_referred                      as infant_referred,
                                ho.hei_pmtct_final_outcome             as final_out_come
                         FROM mamba_flat_encounter_hei_enrollment as h
                                  LEFT JOIN mamba_dim_patient_identifier as client
                                            on h.service_delivery_point_number = client.identifier
                                                and client.identifier_type = 'MRN'
                                  LEFT JOIN mamba_flat_encounter_pmtct_enrollment
                             as pm on pm.client_id = client.patient_id
                                  LEFT JOIN mamba_flat_encounter_follow_up as f
                                            on f.client_id = pm.client_id
                                  LEFT JOIN mamba_flat_encounter_follow_up_7 as f7
                                            on f7.encounter_id = f.encounter_id
                                  LEFT JOIN mamba_flat_encounter_follow_up_5 as f5
                                            on f5.encounter_id = f.encounter_id
                                  LEFT JOIN mamba_flat_encounter_hei_final_outcome as ho
                                            on h.client_id = ho.client_id
                         WHERE (client.identifier is null and (h.date_enrolled_in_care between REPORT_START_DATE
                             and REPORT_END_DATE))
                            or (client.identifier is not null
                             and (pm.date_of_enrollment_or_booking between
                                 REPORT_START_DATE and REPORT_END_DATE)
                             and
                                h.date_enrolled_in_care between REPORT_START_DATE and DATE_ADD(REPORT_START_DATE, INTERVAL 12 MONTH))),

         -- Number of HEI born to HIV+ mothers who enrolled in PMTCT during this cohort month/year.
         HIEInCohort AS (SELECT a.*
                              , ROW_NUMBER() OVER (PARTITION BY a.hei_client_id ORDER BY a.hei_enrollment_date DESC) AS row_num
                         FROM HeiEnrolled a
                         WHERE hei_enrollment_date is not null),

         HIEInCohortFiltered AS (SELECT * FROM HIEInCohort WHERE row_num = 1),


         -- Defines the cohort analysis intervals in months.
         IntervalsDef AS (SELECT 12 AS interval_month
                          UNION ALL
                          SELECT 18
                          UNION ALL
                          SELECT 24
                          UNION ALL
                          SELECT 30),

         -- Creates an evaluation point for each patient at each interval.
         TransferedInHEI AS (SELECT a.hei_client_id,
                                    a.date_of_erollement,
                                    i.interval_month,
                                    fn_get_ti_status(a.mother_client_id, a.date_of_erollement,
                                                     DATE_ADD(REPORT_START_DATE,
                                                              INTERVAL i.interval_month MONTH)
                                    )                                   as ti_status,
                                    DATE_ADD(REPORT_START_DATE,
                                             INTERVAL i
                                                 .interval_month MONTH) as interval_end_date
                             FROM HIEInCohortFiltered a
                                      CROSS JOIN IntervalsDef i),

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
                                REPORT_START_DATE and DATE_ADD(REPORT_START_DATE,
                                                               INTERVAL i.interval_month MONTH)

                            union all

                            SELECT h.hei_client_id, h.mother_client_id, i.interval_month
                            from mamba_flat_encounter_hei_followup
                                     as hf
                                     inner join HIEInCohortFiltered as h on h.hei_client_id = hf.client_id
                                     cross join IntervalsDef as i
                            where hf.decision in ('Lost to Follow-up', 'TO', 'Dead', 'Died')
                              and hf.followup_date_followup between
                                REPORT_START_DATE and DATE_ADD(REPORT_START_DATE,
                                                               INTERVAL i.interval_month MONTH)),
         PCRTestBelowTwoYear as (select h.hei_client_id
                                 from mamba_flat_encounter_hei_hiv_test
                                          as hf
                                          inner join HIEInCohortFiltered as h on h.hei_client_id = hf.client_id
                                          inner join mamba_dim_client as c on c.client_id = h.hei_client_id
                                 where hf.hiv_test_performed is not null
                                   and DATE_SUB(h.date_of_erollement, interval 1 YEAR) > REPORT_END_DATE
                                   and TIMESTAMPDIFF(month, c.date_of_birth,
                                                     COALESCE(hf.date_dbs_result_received, CURDATE())) between 2 and
                                     24),

         PCRTestAboveTwoYear as (select h.hei_client_id
                                 from mamba_flat_encounter_hei_hiv_test
                                          as hf
                                          inner join HIEInCohortFiltered as h on h.hei_client_id = hf.client_id
                                          inner join mamba_dim_client as c on c.client_id = h.hei_client_id
                                 where hf.hiv_test_performed is not null
                                   and DATE_SUB(h.date_of_erollement, interval 1 YEAR) > REPORT_END_DATE
                                   and TIMESTAMPDIFF(YEAR, c.date_of_birth,
                                                     COALESCE(date_dbs_result_received, CURDATE())) >= 2),

         FinalOutComeHIE as (select hf.client_id, hf.hei_pmtct_final_outcome
                             from mamba_flat_encounter_hei_final_outcome hf
                                      inner join
                                  HIEInCohortFiltered as hi on hf.client_id = hi.hei_client_id
                             where DATE_ADD(hi.hei_enrollment_date, INTERVAL 30 MONTH) <= hf
                                 .date_when_final_outcome_was_known)


    select 'A. Number of HEI born to HIV+mothers who enrolled in PMTCT during this cohort month/year:' as name,
           count(h.hei_client_id)                                                                      as ' Maternal Cohort Month 12',

           ''                                                                                          AS 'Maternal Cohort Month18',
           ''                                                                                          AS 'Maternal Cohort Month 24',
           ''                                                                                          AS 'Maternal Cohort Month 30'

    from HIEInCohortFiltered as h
    union all

    select 'B. Total number of HEI Transfer in (TI) since Month 0:' as name,
           CAST(IFNULL(SUM(CASE WHEN h.interval_month = 12 and h.ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED)                                as ' Maternal Cohort Month 12',

           CAST(IFNULL(SUM(CASE WHEN h.interval_month = 18 and h.ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED)
                                                                    AS 'Maternal Cohort Month  18',
           CAST(IFNULL(SUM(CASE WHEN h.interval_month = 24 and h.ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED)                                AS 'Maternal Cohort Month 24',
           CAST(IFNULL(SUM(CASE WHEN h.interval_month = 30 and h.ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED)                                AS 'Maternal Cohort Month 30'

    from TransferedInHEI as h

    UNION all

    select 'C. Number of HEI in the current cohort (A+B):'     as name,
           CAST(IFNULL(SUM(CASE WHEN h.interval_month = 12 and h.ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED) + COUNT(hf.hei_client_id) as ' Maternal Cohort Month 12',

           CAST(IFNULL(SUM(CASE WHEN h.interval_month = 18 and h.ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED) + COUNT(hf.hei_client_id)
                                                               AS 'Maternal Cohort Month  18',
           CAST(IFNULL(SUM(CASE WHEN h.interval_month = 24 and h.ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED) + COUNT(hf.hei_client_id) AS 'Maternal Cohort Month 24',
           CAST(IFNULL(SUM(CASE WHEN h.interval_month = 30 and h.ti_status = 'TI' THEN 1 ELSE 0 END),
                       0) AS SIGNED) + COUNT(hf.hei_client_id) AS 'Maternal Cohort Month 30'

    from TransferedInHEI as h
             inner join
         HIEInCohortFiltered as hf on hf.hei_client_id = h.hei_client_id
    union all

    select 'D. HEI Lost to F/U (not seen > 1 month after scheduled appointment)' as name,
           CAST(IFNULL(SUM(CASE WHEN h.interval_month = 12 THEN 1 ELSE 0 END),
                       0) AS SIGNED)                                             as 'Maternal Cohort Month 12',

           CAST(IFNULL(SUM(CASE WHEN h.interval_month = 18 THEN 1 ELSE 0 END),
                       0) AS SIGNED)
                                                                                 AS 'Maternal Cohort Month  18',
           CAST(IFNULL(SUM(CASE WHEN h.interval_month = 24 THEN 1 ELSE 0 END),
                       0) AS SIGNED)                                             AS 'Maternal Cohort Month 24',
           CAST(IFNULL(SUM(CASE WHEN h.interval_month = 30 THEN 1 ELSE 0 END),
                       0) AS SIGNED)                                             AS 'Maternal Cohort Month 30'

    from LostToFollowUp as h
    union all

    select 'E. HEI with DNA PCR collected by 2 months of age' as name,
           count(h.hei_client_id)                             as 'Maternal Cohort Month 12',

           ''                                                 AS 'Maternal Cohort Month18',
           ''                                                 AS 'Maternal Cohort Month 24',
           ''                                                 AS 'Maternal Cohort Month 30'


    from PCRTestBelowTwoYear as h
    union all
    select 'F. HEI with DNA PCR collected between 2 and 12 months of age' as name,
           count(h.hei_client_id)                                         as 'Maternal Cohort Month 12',

           ''                                                             AS 'Maternal Cohort Month 18',
           ''                                                             AS 'Maternal Cohort Month 24',
           ''                                                             AS 'Maternal Cohort Month 30'


    from PCRTestAboveTwoYear as h
    union all

    select 'G. HEI discharged negative (DN)' as name,
           ''                                as 'Maternal Cohort Month 12',

           ''                                AS 'Maternal Cohort Month 18',
           ''                                AS 'Maternal Cohort Month 24',
           count(h.client_id)                AS 'Maternal Cohort Month 30'


    from FinalOutComeHIE as h
    where h.hei_pmtct_final_outcome = 'Discharged Negative'
    union all

    select 'H. HEI diagnosed positive (p)' as name,
           ''                              as 'Maternal Cohort Month 12',

           ''                              AS 'Maternal Cohort Month 18',
           ''                              AS 'Maternal Cohort Month 24',
           count(h.client_id)              AS 'Maternal Cohort Month 30'


    from FinalOutComeHIE as h
    where h.hei_pmtct_final_outcome = 'Positive'
    union all
    select 'I. Final HEI Lost to F/U' as name,
           ''                         as ' Maternal Cohort Month 12',

           ''                         AS 'Maternal Cohort Month18',
           ''                         AS 'Maternal Cohort Month 24',
           count(h.client_id)         AS 'Maternal Cohort Month 30'


    from FinalOutComeHIE as h
    where h.hei_pmtct_final_outcome = 'Lost to Follow-up'
    union all
    select 'J. HEI still exposed /breastfeeding (CPT)' as name,
           ''                                          as ' Maternal Cohort Month 12',

           ''                                          AS 'Maternal Cohort Month18',
           ''                                          AS 'Maternal Cohort Month 24',
           count(h.client_id)                          AS 'Maternal Cohort Month 30'


    from FinalOutComeHIE as h
    where h.hei_pmtct_final_outcome = 'Still on BF/Exposed'
    union all
    select 'k. HEI known dead' as name,
           ''                  as ' Maternal Cohort Month 12',

           ''                  AS 'Maternal Cohort Month18',
           ''                  AS 'Maternal Cohort Month 24',
           count(h.client_id)  AS 'Maternal Cohort Month 30'


    from FinalOutComeHIE as h
    where h.hei_pmtct_final_outcome = 'Died'

    union all
    select 'L. HEI transferred out (TO to another facility-NOT to ART clinic) ' as name,
           ''                                                                   as ' Maternal Cohort Month 12',

           ''                                                                   AS 'Maternal Cohort Month18',
           ''                                                                   AS 'Maternal Cohort Month 24',
           count(h.client_id)                                                   AS 'Maternal Cohort Month 30'


    from FinalOutComeHIE as h
    where h.hei_pmtct_final_outcome = 'TO';

END //
DELIMITER ;




