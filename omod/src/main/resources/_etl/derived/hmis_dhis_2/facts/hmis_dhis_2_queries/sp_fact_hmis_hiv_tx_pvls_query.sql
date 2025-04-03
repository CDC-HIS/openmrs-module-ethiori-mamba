DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_tx_pvls_query;

CREATE PROCEDURE sp_fact_hmis_hiv_tx_pvls_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

WITH FollowUp AS (select follow_up.encounter_id,
                         follow_up.client_id,
                         follow_up_status,
                         follow_up_date_followup_            AS follow_up_date,
                         art_antiretroviral_start_date       AS art_start_date,
                         treatment_end_date,
                         next_visit_date,
                         regimen,
                         currently_breastfeeding_child          breast_feeding_status,
                         pregnancy_status,
                         transferred_in_check_this_for_all_t as transferred_in,
                         date_viral_load_results_received    as viral_load_performed_date,
                         hiv_viral_load                      as viral_load_count,
                         viral_load_test_status,
                         viral_load_test_indication,
                         viral_load_received_                as viral_load_performed,
                         date_of_reported_hiv_viral_load     as viral_load_sent_date
                  FROM mamba_flat_encounter_follow_up follow_up
                           JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                ON follow_up.encounter_id = follow_up_1.encounter_id
                           JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                ON follow_up.encounter_id = follow_up_2.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                     ON follow_up.encounter_id = follow_up_3.encounter_id),
     tmp_latest_follow_up AS (SELECT client_id,
                                     follow_up_date                                                                             AS FollowupDate,
                                     encounter_id,
                                     follow_up_status,
                                     treatment_end_date,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND art_start_date IS NOT NULL
                                AND follow_up_date <= REPORT_END_DATE),
     latest_follow_up AS (select *
                          from tmp_latest_follow_up
                          where row_num = 1

     ),
     tmp_pvls as (select encounter_id,
                         client_id,
                         follow_up_status,
                         follow_up_date,
                         viral_load_performed_date,
                         pregnancy_status,
                         viral_load_count,
                         viral_load_test_status,
                         viral_load_sent_date,
                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_performed_date DESC, encounter_id DESC) AS row_num
                  from FollowUp
                  where viral_load_performed_date is not null
                    and viral_load_performed_date >= REPORT_START_DATE
                    AND viral_load_performed_date <= REPORT_END_DATE),
     tmp_pvls_2 as (select * from tmp_pvls where row_num = 1),
     pvls as (select client.client_id,
                     pregnancy_status,
                     sex,
                     date_of_birth,
                     viral_load_count,
                     viral_load_test_status,
                     follow_up_date
              from tmp_pvls_2
                       inner join mamba_dim_client client on tmp_pvls_2.client_id = client.client_id
                       inner join latest_follow_up on tmp_pvls_2.client_id = latest_follow_up.client_id)
        ,
     pvls_percentage AS (SELECT 'HIV_TX_PVLS'                                                                                                                                            AS S_NO,
                                'Viral load Suppression (Percentage of ART clients with a suppressed viral load among those with a viral load test at 12 month in the reporting period)' AS Activity,
                                CASE
                                    WHEN (SELECT COUNT(*) FROM pvls) = 0 THEN
                                        '0'
                                    ELSE
                                        CAST(ROUND(
                                                (CAST((SELECT COUNT(*)
                                                       FROM pvls
                                                       WHERE ((viral_load_count < 50 OR viral_load_count IS NULL) AND
                                                              viral_load_test_status = 'Suppressed')
                                                          OR (viral_load_count BETWEEN 50 AND 1000)) AS REAL) * 100.0) /
                                                (SELECT COUNT(*) FROM pvls)
                                            , 2) AS CHAR)
                                    END                                                                                                                                                  AS Value
FROM (SELECT 1) AS dummy -- Added a dummy table to ensure the CTE returns at least one row
    )

SELECT S_NO,
       Activity,
       Value
FROM pvls_percentage

UNION ALL
SELECT 'HIV_TX_PVLS.1'                                                                                                                            AS S_NO,
       'Number of adult and pediatric ART patients for whom viral load test result received in the reporting period (with in the past 12 months)' as Activity,
       COUNT(*)                                                                                                                                   as Value
FROM pvls

UNION ALL
SELECT 'HIV_TX_PVLS.1. 1' AS S_NO,
       '< 1 year, Male'   as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) < 1
  AND sex = 'Male'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 3'                AS S_NO,
       '< 1 year, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) < 1
  AND sex = 'Female'


UNION ALL
SELECT 'HIV_TX_PVLS.1. 4'  AS S_NO,
       '1 - 4 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 1 AND 4
  AND sex = 'Male'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 6'                   AS S_NO,
       '1 - 4 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 1 AND 4
  AND sex = 'Female'


UNION ALL
SELECT 'HIV_TX_PVLS.1. 7'  AS S_NO,
       '5 - 9 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 5 AND 9
  AND sex = 'Male'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 9'                   AS S_NO,
       '5 - 9 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 5 AND 9
  AND sex = 'Female'


UNION ALL
SELECT 'HIV_TX_PVLS.1. 10'   AS S_NO,
       '10 - 14 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 10 AND 14
  AND sex = 'Male'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 12'                    AS S_NO,
       '10 - 14 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 10 AND 14
  AND sex = 'Female'


UNION ALL
SELECT 'HIV_TX_PVLS.1. 13'   AS S_NO,
       '15 - 19 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 15 AND 19
  AND sex = 'Male'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 14'                AS S_NO,
       '15 - 19 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 15 AND 19
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 15'                    AS S_NO,
       '15 - 19 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 15 AND 19
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)

UNION ALL
SELECT 'HIV_TX_PVLS.1. 16'   AS S_NO,
       '20 - 24 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 20 AND 24
  AND sex = 'Male'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 17'                AS S_NO,
       '20 - 24 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 20 AND 24
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 18'                    AS S_NO,
       '20 - 24 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 20 AND 24
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)

UNION ALL
SELECT 'HIV_TX_PVLS.1. 19'   AS S_NO,
       '25 - 29 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 25 AND 29
  AND sex = 'Male'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 20'                AS S_NO,
       '25 - 29 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 25 AND 29
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 21'                    AS S_NO,
       '25 - 29 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 25 AND 29
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)

UNION ALL
SELECT 'HIV_TX_PVLS.1. 22'   AS S_NO,
       '30 - 34 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 30 AND 34
  AND sex = 'Male'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 23'                AS S_NO,
       '30 - 34 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 30 AND 34
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 24'                    AS S_NO,
       '30 - 34 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 30 AND 34
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)

UNION ALL
SELECT 'HIV_TX_PVLS.1. 25'   AS S_NO,
       '35 - 39 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 35 AND 39
  AND sex = 'Male'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 26'                AS S_NO,
       '35 - 39 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 35 AND 39
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 27'                    AS S_NO,
       '35 - 39 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 35 AND 39
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)

UNION ALL
SELECT 'HIV_TX_PVLS.1. 28'   AS S_NO,
       '40 - 44 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 40 AND 44
  AND sex = 'Male'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 29'                AS S_NO,
       '40 - 44 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 40 AND 44
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 30'                    AS S_NO,
       '40 - 44 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 40 AND 44
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)

UNION ALL
SELECT 'HIV_TX_PVLS.1. 31'   AS S_NO,
       '45 - 49 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 45 AND 49
  AND sex = 'Male'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 32'                AS S_NO,
       '45 - 49 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 45 AND 49
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 33'                    AS S_NO,
       '45 - 49 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 45 AND 49
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)

UNION ALL
SELECT 'HIV_TX_PVLS.1. 34' AS S_NO,
       '>= 50 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) >= 50
  AND sex = 'Male'

UNION ALL
SELECT 'HIV_TX_PVLS.1. 36'                  AS S_NO,
       '>= 50 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) >= 50
  AND sex = 'Female'


UNION ALL
SELECT 'HIV_TX_PVLS_UN'                                                                                                                                         AS S_NO,
       'Total number of adult and paediatric ART patients with an undetectable viral load(<50 copies/ml) in the reporting period  (with in the past 12 months)' as Activity,
       COUNT(*)                                                                                                                                                 as Value
FROM pvls
where (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 1' AS S_NO,
       '< 1 year, Male'    as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) < 1
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 3'               AS S_NO,
       '< 1 year, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) < 1
  AND sex = 'Female'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'


UNION ALL
SELECT 'HIV_TX_PVLS_UN. 4' AS S_NO,
       '1 - 4 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 1 AND 4
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 6'                  AS S_NO,
       '1 - 4 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 1 AND 4
  AND sex = 'Female'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'


UNION ALL
SELECT 'HIV_TX_PVLS_UN. 7' AS S_NO,
       '5 - 9 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 5 AND 9
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 9'                  AS S_NO,
       '5 - 9 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 5 AND 9
  AND sex = 'Female'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'


UNION ALL
SELECT 'HIV_TX_PVLS_UN. 10'  AS S_NO,
       '10 - 14 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 10 AND 14
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 12'                   AS S_NO,
       '10 - 14 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 10 AND 14
  AND sex = 'Female'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'


UNION ALL
SELECT 'HIV_TX_PVLS_UN. 13'  AS S_NO,
       '15 - 19 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 15 AND 19
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 14'               AS S_NO,
       '15 - 19 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 15 AND 19
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 15'                   AS S_NO,
       '15 - 19 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 15 AND 19
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 16'  AS S_NO,
       '20 - 24 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 20 AND 24
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 17'               AS S_NO,
       '20 - 24 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 20 AND 24
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 18'                   AS S_NO,
       '20 - 24 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 20 AND 24
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 19'  AS S_NO,
       '25 - 29 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 25 AND 29
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 20'               AS S_NO,
       '25 - 29 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 25 AND 29
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 21'                   AS S_NO,
       '25 - 29 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 25 AND 29
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 22'  AS S_NO,
       '30 - 34 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 30 AND 34
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 23'               AS S_NO,
       '30 - 34 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 30 AND 34
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 24'                   AS S_NO,
       '30 - 34 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 30 AND 34
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 25'  AS S_NO,
       '35 - 39 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 35 AND 39
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 26'               AS S_NO,
       '35 - 39 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 35 AND 39
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 27'                   AS S_NO,
       '35 - 39 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 35 AND 39
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 28'  AS S_NO,
       '40 - 44 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 40 AND 44
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 29'               AS S_NO,
       '40 - 44 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 40 AND 44
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 30'                   AS S_NO,
       '40 - 44 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 40 AND 44
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 31'  AS S_NO,
       '45 - 49 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 45 AND 49
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 32'               AS S_NO,
       '45 - 49 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 45 AND 49
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 33'                   AS S_NO,
       '45 - 49 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 45 AND 49
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 34' AS S_NO,
       '>= 50 years, Male'  as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) >= 50
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_UN. 36'                 AS S_NO,
       '>= 50 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) >= 50
  AND sex = 'Female'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'

UNION ALL
SELECT 'HIV_TX_PVLS_LV'                                                                                                                                      AS S_NO,
       'Total number of adult and paediatric ART patients with low level viremia (50 -1000 copies/ml) in the reporting period  (with in the past 12 months)' as Activity,
       COUNT(*)                                                                                                                                              as Value
FROM pvls
where viral_load_count BETWEEN 50 AND 1000

UNION ALL
SELECT 'HIV_TX_PVLS_LV. 1' AS S_NO,
       '< 1 year, Male'    as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) < 1
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 3'               AS S_NO,
       '< 1 year, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) < 1
  AND sex = 'Female'
  AND viral_load_count BETWEEN 50 AND 1000



UNION ALL
SELECT 'HIV_TX_PVLS_LV. 4' AS S_NO,
       '1 - 4 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 1 AND 4
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 6'                  AS S_NO,
       '1 - 4 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 1 AND 4
  AND sex = 'Female'
  AND viral_load_count BETWEEN 50 AND 1000



UNION ALL
SELECT 'HIV_TX_PVLS_LV. 7' AS S_NO,
       '5 - 9 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 5 AND 9
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 9'                  AS S_NO,
       '5 - 9 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 5 AND 9
  AND sex = 'Female'
  AND viral_load_count BETWEEN 50 AND 1000



UNION ALL
SELECT 'HIV_TX_PVLS_LV. 10'  AS S_NO,
       '10 - 14 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 10 AND 14
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 12'                   AS S_NO,
       '10 - 14 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 10 AND 14
  AND sex = 'Female'
  AND viral_load_count BETWEEN 50 AND 1000



UNION ALL
SELECT 'HIV_TX_PVLS_LV. 13'  AS S_NO,
       '15 - 19 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 15 AND 19
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 14'               AS S_NO,
       '15 - 19 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 15 AND 19
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 15'                   AS S_NO,
       '15 - 19 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 15 AND 19
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 16'  AS S_NO,
       '20 - 24 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 20 AND 24
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 17'               AS S_NO,
       '20 - 24 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 20 AND 24
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 18'                   AS S_NO,
       '20 - 24 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 20 AND 24
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 19'  AS S_NO,
       '25 - 29 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 25 AND 29
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 20'               AS S_NO,
       '25 - 29 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 25 AND 29
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 21'                   AS S_NO,
       '25 - 29 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 25 AND 29
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 22'  AS S_NO,
       '30 - 34 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 30 AND 34
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 23'               AS S_NO,
       '30 - 34 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 30 AND 34
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 24'                   AS S_NO,
       '30 - 34 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 30 AND 34
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 25'  AS S_NO,
       '35 - 39 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 35 AND 39
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 26'               AS S_NO,
       '35 - 39 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 35 AND 39
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 27'                   AS S_NO,
       '35 - 39 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 35 AND 39
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 28'  AS S_NO,
       '40 - 44 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 40 AND 44
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 29'               AS S_NO,
       '40 - 44 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 40 AND 44
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 30'                   AS S_NO,
       '40 - 44 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 40 AND 44
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 31'  AS S_NO,
       '45 - 49 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 45 AND 49
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 32'               AS S_NO,
       '45 - 49 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 45 AND 49
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 33'                   AS S_NO,
       '45 - 49 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) BETWEEN 45 AND 49
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND viral_load_count BETWEEN 50 AND 1000


UNION ALL
SELECT 'HIV_TX_PVLS_LV. 34' AS S_NO,
       '>= 50 years, Male'  as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) >= 50
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000
UNION ALL
SELECT 'HIV_TX_PVLS_LV. 36'                 AS S_NO,
       '>= 50 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) >= 50
  AND sex = 'Female'
  AND viral_load_count BETWEEN 50 AND 1000;
END //

DELIMITER ;