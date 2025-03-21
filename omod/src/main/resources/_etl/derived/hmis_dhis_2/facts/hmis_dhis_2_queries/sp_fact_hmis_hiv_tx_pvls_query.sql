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
                          --  AND follow_up_status in ('Alive', 'Restart medication')
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
                     viral_load_test_status
              from tmp_pvls_2
                       inner join mamba_dim_client client on tmp_pvls_2.client_id = client.client_id
                       inner join latest_follow_up on tmp_pvls_2.client_id = latest_follow_up.client_id)
        ,
     pvls_percentage as (SELECT 'HIV_TX_PVLS'                                                                                                                                            AS S_NO,
                                'Viral load Suppression (Percentage of ART clients with a suppressed viral load among those with a viral load test at 12 month in the reporting period)' AS Activity,
                                CAST(ROUND((SELECT COUNT(*)
                                            FROM pvls
                                            WHERE ((viral_load_count < 50 or viral_load_count is null) and
                                                   viral_load_test_status = 'Suppressed')
                                               or (viral_load_count BETWEEN 50 AND 1000)) *
                                           100.0 /
                                           (SELECT COUNT(*) FROM pvls),
                                           2) AS CHAR)                                                                                                                                   AS Value
FROM pvls
    LIMIT 1)

SELECT S_NO,
       Activity,
       Value
FROM pvls_percentage
--
UNION ALL
SELECT 'HIV_TX_PVLS.1'                                                                                               AS S_NO,
       'Number of adult and pediatric ART patients for whom viral load test result received in the reporting period' as Activity,
       COUNT(*)                                                                                                      as Value
FROM pvls
-- 1.1 < 1 year, Male
UNION ALL
SELECT 'HIV_TX_PVLS.1.1' AS S_NO,
       '< 1 year, Male'  as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
  AND sex = 'Male'
-- 1.3 < 1 year, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.3'                 AS S_NO,
       '< 1 year, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
  AND sex = 'Female'
--  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.4 1 - 4 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS.1.4'   AS S_NO,
       '1 - 4 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
  AND sex = 'Male'
-- 1.6 1 - 4 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.6'                    AS S_NO,
       '1 - 4 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
  AND sex = 'Female'
--  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.7 5 - 9 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS.1.7'   AS S_NO,
       '5 - 9 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
  AND sex = 'Male'
-- 1.9 5 - 9 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.9'                    AS S_NO,
       '5 - 9 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
  AND sex = 'Female'
--  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.10 10 - 14 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS.1.10'    AS S_NO,
       '10 - 14 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
  AND sex = 'Male'
-- 1.12 10 - 14 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.12'                     AS S_NO,
       '10 - 14 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
  AND sex = 'Female'
--  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.13 15 - 19 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS.1.13'    AS S_NO,
       '15 - 19 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
  AND sex = 'Male'
-- 1.14 15 - 19 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.14'                 AS S_NO,
       '15 - 19 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
-- 1.15 15 - 19 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.15'                     AS S_NO,
       '15 - 19 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.16 20 - 24 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS.1.16'    AS S_NO,
       '20 - 24 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
  AND sex = 'Male'
-- 1.17 20 - 24 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.17'                 AS S_NO,
       '20 - 24 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
-- 1.18 20 - 24 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.18'                     AS S_NO,
       '20 - 24 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.19 25 - 29 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS.1.19'    AS S_NO,
       '25 - 29 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
  AND sex = 'Male'
-- 1.20 25 - 29 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.20'                 AS S_NO,
       '25 - 29 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
-- 1.21 25 - 29 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.21'                     AS S_NO,
       '25 - 29 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.22 30 - 34 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS.1.22'    AS S_NO,
       '30 - 34 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
  AND sex = 'Male'
-- 1.23 30 - 34 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.23'                 AS S_NO,
       '30 - 34 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
-- 1.24 30 - 34 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.24'                     AS S_NO,
       '30 - 34 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.25 35 - 39 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS.1.25'    AS S_NO,
       '35 - 39 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
  AND sex = 'Male'
-- 1.26 35 - 39 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.26'                 AS S_NO,
       '35 - 39 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
-- 1.27 35 - 39 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.27'                     AS S_NO,
       '35 - 39 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.28 40 - 44 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS.1.28'    AS S_NO,
       '40 - 44 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
  AND sex = 'Male'
-- 1.29 40 - 44 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.29'                 AS S_NO,
       '40 - 44 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
-- 1.30 40 - 44 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.30'                     AS S_NO,
       '40 - 44 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.31 45 - 49 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS.1.31'    AS S_NO,
       '45 - 49 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
  AND sex = 'Male'
-- 1.32 45 - 49 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.32'                 AS S_NO,
       '45 - 49 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
-- 1.33 45 - 49 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.33'                     AS S_NO,
       '45 - 49 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.34 >= 50 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS.1.34'  AS S_NO,
       '>= 50 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
  AND sex = 'Male'
-- 1.35 >= 50 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS.1.36'                   AS S_NO,
       '>= 50 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
  AND sex = 'Female'

-- Undetectable viral load
UNION ALL
SELECT 'HIV_TX_PVLS_UN'                                                                                                                    AS S_NO,
       'Total number of adult and pediatric ART patients with an undetectable viral load (Less than 50 copies/ml) in the reporting period' as Activity,
       COUNT(*)                                                                                                                            as Value
FROM pvls
where (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 1 < 1 year, Male
UNION ALL
SELECT 'HIV_TX_PVLS_UN1' AS S_NO,
       '< 1 year, Male'  as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 3 < 1 year, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN3'                 AS S_NO,
       '< 1 year, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
  AND sex = 'Female'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
--  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.4 1 - 4 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_UN4'   AS S_NO,
       '1 - 4 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 6 - 4 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN6'                    AS S_NO,
       '1 - 4 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
  AND sex = 'Female'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
--  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 7 5 - 9 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_UN7'   AS S_NO,
       '5 - 9 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 9 5 - 9 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN9'                    AS S_NO,
       '5 - 9 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
  AND sex = 'Female'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
--  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 10 10 - 14 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_UN10'    AS S_NO,
       '10 - 14 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 12 10 - 14 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN12'                     AS S_NO,
       '10 - 14 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
  AND sex = 'Female'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
--  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 13 15 - 19 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_UN13'    AS S_NO,
       '15 - 19 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 14 15 - 19 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN14'                 AS S_NO,
       '15 - 19 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 15 15 - 19 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN15'                     AS S_NO,
       '15 - 19 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 16 20 - 24 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_UN16'    AS S_NO,
       '20 - 24 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 17 20 - 24 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN17'                 AS S_NO,
       '20 - 24 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 18 20 - 24 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN18'                     AS S_NO,
       '20 - 24 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 19 25 - 29 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_UN19'    AS S_NO,
       '25 - 29 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 20 25 - 29 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN20'                 AS S_NO,
       '25 - 29 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 21 25 - 29 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN21'                     AS S_NO,
       '25 - 29 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 22 30 - 34 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_UN22'    AS S_NO,
       '30 - 34 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 23 30 - 34 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN23'                 AS S_NO,
       '30 - 34 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 24 30 - 34 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN24'                     AS S_NO,
       '30 - 34 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 25 35 - 39 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_UN25'    AS S_NO,
       '35 - 39 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 26 35 - 39 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN26'                 AS S_NO,
       '35 - 39 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 27 35 - 39 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN27'                     AS S_NO,
       '35 - 39 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 28 40 - 44 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_UN28'    AS S_NO,
       '40 - 44 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 29 40 - 44 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN29'                 AS S_NO,
       '40 - 44 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 30 40 - 44 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN30'                     AS S_NO,
       '40 - 44 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 31 45 - 49 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_UN31'    AS S_NO,
       '45 - 49 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 32 45 - 49 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN32'                 AS S_NO,
       '45 - 49 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 33 45 - 49 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN33'                     AS S_NO,
       '45 - 49 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 34 >= 50 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_UN34'  AS S_NO,
       '>= 50 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
  AND sex = 'Male'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'
-- 36 >= 50 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_UN36'                   AS S_NO,
       '>= 50 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
  AND sex = 'Female'
  AND (viral_load_count < 50 or viral_load_count is null)
  and viral_load_test_status = 'Suppressed'


-- Low level viral load
UNION ALL
SELECT 'HIV_TX_PVLS_LV'                                                                                                                    AS S_NO,
       'Total number of adult and pediatric ART patients with an undetectable viral load (Less than 50 copies/ml) in the reporting period' as Activity,
       COUNT(*)                                                                                                                            as Value
FROM pvls
where viral_load_count BETWEEN 50 AND 1000
-- 1 < 1 year, Male
UNION ALL
SELECT 'HIV_TX_PVLS_LV1' AS S_NO,
       '< 1 year, Male'  as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000

-- 3 < 1 year, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV3'                 AS S_NO,
       '< 1 year, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
  AND sex = 'Female'
  AND viral_load_count BETWEEN 50 AND 1000

--  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.4 1 - 4 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_LV4'   AS S_NO,
       '1 - 4 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000

-- 6 - 4 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV6'                    AS S_NO,
       '1 - 4 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
  AND sex = 'Female'
  AND viral_load_count BETWEEN 50 AND 1000

--  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 7 5 - 9 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_LV7'   AS S_NO,
       '5 - 9 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000

-- 9 5 - 9 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV9'                    AS S_NO,
       '5 - 9 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
  AND sex = 'Female'
  AND viral_load_count BETWEEN 50 AND 1000

--  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 10 10 - 14 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_LV10'    AS S_NO,
       '10 - 14 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000

-- 12 10 - 14 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV12'                     AS S_NO,
       '10 - 14 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
  AND sex = 'Female'
  AND viral_load_count BETWEEN 50 AND 1000

--  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 13 15 - 19 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_LV13'    AS S_NO,
       '15 - 19 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000

-- 14 15 - 19 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV14'                 AS S_NO,
       '15 - 19 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND viral_load_count BETWEEN 50 AND 1000

-- 15 15 - 19 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV15'                     AS S_NO,
       '15 - 19 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND viral_load_count BETWEEN 50 AND 1000

-- 16 20 - 24 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_LV16'    AS S_NO,
       '20 - 24 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000

-- 17 20 - 24 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV17'                 AS S_NO,
       '20 - 24 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND viral_load_count BETWEEN 50 AND 1000

-- 18 20 - 24 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV18'                     AS S_NO,
       '20 - 24 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND viral_load_count BETWEEN 50 AND 1000

-- 19 25 - 29 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_LV19'    AS S_NO,
       '25 - 29 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000

-- 20 25 - 29 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV20'                 AS S_NO,
       '25 - 29 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND viral_load_count BETWEEN 50 AND 1000

-- 21 25 - 29 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV21'                     AS S_NO,
       '25 - 29 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND viral_load_count BETWEEN 50 AND 1000

-- 22 30 - 34 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_LV22'    AS S_NO,
       '30 - 34 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000

-- 23 30 - 34 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV23'                 AS S_NO,
       '30 - 34 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND viral_load_count BETWEEN 50 AND 1000

-- 24 30 - 34 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV24'                     AS S_NO,
       '30 - 34 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND viral_load_count BETWEEN 50 AND 1000

-- 25 35 - 39 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_LV25'    AS S_NO,
       '35 - 39 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000

-- 26 35 - 39 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV26'                 AS S_NO,
       '35 - 39 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND viral_load_count BETWEEN 50 AND 1000

-- 27 35 - 39 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV27'                     AS S_NO,
       '35 - 39 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND viral_load_count BETWEEN 50 AND 1000

-- 28 40 - 44 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_LV28'    AS S_NO,
       '40 - 44 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000

-- 29 40 - 44 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV29'                 AS S_NO,
       '40 - 44 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND viral_load_count BETWEEN 50 AND 1000

-- 30 40 - 44 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV30'                     AS S_NO,
       '40 - 44 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND viral_load_count BETWEEN 50 AND 1000

-- 31 45 - 49 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_LV31'    AS S_NO,
       '45 - 49 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000

-- 32 45 - 49 years, Female - pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV32'                 AS S_NO,
       '45 - 49 years, Female - pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
  AND sex = 'Female'
  AND pregnancy_status = 'Yes'
  AND viral_load_count BETWEEN 50 AND 1000

-- 33 45 - 49 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV33'                     AS S_NO,
       '45 - 49 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
  AND sex = 'Female'
  AND (pregnancy_status = 'No' or pregnancy_status is null)
  AND viral_load_count BETWEEN 50 AND 1000

-- 34 >= 50 years, Male
UNION ALL
SELECT 'HIV_TX_PVLS_LV34'  AS S_NO,
       '>= 50 years, Male' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
  AND sex = 'Male'
  AND viral_load_count BETWEEN 50 AND 1000

-- 36 >= 50 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_TX_PVLS_LV36'                   AS S_NO,
       '>= 50 years, Female - non-pregnant' as Activity,
       COUNT(*)
FROM pvls
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
  AND sex = 'Female'
  AND viral_load_count BETWEEN 50 AND 1000;
END //

DELIMITER ;