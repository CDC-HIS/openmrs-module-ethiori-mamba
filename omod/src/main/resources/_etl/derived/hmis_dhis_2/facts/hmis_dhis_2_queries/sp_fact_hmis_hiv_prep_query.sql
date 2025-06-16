DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_prep_query;

CREATE PROCEDURE  sp_fact_hmis_hiv_prep_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
WITH PreExposure AS (select screening.client_id,
                            screening.encounter_id,
                            screening.encounter_datetime,
                            screening.sex_worker,
                            screening.prep_started,
                            screening.type_of_client,
                            screening.treatment_start_date,
                            screening.do_you_have_an_hiv_positive_partner,
                            screening.antiretroviral_art_dispensed_dose_i as dose_dispensed,
                            follow_up.follow_up_status,
                            follow_up.pregnancy_status as f_pregnancy_status,
                            follow_up.prep_dose_end_date,
                            follow_up.follow_up_date_followup_
                     FROM mamba_flat_encounter_pre_exposure_scree screening
                              LEFT JOIN mamba_flat_encounter_pre_exposure_follo follow_up
                                        on screening.client_id = follow_up.client_id),
     tmp_latest_follow_up as (select prep_dose_end_date,
                                     prep_started,
                                     date_of_birth,
                                     sex,
                                     do_you_have_an_hiv_positive_partner,
                                     sex_worker,
                                     treatment_start_date,
                                     follow_up_status,
                                     type_of_client,
                                     client.client_id,
                                     follow_up_date_followup_,
                                     dose_dispensed,
                                     ROW_NUMBER() OVER (
                                         PARTITION BY PreExposure.client_id
                                         ORDER BY
                                             CASE
                                                 WHEN PreExposure.follow_up_date_followup_ IS NULL THEN 1
                                                 ELSE 0
                                                 END,
                                             PreExposure.follow_up_date_followup_ DESC,
                                             PreExposure.encounter_id DESC
                                         ) AS row_num
                              from PreExposure
                                       join mamba_dim_client client on PreExposure.client_id = client.client_id
                              where (follow_up_date_followup_ <= REPORT_END_DATE
                                 or follow_up_date_followup_ is null)
                                  AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
     ),
     tx_new as (select *
                from tmp_latest_follow_up
                WHERE treatment_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                  AND type_of_client = 'New client'
                  AND row_num = 1

         -- AND follow_up_status
     ),
     tx_curr as (select *
              from tmp_latest_follow_up
              where row_num = 1
                and (   (prep_dose_end_date >= REPORT_END_DATE or
                         DATE_ADD(treatment_start_date, INTERVAL dose_dispensed DAY) >= REPORT_END_DATE) -- Param 6 @end_date, Param 7 @end_date
                            and prep_started = 'Yes'
                            )
                  or follow_up_date_followup_ BETWEEN REPORT_START_DATE AND REPORT_END_DATE AND final_hiv_test_result = 'Positive') -- Param 8 @end_date, Param 9 @end_date
     )
-- Number of individuals receiving Pre-Exposure Prophylaxis
SELECT 'HIV_PrEP'                                                 AS S_NO,
       'Number of individuals receiving Pre-Exposure Prophylaxis' as Activity,
       COUNT(*)                                                   as Value
FROM tx_new
UNION ALL
-- Number of individuals receiving Pre-Exposure Prophylaxis
SELECT 'HIV_PrEP.1'                                                       AS S_NO,
       'PrEP (New Number of individuals who were newly enrolled on PrEP)' as Activity,
       COUNT(*)                                                           as Value
FROM tx_new
-- PrEP (New Number of individuals who were newly enrolled on PrEP)
UNION ALL
SELECT 'HIV_PrEP.1.1'   AS S_NO,
       'By age and Sex' as Activity,
       COUNT(*)         as Value
FROM tx_new
-- 15 - 19 years, Male
UNION ALL
SELECT 'HIV_PrEP.1.1. 1'     AS S_NO,
       '15 - 19 years, Male' as Activity,
       COUNT(*)              as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
  AND sex = 'Male'
-- 15 - 19 years, Female
UNION ALL
SELECT 'HIV_PrEP.1.1. 2'       AS S_NO,
       '15 - 19 years, Female' as Activity,
       COUNT(*)                as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
  AND sex = 'Female'
-- 20 - 24 years, Male
UNION ALL
SELECT 'HIV_PrEP.1.1. 3'     AS S_NO,
       '20 - 24 years, Male' as Activity,
       COUNT(*)              as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
  AND sex = 'Male'
-- 20 - 24 years, Female
UNION ALL
SELECT 'HIV_PrEP.1.1. 4'       AS S_NO,
       '20 - 24 years, Female' as Activity,
       COUNT(*)                as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
  AND sex = 'Female'
-- 25 - 29 years, Male
UNION ALL
SELECT 'HIV_PrEP.1.1. 5'     AS S_NO,
       '25 - 29 years, Male' as Activity,
       COUNT(*)              as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
  AND sex = 'Male'
-- 25 - 29 years, Female
UNION ALL
SELECT 'HIV_PrEP.1.1. 6'       AS S_NO,
       '25 - 29 years, Female' as Activity,
       COUNT(*)                as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
  AND sex = 'Female'
-- 30 - 34 years, Male
UNION ALL
SELECT 'HIV_PrEP.1.1. 7'     AS S_NO,
       '30 - 34 years, Male' as Activity,
       COUNT(*)              as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
  AND sex = 'Male'
-- 30 - 34 years, Female
UNION ALL
SELECT 'HIV_PrEP.1.1. 8'       AS S_NO,
       '30 - 34 years, Female' as Activity,
       COUNT(*)                as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
  AND sex = 'Female'
-- 35 - 39 years, Male
UNION ALL
SELECT 'HIV_PrEP.1.1. 9'     AS S_NO,
       '35 - 39 years, Male' as Activity,
       COUNT(*)              as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
  AND sex = 'Male'
-- 35 - 39 years, Female
UNION ALL
SELECT 'HIV_PrEP.1.1. 10'      AS S_NO,
       '35 - 39 years, Female' as Activity,
       COUNT(*)                as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
  AND sex = 'Female'
-- 40 - 44 years, Male
UNION ALL
SELECT 'HIV_PrEP.1.1. 11'    AS S_NO,
       '40 - 44 years, Male' as Activity,
       COUNT(*)              as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
  AND sex = 'Male'
-- 40 - 44 years, Female
UNION ALL
SELECT 'HIV_PrEP.1.1. 12'      AS S_NO,
       '40 - 44 years, Female' as Activity,
       COUNT(*)                as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
  AND sex = 'Female'
-- 45 - 49 years, Male
UNION ALL
SELECT 'HIV_PrEP.1.1. 13'    AS S_NO,
       '45 - 49 years, Male' as Activity,
       COUNT(*)              as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
  AND sex = 'Male'
-- 45 - 49 years, Female
UNION ALL
SELECT 'HIV_PrEP.1.1. 14'      AS S_NO,
       '45 - 49 years, Female' as Activity,
       COUNT(*)                as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
  AND sex = 'Female'
-- Greater than or equal to  50 years, Male
UNION ALL
SELECT 'HIV_PrEP.1.1. 15'  AS S_NO,
       '>= 50 years, Male' as Activity,
       COUNT(*)            as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
  AND sex = 'Male'
-- Greater than or equal to  50 years, Female
UNION ALL
SELECT 'HIV_PrEP.1.1. 16'    AS S_NO,
       '>= 50 years, Female' as Activity,
       COUNT(*)              as Value
FROM tx_new
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
  AND sex = 'Female'
-- By Client Category
UNION ALL
SELECT 'HIV_PrEP.1.2'       AS S_NO,
       'By Client Category' as Activity,
       COUNT(*)             as Value
FROM tx_new
-- Discordant Couple
UNION ALL
SELECT 'HIV_PrEP.1.2. 1'   AS S_NO,
       'Discordant Couple' as Activity,
       COUNT(*)            as Value
FROM tx_new
WHERE do_you_have_an_hiv_positive_partner = 'Yes'
-- Female sex worker[FSW]
UNION ALL
SELECT 'HIV_PrEP.1.2. 2'        AS S_NO,
       'Female sex worker[FSW]' as Activity,
       COUNT(*)                 as Value
FROM tx_new
WHERE sex_worker = 'Yes'
-- HIV_PrEP_CURR.1.
UNION ALL
SELECT 'HIV_PrEP_CURR.1'                                                                       AS S_NO,
       'PrEP Curr (Number of individuals that received oral PrEP during the reporting period)' as Activity,
       COUNT(*)                                                                                as Value
FROM tx_curr
-- By age and Sex
UNION ALL
SELECT 'HIV_PrEP_CURR.1' AS S_NO,
       'By age and Sex'  as Activity,
       COUNT(*)          as Value
FROM tx_curr
-- 15 - 19 years, Male
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 1'  AS S_NO,
       '15 - 19 years, Male' as Activity,
       COUNT(*)              as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
  AND sex = 'Male'
-- 15 - 19 years, Female
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 2'    AS S_NO,
       '15 - 19 years, Female' as Activity,
       COUNT(*)                as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
  AND sex = 'Female'
-- 20 - 24 years, Male
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 3'  AS S_NO,
       '20 - 24 years, Male' as Activity,
       COUNT(*)              as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
  AND sex = 'Male'
-- 20 - 24 years, Female
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 4'    AS S_NO,
       '20 - 24 years, Female' as Activity,
       COUNT(*)                as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
  AND sex = 'Female'
-- 25 - 29 years, Male
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 5'  AS S_NO,
       '25 - 29 years, Male' as Activity,
       COUNT(*)              as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
  AND sex = 'Male'
-- 25 - 29 years, Female
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 6'    AS S_NO,
       '25 - 29 years, Female' as Activity,
       COUNT(*)                as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
  AND sex = 'Female'
-- 30 - 34 years, Male
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 7'  AS S_NO,
       '30 - 34 years, Male' as Activity,
       COUNT(*)              as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
  AND sex = 'Male'
-- 30 - 34 years, Female
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 8'    AS S_NO,
       '30 - 34 years, Female' as Activity,
       COUNT(*)                as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
  AND sex = 'Female'
-- 35 - 39 years, Male
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 9'  AS S_NO,
       '35 - 39 years, Male' as Activity,
       COUNT(*)              as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
  AND sex = 'Male'
-- 35 - 39 years, Female
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 10'   AS S_NO,
       '35 - 39 years, Female' as Activity,
       COUNT(*)                as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
  AND sex = 'Female'
-- 40 - 44 years, Male
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 11' AS S_NO,
       '40 - 44 years, Male' as Activity,
       COUNT(*)              as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
  AND sex = 'Male'
-- 40 - 44 years, Female
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 12'   AS S_NO,
       '40 - 44 years, Female' as Activity,
       COUNT(*)                as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
  AND sex = 'Female'
-- 45 - 49 years, Male
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 13' AS S_NO,
       '45 - 49 years, Male' as Activity,
       COUNT(*)              as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
  AND sex = 'Male'
-- 45 - 49 years, Female
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 14'   AS S_NO,
       '45 - 49 years, Female' as Activity,
       COUNT(*)                as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
  AND sex = 'Female'
-- Greater than or equal to  50 years, Male
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 15' AS S_NO,
       '>= 50 years, Male'   as Activity,
       COUNT(*)              as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
  AND sex = 'Male'
-- Greater than or equal to  50 years, Female
UNION ALL
SELECT 'HIV_PrEP_CURR.1. 16' AS S_NO,
       '>= 50 years, Female' as Activity,
       COUNT(*)              as Value
FROM tx_curr
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
  AND sex = 'Female'
-- By Client Category
UNION ALL
SELECT 'HIV_PrEP_CURR.2'    AS S_NO,
       'By Client Category' as Activity,
       COUNT(*)             as Value  -- TODO check sero-discordant couple handling
FROM tx_curr
WHERE do_you_have_an_hiv_positive_partner = 'Yes'
  OR sex_worker = 'Yes'
-- Discordant Couple
UNION ALL
SELECT 'HIV_PrEP_CURR.2. 1' AS S_NO,
       'Discordant Couple'  as Activity,
       COUNT(*)             as Value
FROM tx_curr
WHERE do_you_have_an_hiv_positive_partner = 'Yes'
  and sex_worker != 'Yes'
-- Female sex worker[FSW]
UNION ALL
SELECT 'HIV_PrEP_CURR.2. 2'     AS S_NO,
       'Female sex worker[FSW]' as Activity,
       COUNT(*)                 as Value
FROM tx_curr
WHERE sex_worker = 'Yes';
END //

DELIMITER ;