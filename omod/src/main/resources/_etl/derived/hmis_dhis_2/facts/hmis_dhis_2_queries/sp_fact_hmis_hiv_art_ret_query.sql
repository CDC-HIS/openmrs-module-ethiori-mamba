DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_art_ret_query;

CREATE PROCEDURE sp_fact_hmis_hiv_art_ret_query(  IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUp AS (SELECT follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_status,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date       AS art_start_date,
                             treatment_end_date,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child          breast_feeding_status,
                             pregnancy_status,
                             transferred_in_check_this_for_all_t AS transferred_in
                      FROM mamba_flat_encounter_follow_up follow_up
                               JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                    ON follow_up.encounter_id = follow_up_1.encounter_id
                               JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                    ON follow_up.encounter_id = follow_up_2.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id),

         tx_curr_all AS (SELECT client_id,
                                follow_up_date                                                                             AS FollowupDate,
                                encounter_id,
                                follow_up_status,
                                treatment_end_date,
                                art_start_date,
                                pregnancy_status,
                                ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                         FROM FollowUp
                         WHERE follow_up_status IS NOT NULL
                           AND art_start_date IS NOT NULL
                           AND follow_up_date <= REPORT_END_DATE
                           AND follow_up_status in ('Alive', 'Restart medication')
                           AND treatment_end_date >= REPORT_END_DATE),
         started_art_12m AS (select *,
                                    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date , encounter_id ) AS row_num
                             from FollowUp
                             where art_start_date >= DATE_ADD(REPORT_START_DATE, INTERVAL -1 YEAR)
                               AND art_start_date <= DATE_ADD(REPORT_END_DATE, INTERVAL -1 YEAR)),
         tx_curr_ret AS (select tx_curr_all.*,
                                client.sex,
                                client.date_of_birth,
                                DATE_ADD(art_start_date, INTERVAL -1 YEAR)
                         from tx_curr_all
                                  join mamba_dim_client client on tx_curr_all.client_id = client.client_id
                         where row_num = 1
                           AND art_start_date >= DATE_ADD(REPORT_START_DATE, INTERVAL -1 YEAR)
                           AND art_start_date <= DATE_ADD(REPORT_END_DATE, INTERVAL -1 YEAR)),
         ret_percentage as (SELECT 'HIV_ART_RET'                                                                                                        AS S_NO,
                                   'ART retention rate (Percentage of adult and children on ART treatment after 12 month of initation of ARV therapy )' AS Activity,
                                   CAST(ROUND((SELECT COUNT(*)
                                               FROM tx_curr_ret) *
                                              100.0 /
                                              (SELECT COUNT(*) FROM started_art_12m), 2) AS CHAR)
                                                                                                                                                        AS Value
                            FROM tx_curr_ret
                            LIMIT 1)

    SELECT S_NO,
           Activity,
           Value
    FROM ret_percentage
    UNION ALL
    SELECT 'HIV_ART_RET.1'                                                                              AS S_NO,
           'Number of adults and children who are still on treatment at 12 months after initiating ART' AS Activity,
           COUNT(*)                                                                                     AS Value
    FROM tx_curr_ret
-- 1.1 < 1 year, Male
    UNION ALL
    SELECT 'HIV_ART_RET.1. 1' AS S_NO,
           '< 1 year, Male'   AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND sex = 'Male'
-- 1.3 < 1 year, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 3'                AS S_NO,
           '< 1 year, Female - non-pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.4 1 - 4 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET.1. 4'  AS S_NO,
           '1 - 4 years, Male' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND sex = 'Male'
-- 1.6 1 - 4 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 6'                   AS S_NO,
           '1 - 4 years, Female - non-pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.7 5 - 9 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET.1. 7'  AS S_NO,
           '5 - 9 years, Male' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND sex = 'Male'
-- 1.9 5 - 9 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 9'                   AS S_NO,
           '5 - 9 years, Female - non-pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND sex = 'Female'
    --  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.10 10 - 14 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET.1. 10'   AS S_NO,
           '10 - 14 years, Male' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND sex = 'Male'
-- 1.12 10 - 14 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 12'                    AS S_NO,
           '10 - 14 years, Female - non-pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND sex = 'Female'
    --  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 1.13 15 - 19 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET.1. 13'   AS S_NO,
           '15 - 19 years, Male' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND sex = 'Male'
-- 1.14 15 - 19 years, Female - pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 14'                AS S_NO,
           '15 - 19 years, Female - pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 1.15 15 - 19 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 15'                    AS S_NO,
           '15 - 19 years, Female - non-pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND sex = 'Female'
      AND (
        pregnancy_status = 'No'
            OR pregnancy_status is null
        )
-- 1.16 20 - 24 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET.1. 16'   AS S_NO,
           '20 - 24 years, Male' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND sex = 'Male'
-- 1.17 20 - 24 years, Female - pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 17'                AS S_NO,
           '20 - 24 years, Female - pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 1.18 20 - 24 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 18'                    AS S_NO,
           '20 - 24 years, Female - non-pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND sex = 'Female'
      AND (
        pregnancy_status = 'No'
            OR pregnancy_status is null
        )
-- 1.19 25 - 29 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET.1. 19'   AS S_NO,
           '25 - 29 years, Male' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND sex = 'Male'
-- 1.20 25 - 29 years, Female - pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 20'                AS S_NO,
           '25 - 29 years, Female - pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 1.21 25 - 29 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 21'                    AS S_NO,
           '25 - 29 years, Female - non-pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND sex = 'Female'
      AND (
        pregnancy_status = 'No'
            OR pregnancy_status is null
        )
-- 1.22 30 - 34 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET.1. 22'   AS S_NO,
           '30 - 34 years, Male' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND sex = 'Male'
-- 1.23 30 - 34 years, Female - pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 23'                AS S_NO,
           '30 - 34 years, Female - pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 1.24 30 - 34 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 24'                    AS S_NO,
           '30 - 34 years, Female - non-pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND sex = 'Female'
      AND (
        pregnancy_status = 'No'
            OR pregnancy_status is null
        )
-- 1.25 35 - 39 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET.1. 25'   AS S_NO,
           '35 - 39 years, Male' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND sex = 'Male'
-- 1.26 35 - 39 years, Female - pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 26'                AS S_NO,
           '35 - 39 years, Female - pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 1.27 35 - 39 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 27'                    AS S_NO,
           '35 - 39 years, Female - non-pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND sex = 'Female'
      AND (
        pregnancy_status = 'No'
            OR pregnancy_status is null
        )
-- 1.28 40 - 44 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET.1. 28'   AS S_NO,
           '40 - 44 years, Male' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND sex = 'Male'
-- 1.29 40 - 44 years, Female - pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 29'                AS S_NO,
           '40 - 44 years, Female - pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 1.30 40 - 44 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 30'                    AS S_NO,
           '40 - 44 years, Female - non-pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND sex = 'Female'
      AND (
        pregnancy_status = 'No'
            OR pregnancy_status is null
        )
-- 1.31 45 - 49 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET.1. 31'   AS S_NO,
           '45 - 49 years, Male' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND sex = 'Male'
-- 1.32 45 - 49 years, Female - pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 32'                AS S_NO,
           '45 - 49 years, Female - pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 1.33 45 - 49 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 33'                    AS S_NO,
           '45 - 49 years, Female - non-pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND sex = 'Female'
      AND (
        pregnancy_status = 'No'
            OR pregnancy_status is null
        )
-- 1.34 >= 50 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET.1. 34' AS S_NO,
           '>= 50 years, Male' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND sex = 'Male'
-- 1.36 >= 50 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET.1. 36'                  AS S_NO,
           '>= 50 years, Female - non-pregnant' AS Activity,
           COUNT(*)
    FROM tx_curr_ret
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND sex = 'Female';

END //

DELIMITER ;