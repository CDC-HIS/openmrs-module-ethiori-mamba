DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_tx_new_query;

CREATE PROCEDURE sp_fact_hmis_tx_new_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
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
                             transferred_in_check_this_for_all_t as transferred_in

                      FROM mamba_flat_encounter_follow_up follow_up
                               JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                    ON follow_up.encounter_id = follow_up_1.encounter_id
                               JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                    ON follow_up.encounter_id = follow_up_2.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id),
         tmp_latest_follow_up as (SELECT client_id,
                                         follow_up_date                                                                             AS FollowupDate,
                                         encounter_id,
                                         follow_up_status,
                                         art_start_date,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
                                    AND follow_up_date <= REPORT_END_DATE),
         latest_follow_up as (SELECT *
                              from tmp_latest_follow_up
                              where row_num = 1),
         tmp_first_follow_up as (SELECT client_id,
                                        follow_up_date                                                                     AS FollowupDate,
                                        encounter_id,
                                        transferred_in,
                                        pregnancy_status,
                                        follow_up_status,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date , encounter_id ) AS row_num
                                 FROM FollowUp
                                 WHERE art_start_date IS NOT NULL),
         first_follow_up as (select * from tmp_first_follow_up where row_num = 1),

         tx_new_tmp as (select latest_follow_up.client_id,
                               latest_follow_up.art_start_date,
                               latest_follow_up.FollowupDate,
                               transferred_in,
                               pregnancy_status
                        from latest_follow_up
                                 join first_follow_up on latest_follow_up.client_id = first_follow_up.client_id
                        where art_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                          AND (first_follow_up.transferred_in is null or first_follow_up.transferred_in != 'Yes')
                          AND first_follow_up.follow_up_status in ('Alive', 'Restart medication')),
         tx_new as (select tx_new_tmp.client_id, sex, pregnancy_status, date_of_birth
                    from tx_new_tmp
                             join mamba_dim_client client on tx_new_tmp.client_id = client.client_id)

    SELECT 'HIV_TX_NEW'                                                            AS S_NO,
           'Number of adults and children with HIV infection newly started on ART' as Activity,
           COUNT(*)                                                                as Value
    FROM tx_new
-- 1 < 1 year, Male
    UNION ALL
    SELECT 'HIV_TX_NEW. 1'  AS S_NO,
           '< 1 year, Male' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) < 1
-- 3 1 < 1 year, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 3'                   AS S_NO,
           '< 1 year, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) < 1
      AND sex = 'Female'
    --  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 4 1 - 4 years, Male
    UNION ALL
    SELECT 'HIV_TX_NEW. 4'     AS S_NO,
           '1 - 4 years, Male' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND sex = 'Male'
-- 6 1-4 year, Female-non-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 6'                 AS S_NO,
           '1 - 4 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND sex = 'Female'
    --  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 7 5-9 year, Male
    UNION ALL
    SELECT 'HIV_TX_NEW. 7'  AS S_NO,
           '5 - 9 years, Male' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND sex = 'Male'
-- 9 5-9 year, Female-non-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 9'                 AS S_NO,
           '5 - 9 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND sex = 'Female'
    --  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 10 10-14 year, Male
    UNION ALL
    SELECT 'HIV_TX_NEW. 10'   AS S_NO,
           '10 - 14 years, Male' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND sex = 'Male'
-- 12 10-14 year, Female-non-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 12'                  AS S_NO,
           '10 - 14 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND sex = 'Female'
    --  AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 13 15-19 year, Male
    UNION ALL
    SELECT 'HIV_TX_NEW. 13'   AS S_NO,
           '15 - 19 years, Male' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND sex = 'Male'
-- 14 15-19 year, Female-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 14'              AS S_NO,
           '15 - 19 years, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 15 15-19 year, Female-non-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 15'                  AS S_NO,
           '15 - 19 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 16 20-24 year, Male
    UNION ALL
    SELECT 'HIV_TX_NEW. 16'   AS S_NO,
           '20 - 24 years, Male' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND sex = 'Male'
-- 17 20-24 year, Female-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 17'              AS S_NO,
           '20 - 24 years, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 18 20-24 year, Female-non-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 18'                  AS S_NO,
           '20 - 24 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 19 25-29 year, Male
    UNION ALL
    SELECT 'HIV_TX_NEW. 19'   AS S_NO,
           '25 - 29 years, Male' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND sex = 'Male'
-- 20 25-29 year, Female-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 20'              AS S_NO,
           '25 - 29 years, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 21 25-29 year, Female-non-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 21'                  AS S_NO,
           '25 - 29 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 22 30-34 year, Male
    UNION ALL
    SELECT 'HIV_TX_NEW. 22'   AS S_NO,
           '30 - 34 years, Male' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND sex = 'Male'
-- 23 30-34 year, Female-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 23'              AS S_NO,
           '30 - 34 years, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 24 30-34 year, Female-non-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 24'                  AS S_NO,
           '30 - 34 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 25 35-39 year, Male
    UNION ALL
    SELECT 'HIV_TX_NEW. 25'   AS S_NO,
           '35 - 39 years, Male' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND sex = 'Male'
-- 26 35-39 year, Female-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 26'              AS S_NO,
           '35 - 39 years, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 27 35-39 year, Female-non-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 27'                  AS S_NO,
           '35 - 39 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 28 35-39 year, Male
    UNION ALL
    SELECT 'HIV_TX_NEW. 28'   AS S_NO,
           '40 - 44 years, Male' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND sex = 'Male'
-- 29 40-44 year, Female-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 29'              AS S_NO,
           '40 - 44 years, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 30 40-44 year, Female-non-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 30'                  AS S_NO,
           '40 - 44 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 31 45-49 year, Male
    UNION ALL
    SELECT 'HIV_TX_NEW. 31'   AS S_NO,
           '45 - 49 years, Male' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND sex = 'Male'
-- 32 45-49 year, Female-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 32'              AS S_NO,
           '45 - 49 years, Female - pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 33 45-49 year, Female-non-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 33'                  AS S_NO,
           '45 - 49 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)

-- 34 >=50 year, Male
    UNION ALL
    SELECT 'HIV_TX_NEW. 34'  AS S_NO,
           '>= 50 years, Male' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) >= 50
      AND sex = 'Male'
-- 36 >=50 year, Female-non-pregnant
    UNION ALL
    SELECT 'HIV_TX_NEW. 36'                 AS S_NO,
           '>= 50 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM tx_new
    WHERE TIMESTAMPDIFF(YEAR,date_of_birth, REPORT_END_DATE) >= 50
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null);
END //

DELIMITER ;