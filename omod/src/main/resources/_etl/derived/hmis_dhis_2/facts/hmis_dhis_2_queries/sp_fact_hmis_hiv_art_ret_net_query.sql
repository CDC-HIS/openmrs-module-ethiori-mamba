DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_art_ret_net_query;

CREATE PROCEDURE sp_fact_hmis_hiv_art_ret_net_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
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
                               LEFT JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                         ON follow_up.encounter_id = follow_up_1.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                         ON follow_up.encounter_id = follow_up_2.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                         ON follow_up.encounter_id = follow_up_4.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_5 follow_up_5
                                         ON follow_up.encounter_id = follow_up_5.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_6 follow_up_6
                                         ON follow_up.encounter_id = follow_up_6.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_7 follow_up_7
                                         ON follow_up.encounter_id = follow_up_7.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_8 follow_up_8
                                         ON follow_up.encounter_id = follow_up_8.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_9 follow_up_9
                                         ON follow_up.encounter_id = follow_up_9.encounter_id),
         tmp_latest_follow_up as (select client_id,
                                         follow_up_status,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  from FollowUp
                                  where follow_up_date <= REPORT_END_DATE),
         latest_follow_up as (select * from tmp_latest_follow_up where row_num = 1),
         tmp_art_start_date_before_12_months as (SELECT encounter_id,
                                                        client_id,
                                                        follow_up_date                                                                             AS FollowupDate,
                                                        follow_up_status,
                                                        art_start_date,
                                                        treatment_end_date,
                                                        TIMESTAMPDIFF(MONTH, art_start_date, follow_up_date)                                       as months_on_art,
                                                        pregnancy_status,
                                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                                 FROM FollowUp
                                                 WHERE follow_up_status IS NOT NULL
                                                   AND art_start_date IS NOT NULL
                                                   AND art_start_date >=  fn_ethiopian_to_gregorian_calendar(date_add(fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL -12 MONTH))
                                                   AND art_start_date <=  fn_ethiopian_to_gregorian_calendar(date_add(fn_gregorian_to_ethiopian_calendar(REPORT_END_DATE, 'Y-M-D'), INTERVAL -12 MONTH))
                                                   AND follow_up_date <= REPORT_END_DATE),
         art_start_date_before_12_months as (select curr.client_id,
                                                    curr.FollowupDate,
                                                    curr.follow_up_status,
                                                    curr.pregnancy_status,
                                                    client.date_of_birth,
                                                    client.sex
                                             from tmp_art_start_date_before_12_months as curr
                                                      join mamba_dim_client client on curr.client_id = client.client_id
                                                      join latest_follow_up latest_follow_up
                                                           on curr.client_id = latest_follow_up.client_id
                                             where curr.row_num = 1
                                               and latest_follow_up.follow_up_status != 'Transferred out')
    SELECT 'HIV_ART_RET_NET'                                                                                                                  AS S_NO,
           'Number of persons on ART in the original cohort including those transferred in, minus those transferred out (net current cohort)' as Activity,
           COUNT(*)                                                                                                                           as Value
    FROM art_start_date_before_12_months
-- 1 < 1 year, Male
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 1' AS S_NO,
           '< 1 year, Male'    as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND sex = 'Male'
-- 2 < 1 year, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 3'               AS S_NO,
           '< 1 year, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND sex = 'Female'
-- 4 1 - 4 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 4' AS S_NO,
           '1 - 4 years, Male' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND sex = 'Male'
-- 6 1 - 4 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 6'                  AS S_NO,
           '1 - 4 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND sex = 'Female'
-- 7 5 - 9 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 7' AS S_NO,
           '5 - 9 years, Male' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND sex = 'Male'
-- 9 5 - 9 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 9'                  AS S_NO,
           '5 - 9 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND sex = 'Female'
-- 10 10 - 14 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 10'  AS S_NO,
           '10 - 14 years, Male' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND sex = 'Male'
-- 12 10 - 14 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 12'                   AS S_NO,
           '10 - 14 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND sex = 'Female'
-- 13 15 - 19 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 13'  AS S_NO,
           '15 - 19 years, Male' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND sex = 'Male'
-- 14 15 - 19 years, Female - pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 14'               AS S_NO,
           '15 - 19 years, Female - pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 15 15 - 19 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 15'                   AS S_NO,
           '15 - 19 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 16 20 - 24 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 16'  AS S_NO,
           '20 - 24 years, Male' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND sex = 'Male'
-- 17 20 - 24 years, Female - pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 17'               AS S_NO,
           '20 - 24 years, Female - pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 18 20 - 24 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 18'                   AS S_NO,
           '20 - 24 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 19 25 - 29 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 19'  AS S_NO,
           '25 - 29 years, Male' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND sex = 'Male'
-- 20 25 - 29 years, Female - pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 20'               AS S_NO,
           '25 - 29 years, Female - pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 21 25 - 29 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 21'                   AS S_NO,
           '25 - 29 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 22 30 - 34 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 22'  AS S_NO,
           '30 - 34 years, Male' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND sex = 'Male'
-- 23 30 - 34 years, Female - pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 23'               AS S_NO,
           '30 - 34 years, Female - pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 24 30 - 34 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 24'                   AS S_NO,
           '30 - 34 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 25 35 - 39 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 25'  AS S_NO,
           '35 - 39 years, Male' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND sex = 'Male'
-- 26 35 - 39 years, Female - pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 26'               AS S_NO,
           '35 - 39 years, Female - pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 27 35 - 39 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 27'                   AS S_NO,
           '35 - 39 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 28 40 - 44 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 28'  AS S_NO,
           '40 - 44 years, Male' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND sex = 'Male'
-- 29 40 - 44 years, Female - pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 29'               AS S_NO,
           '40 - 44 years, Female - pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 30 40 - 44 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 30'                   AS S_NO,
           '40 - 44 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 31 45 - 49 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 31'  AS S_NO,
           '45 - 49 years, Male' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND sex = 'Male'
-- 32 45 - 49 years, Female - pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 32'               AS S_NO,
           '45 - 49 years, Female - pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 33 45 - 49 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 33'                   AS S_NO,
           '45 - 49 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 34 >= 50 years, Male
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 34' AS S_NO,
           '>= 50 years, Male'  as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND sex = 'Male'
-- 36 >= 50 years, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_ART_RET_NET. 36'                 AS S_NO,
           '>= 50 years, Female - non-pregnant' as Activity,
           COUNT(*)
    FROM art_start_date_before_12_months
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND sex = 'Female';
END //

DELIMITER ;