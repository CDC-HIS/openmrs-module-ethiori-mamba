DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_art_ret_net_query_v2;

CREATE PROCEDURE sp_fact_hmis_hiv_art_ret_net_query_v2(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH FollowUp AS (SELECT encounter_id,
                             client_id,
                             follow_up_status,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date       AS art_start_date,
                             treatment_end_date,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child          breast_feeding_status,
                             pregnancy_status,
                             transferred_in_check_this_for_all_t AS transferred_in
                      FROM tmp_hmis_follow_up),
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
                                               and latest_follow_up.follow_up_status != 'Transferred out'),
         net_agg AS (
             SELECT
                 COUNT(*)                                                                                                                                          AS total,
                 SUM(CASE WHEN age < 1    AND sex = 'Male'                                                                        THEN 1 ELSE 0 END) AS u1_male,
                 SUM(CASE WHEN age < 1    AND sex = 'Female'                                                                      THEN 1 ELSE 0 END) AS u1_female_np,
                 SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Male'                                                            THEN 1 ELSE 0 END) AS u4_male,
                 SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Female'                                                          THEN 1 ELSE 0 END) AS u4_female_np,
                 SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Male'                                                            THEN 1 ELSE 0 END) AS u9_male,
                 SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Female'                                                          THEN 1 ELSE 0 END) AS u9_female_np,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Male'                                                            THEN 1 ELSE 0 END) AS u14_male,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Female'                                                          THEN 1 ELSE 0 END) AS u14_female_np,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Male'                                                            THEN 1 ELSE 0 END) AS u19_male,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Female' AND pregnancy_status = 'Yes'                             THEN 1 ELSE 0 END) AS u19_female_p,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END) AS u19_female_np,
                 SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Male'                                                            THEN 1 ELSE 0 END) AS u24_male,
                 SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Female' AND pregnancy_status = 'Yes'                             THEN 1 ELSE 0 END) AS u24_female_p,
                 SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END) AS u24_female_np,
                 SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Male'                                                            THEN 1 ELSE 0 END) AS u29_male,
                 SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Female' AND pregnancy_status = 'Yes'                             THEN 1 ELSE 0 END) AS u29_female_p,
                 SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END) AS u29_female_np,
                 SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Male'                                                            THEN 1 ELSE 0 END) AS u34_male,
                 SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Female' AND pregnancy_status = 'Yes'                             THEN 1 ELSE 0 END) AS u34_female_p,
                 SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END) AS u34_female_np,
                 SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Male'                                                            THEN 1 ELSE 0 END) AS u39_male,
                 SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Female' AND pregnancy_status = 'Yes'                             THEN 1 ELSE 0 END) AS u39_female_p,
                 SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END) AS u39_female_np,
                 SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Male'                                                            THEN 1 ELSE 0 END) AS u44_male,
                 SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Female' AND pregnancy_status = 'Yes'                             THEN 1 ELSE 0 END) AS u44_female_p,
                 SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END) AS u44_female_np,
                 SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Male'                                                            THEN 1 ELSE 0 END) AS u49_male,
                 SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Female' AND pregnancy_status = 'Yes'                             THEN 1 ELSE 0 END) AS u49_female_p,
                 SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END) AS u49_female_np,
                 SUM(CASE WHEN age >= 50 AND sex = 'Male'                                                                        THEN 1 ELSE 0 END) AS o50_male,
                 SUM(CASE WHEN age >= 50 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL)            THEN 1 ELSE 0 END) AS o50_female_np
             FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age FROM art_start_date_before_12_months) t
         )

    SELECT 'HIV_ART_RET_NET' AS S_NO, 'Number of persons on ART in the original cohort including those transferred in, minus those transferred out (net current cohort)' AS Activity, total AS Value FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 1',  '< 1 year, Male',                       COALESCE(u1_male,0)        FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 3',  '< 1 year, Female - non-pregnant',       COALESCE(u1_female_np,0)   FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 4',  '1 - 4 years, Male',                     COALESCE(u4_male,0)        FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 6',  '1 - 4 years, Female - non-pregnant',    COALESCE(u4_female_np,0)   FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 7',  '5 - 9 years, Male',                     COALESCE(u9_male,0)        FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 9',  '5 - 9 years, Female - non-pregnant',    COALESCE(u9_female_np,0)   FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 10', '10 - 14 years, Male',                   COALESCE(u14_male,0)       FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 12', '10 - 14 years, Female - non-pregnant',  COALESCE(u14_female_np,0)  FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 13', '15 - 19 years, Male',                   COALESCE(u19_male,0)       FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 14', '15 - 19 years, Female - pregnant',      COALESCE(u19_female_p,0)   FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 15', '15 - 19 years, Female - non-pregnant',  COALESCE(u19_female_np,0)  FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 16', '20 - 24 years, Male',                   COALESCE(u24_male,0)       FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 17', '20 - 24 years, Female - pregnant',      COALESCE(u24_female_p,0)   FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 18', '20 - 24 years, Female - non-pregnant',  COALESCE(u24_female_np,0)  FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 19', '25 - 29 years, Male',                   COALESCE(u29_male,0)       FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 20', '25 - 29 years, Female - pregnant',      COALESCE(u29_female_p,0)   FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 21', '25 - 29 years, Female - non-pregnant',  COALESCE(u29_female_np,0)  FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 22', '30 - 34 years, Male',                   COALESCE(u34_male,0)       FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 23', '30 - 34 years, Female - pregnant',      COALESCE(u34_female_p,0)   FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 24', '30 - 34 years, Female - non-pregnant',  COALESCE(u34_female_np,0)  FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 25', '35 - 39 years, Male',                   COALESCE(u39_male,0)       FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 26', '35 - 39 years, Female - pregnant',      COALESCE(u39_female_p,0)   FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 27', '35 - 39 years, Female - non-pregnant',  COALESCE(u39_female_np,0)  FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 28', '40 - 44 years, Male',                   COALESCE(u44_male,0)       FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 29', '40 - 44 years, Female - pregnant',      COALESCE(u44_female_p,0)   FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 30', '40 - 44 years, Female - non-pregnant',  COALESCE(u44_female_np,0)  FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 31', '45 - 49 years, Male',                   COALESCE(u49_male,0)       FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 32', '45 - 49 years, Female - pregnant',      COALESCE(u49_female_p,0)   FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 33', '45 - 49 years, Female - non-pregnant',  COALESCE(u49_female_np,0)  FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 34', '>= 50 years, Male',                     COALESCE(o50_male,0)       FROM net_agg
    UNION ALL SELECT 'HIV_ART_RET_NET. 36', '>= 50 years, Female - non-pregnant',    COALESCE(o50_female_np,0)  FROM net_agg;
END //

DELIMITER ;