DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_tx_new_query_v2;

CREATE PROCEDURE sp_fact_hmis_tx_new_query_v2(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
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
                             join mamba_dim_client client on tx_new_tmp.client_id = client.client_id),
         tx_new_agg AS (
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
             FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age FROM tx_new) t
         )

    SELECT 'HIV_TX_NEW' AS S_NO, 'Number of adults and children with HIV infection newly started on ART' AS Activity, total AS Value FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 1',  '< 1 year, Male',                       COALESCE(u1_male,0)        FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 3',  '< 1 year, Female - non-pregnant',       COALESCE(u1_female_np,0)   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 4',  '1 - 4 years, Male',                     COALESCE(u4_male,0)        FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 6',  '1 - 4 years, Female - non-pregnant',    COALESCE(u4_female_np,0)   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 7',  '5 - 9 years, Male',                     COALESCE(u9_male,0)        FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 9',  '5 - 9 years, Female - non-pregnant',    COALESCE(u9_female_np,0)   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 10', '10 - 14 years, Male',                   COALESCE(u14_male,0)       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 12', '10 - 14 years, Female - non-pregnant',  COALESCE(u14_female_np,0)  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 13', '15 - 19 years, Male',                   COALESCE(u19_male,0)       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 14', '15 - 19 years, Female - pregnant',      COALESCE(u19_female_p,0)   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 15', '15 - 19 years, Female - non-pregnant',  COALESCE(u19_female_np,0)  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 16', '20 - 24 years, Male',                   COALESCE(u24_male,0)       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 17', '20 - 24 years, Female - pregnant',      COALESCE(u24_female_p,0)   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 18', '20 - 24 years, Female - non-pregnant',  COALESCE(u24_female_np,0)  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 19', '25 - 29 years, Male',                   COALESCE(u29_male,0)       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 20', '25 - 29 years, Female - pregnant',      COALESCE(u29_female_p,0)   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 21', '25 - 29 years, Female - non-pregnant',  COALESCE(u29_female_np,0)  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 22', '30 - 34 years, Male',                   COALESCE(u34_male,0)       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 23', '30 - 34 years, Female - pregnant',      COALESCE(u34_female_p,0)   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 24', '30 - 34 years, Female - non-pregnant',  COALESCE(u34_female_np,0)  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 25', '35 - 39 years, Male',                   COALESCE(u39_male,0)       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 26', '35 - 39 years, Female - pregnant',      COALESCE(u39_female_p,0)   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 27', '35 - 39 years, Female - non-pregnant',  COALESCE(u39_female_np,0)  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 28', '40 - 44 years, Male',                   COALESCE(u44_male,0)       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 29', '40 - 44 years, Female - pregnant',      COALESCE(u44_female_p,0)   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 30', '40 - 44 years, Female - non-pregnant',  COALESCE(u44_female_np,0)  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 31', '45 - 49 years, Male',                   COALESCE(u49_male,0)       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 32', '45 - 49 years, Female - pregnant',      COALESCE(u49_female_p,0)   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 33', '45 - 49 years, Female - non-pregnant',  COALESCE(u49_female_np,0)  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 34', '>= 50 years, Male',                     COALESCE(o50_male,0)       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 36', '>= 50 years, Female - non-pregnant',    COALESCE(o50_female_np,0)  FROM tx_new_agg;
END //

DELIMITER ;
