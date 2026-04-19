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
    UNION ALL SELECT 'HIV_TX_NEW. 1',  '< 1 year, Male',                       u1_male        FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 3',  '< 1 year, Female - non-pregnant',       u1_female_np   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 4',  '1 - 4 years, Male',                     u4_male        FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 6',  '1 - 4 years, Female - non-pregnant',    u4_female_np   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 7',  '5 - 9 years, Male',                     u9_male        FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 9',  '5 - 9 years, Female - non-pregnant',    u9_female_np   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 10', '10 - 14 years, Male',                   u14_male       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 12', '10 - 14 years, Female - non-pregnant',  u14_female_np  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 13', '15 - 19 years, Male',                   u19_male       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 14', '15 - 19 years, Female - pregnant',      u19_female_p   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 15', '15 - 19 years, Female - non-pregnant',  u19_female_np  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 16', '20 - 24 years, Male',                   u24_male       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 17', '20 - 24 years, Female - pregnant',      u24_female_p   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 18', '20 - 24 years, Female - non-pregnant',  u24_female_np  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 19', '25 - 29 years, Male',                   u29_male       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 20', '25 - 29 years, Female - pregnant',      u29_female_p   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 21', '25 - 29 years, Female - non-pregnant',  u29_female_np  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 22', '30 - 34 years, Male',                   u34_male       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 23', '30 - 34 years, Female - pregnant',      u34_female_p   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 24', '30 - 34 years, Female - non-pregnant',  u34_female_np  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 25', '35 - 39 years, Male',                   u39_male       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 26', '35 - 39 years, Female - pregnant',      u39_female_p   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 27', '35 - 39 years, Female - non-pregnant',  u39_female_np  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 28', '40 - 44 years, Male',                   u44_male       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 29', '40 - 44 years, Female - pregnant',      u44_female_p   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 30', '40 - 44 years, Female - non-pregnant',  u44_female_np  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 31', '45 - 49 years, Male',                   u49_male       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 32', '45 - 49 years, Female - pregnant',      u49_female_p   FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 33', '45 - 49 years, Female - non-pregnant',  u49_female_np  FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 34', '>= 50 years, Male',                     o50_male       FROM tx_new_agg
    UNION ALL SELECT 'HIV_TX_NEW. 36', '>= 50 years, Female - non-pregnant',    o50_female_np  FROM tx_new_agg;
END //

DELIMITER ;
