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
     pvls_agg AS (
         SELECT
             COUNT(*)                                                                                                                                                                                                AS total,
             SUM(CASE WHEN ((viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed') OR (viral_load_count BETWEEN 50 AND 1000)                              THEN 1 ELSE 0 END) AS suppressed_or_lv,
             -- Total by age/sex (HIV_TX_PVLS.1)
             SUM(CASE WHEN age < 1    AND sex = 'Male'                                                                        THEN 1 ELSE 0 END)                                                AS t_u1_male,
             SUM(CASE WHEN age < 1    AND sex = 'Female'                                                                      THEN 1 ELSE 0 END)                                                AS t_u1_female_np,
             SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Male'                                                            THEN 1 ELSE 0 END)                                                AS t_u4_male,
             SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Female'                                                          THEN 1 ELSE 0 END)                                                AS t_u4_female_np,
             SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Male'                                                            THEN 1 ELSE 0 END)                                                AS t_u9_male,
             SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Female'                                                          THEN 1 ELSE 0 END)                                                AS t_u9_female_np,
             SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Male'                                                            THEN 1 ELSE 0 END)                                                AS t_u14_male,
             SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Female'                                                          THEN 1 ELSE 0 END)                                                AS t_u14_female_np,
             SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Male'                                                            THEN 1 ELSE 0 END)                                                AS t_u19_male,
             SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Female' AND pregnancy_status = 'Yes'                             THEN 1 ELSE 0 END)                                                AS t_u19_female_p,
             SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END)                                               AS t_u19_female_np,
             SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Male'                                                            THEN 1 ELSE 0 END)                                                AS t_u24_male,
             SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Female' AND pregnancy_status = 'Yes'                             THEN 1 ELSE 0 END)                                                AS t_u24_female_p,
             SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END)                                               AS t_u24_female_np,
             SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Male'                                                            THEN 1 ELSE 0 END)                                                AS t_u29_male,
             SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Female' AND pregnancy_status = 'Yes'                             THEN 1 ELSE 0 END)                                                AS t_u29_female_p,
             SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END)                                               AS t_u29_female_np,
             SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Male'                                                            THEN 1 ELSE 0 END)                                                AS t_u34_male,
             SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Female' AND pregnancy_status = 'Yes'                             THEN 1 ELSE 0 END)                                                AS t_u34_female_p,
             SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END)                                               AS t_u34_female_np,
             SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Male'                                                            THEN 1 ELSE 0 END)                                                AS t_u39_male,
             SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Female' AND pregnancy_status = 'Yes'                             THEN 1 ELSE 0 END)                                                AS t_u39_female_p,
             SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END)                                               AS t_u39_female_np,
             SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Male'                                                            THEN 1 ELSE 0 END)                                                AS t_u44_male,
             SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Female' AND pregnancy_status = 'Yes'                             THEN 1 ELSE 0 END)                                                AS t_u44_female_p,
             SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END)                                               AS t_u44_female_np,
             SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Male'                                                            THEN 1 ELSE 0 END)                                                AS t_u49_male,
             SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Female' AND pregnancy_status = 'Yes'                             THEN 1 ELSE 0 END)                                                AS t_u49_female_p,
             SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END)                                               AS t_u49_female_np,
             SUM(CASE WHEN age >= 50 AND sex = 'Male'                                                                        THEN 1 ELSE 0 END)                                                AS t_o50_male,
             SUM(CASE WHEN age >= 50 AND sex = 'Female'                                                                      THEN 1 ELSE 0 END)                                                AS t_o50_female_np,
             -- Suppressed (UN) by age/sex (HIV_TX_PVLS_UN)
             SUM(CASE WHEN (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                                                                        THEN 1 ELSE 0 END) AS un_total,
             SUM(CASE WHEN age < 1    AND sex = 'Male'   AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                                     THEN 1 ELSE 0 END) AS un_u1_male,
             SUM(CASE WHEN age < 1    AND sex = 'Female' AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                                     THEN 1 ELSE 0 END) AS un_u1_female_np,
             SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Male'   AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                         THEN 1 ELSE 0 END) AS un_u4_male,
             SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Female' AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                         THEN 1 ELSE 0 END) AS un_u4_female_np,
             SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Male'   AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                         THEN 1 ELSE 0 END) AS un_u9_male,
             SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Female' AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                         THEN 1 ELSE 0 END) AS un_u9_female_np,
             SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Male'   AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                         THEN 1 ELSE 0 END) AS un_u14_male,
             SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Female' AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                         THEN 1 ELSE 0 END) AS un_u14_female_np,
             SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Male'   AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                         THEN 1 ELSE 0 END) AS un_u19_male,
             SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Female' AND pregnancy_status = 'Yes'                             AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed' THEN 1 ELSE 0 END) AS un_u19_female_p,
             SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed' THEN 1 ELSE 0 END) AS un_u19_female_np,
             SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Male'   AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                         THEN 1 ELSE 0 END) AS un_u24_male,
             SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Female' AND pregnancy_status = 'Yes'                             AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed' THEN 1 ELSE 0 END) AS un_u24_female_p,
             SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed' THEN 1 ELSE 0 END) AS un_u24_female_np,
             SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Male'   AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                         THEN 1 ELSE 0 END) AS un_u29_male,
             SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Female' AND pregnancy_status = 'Yes'                             AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed' THEN 1 ELSE 0 END) AS un_u29_female_p,
             SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed' THEN 1 ELSE 0 END) AS un_u29_female_np,
             SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Male'   AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                         THEN 1 ELSE 0 END) AS un_u34_male,
             SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Female' AND pregnancy_status = 'Yes'                             AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed' THEN 1 ELSE 0 END) AS un_u34_female_p,
             SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed' THEN 1 ELSE 0 END) AS un_u34_female_np,
             SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Male'   AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                         THEN 1 ELSE 0 END) AS un_u39_male,
             SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Female' AND pregnancy_status = 'Yes'                             AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed' THEN 1 ELSE 0 END) AS un_u39_female_p,
             SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed' THEN 1 ELSE 0 END) AS un_u39_female_np,
             SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Male'   AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                         THEN 1 ELSE 0 END) AS un_u44_male,
             SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Female' AND pregnancy_status = 'Yes'                             AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed' THEN 1 ELSE 0 END) AS un_u44_female_p,
             SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed' THEN 1 ELSE 0 END) AS un_u44_female_np,
             SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Male'   AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                         THEN 1 ELSE 0 END) AS un_u49_male,
             SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Female' AND pregnancy_status = 'Yes'                             AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed' THEN 1 ELSE 0 END) AS un_u49_female_p,
             SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed' THEN 1 ELSE 0 END) AS un_u49_female_np,
             SUM(CASE WHEN age >= 50 AND sex = 'Male'   AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                                     THEN 1 ELSE 0 END) AS un_o50_male,
             SUM(CASE WHEN age >= 50 AND sex = 'Female' AND (viral_load_count < 50 OR viral_load_count IS NULL) AND viral_load_test_status = 'Suppressed'                                     THEN 1 ELSE 0 END) AS un_o50_female_np,
             -- Low viremia (LV) by age/sex (HIV_TX_PVLS_LV)
             SUM(CASE WHEN viral_load_count BETWEEN 50 AND 1000                                                                                                                                THEN 1 ELSE 0 END) AS lv_total,
             SUM(CASE WHEN age < 1    AND sex = 'Male'   AND viral_load_count BETWEEN 50 AND 1000                                                                                             THEN 1 ELSE 0 END) AS lv_u1_male,
             SUM(CASE WHEN age < 1    AND sex = 'Female' AND viral_load_count BETWEEN 50 AND 1000                                                                                             THEN 1 ELSE 0 END) AS lv_u1_female_np,
             SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Male'   AND viral_load_count BETWEEN 50 AND 1000                                                                                  THEN 1 ELSE 0 END) AS lv_u4_male,
             SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Female' AND viral_load_count BETWEEN 50 AND 1000                                                                                  THEN 1 ELSE 0 END) AS lv_u4_female_np,
             SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Male'   AND viral_load_count BETWEEN 50 AND 1000                                                                                  THEN 1 ELSE 0 END) AS lv_u9_male,
             SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Female' AND viral_load_count BETWEEN 50 AND 1000                                                                                  THEN 1 ELSE 0 END) AS lv_u9_female_np,
             SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Male'   AND viral_load_count BETWEEN 50 AND 1000                                                                                  THEN 1 ELSE 0 END) AS lv_u14_male,
             SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Female' AND viral_load_count BETWEEN 50 AND 1000                                                                                  THEN 1 ELSE 0 END) AS lv_u14_female_np,
             SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Male'   AND viral_load_count BETWEEN 50 AND 1000                                                                                  THEN 1 ELSE 0 END) AS lv_u19_male,
             SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Female' AND pregnancy_status = 'Yes'                             AND viral_load_count BETWEEN 50 AND 1000                        THEN 1 ELSE 0 END) AS lv_u19_female_p,
             SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) AND viral_load_count BETWEEN 50 AND 1000                       THEN 1 ELSE 0 END) AS lv_u19_female_np,
             SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Male'   AND viral_load_count BETWEEN 50 AND 1000                                                                                  THEN 1 ELSE 0 END) AS lv_u24_male,
             SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Female' AND pregnancy_status = 'Yes'                             AND viral_load_count BETWEEN 50 AND 1000                        THEN 1 ELSE 0 END) AS lv_u24_female_p,
             SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) AND viral_load_count BETWEEN 50 AND 1000                       THEN 1 ELSE 0 END) AS lv_u24_female_np,
             SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Male'   AND viral_load_count BETWEEN 50 AND 1000                                                                                  THEN 1 ELSE 0 END) AS lv_u29_male,
             SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Female' AND pregnancy_status = 'Yes'                             AND viral_load_count BETWEEN 50 AND 1000                        THEN 1 ELSE 0 END) AS lv_u29_female_p,
             SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) AND viral_load_count BETWEEN 50 AND 1000                       THEN 1 ELSE 0 END) AS lv_u29_female_np,
             SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Male'   AND viral_load_count BETWEEN 50 AND 1000                                                                                  THEN 1 ELSE 0 END) AS lv_u34_male,
             SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Female' AND pregnancy_status = 'Yes'                             AND viral_load_count BETWEEN 50 AND 1000                        THEN 1 ELSE 0 END) AS lv_u34_female_p,
             SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) AND viral_load_count BETWEEN 50 AND 1000                       THEN 1 ELSE 0 END) AS lv_u34_female_np,
             SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Male'   AND viral_load_count BETWEEN 50 AND 1000                                                                                  THEN 1 ELSE 0 END) AS lv_u39_male,
             SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Female' AND pregnancy_status = 'Yes'                             AND viral_load_count BETWEEN 50 AND 1000                        THEN 1 ELSE 0 END) AS lv_u39_female_p,
             SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) AND viral_load_count BETWEEN 50 AND 1000                       THEN 1 ELSE 0 END) AS lv_u39_female_np,
             SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Male'   AND viral_load_count BETWEEN 50 AND 1000                                                                                  THEN 1 ELSE 0 END) AS lv_u44_male,
             SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Female' AND pregnancy_status = 'Yes'                             AND viral_load_count BETWEEN 50 AND 1000                        THEN 1 ELSE 0 END) AS lv_u44_female_p,
             SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) AND viral_load_count BETWEEN 50 AND 1000                       THEN 1 ELSE 0 END) AS lv_u44_female_np,
             SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Male'   AND viral_load_count BETWEEN 50 AND 1000                                                                                  THEN 1 ELSE 0 END) AS lv_u49_male,
             SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Female' AND pregnancy_status = 'Yes'                             AND viral_load_count BETWEEN 50 AND 1000                        THEN 1 ELSE 0 END) AS lv_u49_female_p,
             SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) AND viral_load_count BETWEEN 50 AND 1000                       THEN 1 ELSE 0 END) AS lv_u49_female_np,
             SUM(CASE WHEN age >= 50 AND sex = 'Male'   AND viral_load_count BETWEEN 50 AND 1000                                                                                              THEN 1 ELSE 0 END) AS lv_o50_male,
             SUM(CASE WHEN age >= 50 AND sex = 'Female' AND viral_load_count BETWEEN 50 AND 1000                                                                                              THEN 1 ELSE 0 END) AS lv_o50_female_np
         FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) AS age FROM pvls) t
     )

SELECT 'HIV_TX_PVLS' AS S_NO,
       'Viral load Suppression (Percentage of ART clients with a suppressed viral load among those with a viral load test at 12 month in the reporting period)' AS Activity,
       CASE WHEN total = 0 THEN '0%'
            ELSE CONCAT(CAST(ROUND((CAST(suppressed_or_lv AS REAL) * 100.0) / total, 2) AS CHAR), '%')
            END AS Value
FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1',      'Number of adult and pediatric ART patients for whom viral load test result received in the reporting period (with in the past 12 months)', total          FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 1',  '< 1 year, Male',                       t_u1_male        FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 3',  '< 1 year, Female - non-pregnant',       t_u1_female_np   FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 4',  '1 - 4 years, Male',                     t_u4_male        FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 6',  '1 - 4 years, Female - non-pregnant',    t_u4_female_np   FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 7',  '5 - 9 years, Male',                     t_u9_male        FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 9',  '5 - 9 years, Female - non-pregnant',    t_u9_female_np   FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 10', '10 - 14 years, Male',                   t_u14_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 12', '10 - 14 years, Female - non-pregnant',  t_u14_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 13', '15 - 19 years, Male',                   t_u19_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 14', '15 - 19 years, Female - pregnant',      t_u19_female_p   FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 15', '15 - 19 years, Female - non-pregnant',  t_u19_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 16', '20 - 24 years, Male',                   t_u24_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 17', '20 - 24 years, Female - pregnant',      t_u24_female_p   FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 18', '20 - 24 years, Female - non-pregnant',  t_u24_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 19', '25 - 29 years, Male',                   t_u29_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 20', '25 - 29 years, Female - pregnant',      t_u29_female_p   FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 21', '25 - 29 years, Female - non-pregnant',  t_u29_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 22', '30 - 34 years, Male',                   t_u34_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 23', '30 - 34 years, Female - pregnant',      t_u34_female_p   FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 24', '30 - 34 years, Female - non-pregnant',  t_u34_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 25', '35 - 39 years, Male',                   t_u39_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 26', '35 - 39 years, Female - pregnant',      t_u39_female_p   FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 27', '35 - 39 years, Female - non-pregnant',  t_u39_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 28', '40 - 44 years, Male',                   t_u44_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 29', '40 - 44 years, Female - pregnant',      t_u44_female_p   FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 30', '40 - 44 years, Female - non-pregnant',  t_u44_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 31', '45 - 49 years, Male',                   t_u49_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 32', '45 - 49 years, Female - pregnant',      t_u49_female_p   FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 33', '45 - 49 years, Female - non-pregnant',  t_u49_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 34', '>= 50 years, Male',                     t_o50_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS.1. 36', '>= 50 years, Female - non-pregnant',    t_o50_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN',    'Total number of adult and paediatric ART patients with an undetectable viral load(<50 copies/ml) in the reporting period  (with in the past 12 months)', un_total FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 1',  '< 1 year, Male',                       un_u1_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 3',  '< 1 year, Female - non-pregnant',       un_u1_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 4',  '1 - 4 years, Male',                     un_u4_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 6',  '1 - 4 years, Female - non-pregnant',    un_u4_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 7',  '5 - 9 years, Male',                     un_u9_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 9',  '5 - 9 years, Female - non-pregnant',    un_u9_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 10', '10 - 14 years, Male',                   un_u14_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 12', '10 - 14 years, Female - non-pregnant',  un_u14_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 13', '15 - 19 years, Male',                   un_u19_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 14', '15 - 19 years, Female - pregnant',      un_u19_female_p  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 15', '15 - 19 years, Female - non-pregnant',  un_u19_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 16', '20 - 24 years, Male',                   un_u24_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 17', '20 - 24 years, Female - pregnant',      un_u24_female_p  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 18', '20 - 24 years, Female - non-pregnant',  un_u24_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 19', '25 - 29 years, Male',                   un_u29_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 20', '25 - 29 years, Female - pregnant',      un_u29_female_p  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 21', '25 - 29 years, Female - non-pregnant',  un_u29_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 22', '30 - 34 years, Male',                   un_u34_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 23', '30 - 34 years, Female - pregnant',      un_u34_female_p  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 24', '30 - 34 years, Female - non-pregnant',  un_u34_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 25', '35 - 39 years, Male',                   un_u39_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 26', '35 - 39 years, Female - pregnant',      un_u39_female_p  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 27', '35 - 39 years, Female - non-pregnant',  un_u39_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 28', '40 - 44 years, Male',                   un_u44_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 29', '40 - 44 years, Female - pregnant',      un_u44_female_p  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 30', '40 - 44 years, Female - non-pregnant',  un_u44_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 31', '45 - 49 years, Male',                   un_u49_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 32', '45 - 49 years, Female - pregnant',      un_u49_female_p  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 33', '45 - 49 years, Female - non-pregnant',  un_u49_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 34', '>= 50 years, Male',                     un_o50_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_UN. 36', '>= 50 years, Female - non-pregnant',    un_o50_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV',    'Total number of adult and paediatric ART patients with low level viremia (50 -1000 copies/ml) in the reporting period  (with in the past 12 months)', lv_total FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 1',  '< 1 year, Male',                       lv_u1_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 3',  '< 1 year, Female - non-pregnant',       lv_u1_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 4',  '1 - 4 years, Male',                     lv_u4_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 6',  '1 - 4 years, Female - non-pregnant',    lv_u4_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 7',  '5 - 9 years, Male',                     lv_u9_male       FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 9',  '5 - 9 years, Female - non-pregnant',    lv_u9_female_np  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 10', '10 - 14 years, Male',                   lv_u14_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 12', '10 - 14 years, Female - non-pregnant',  lv_u14_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 13', '15 - 19 years, Male',                   lv_u19_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 14', '15 - 19 years, Female - pregnant',      lv_u19_female_p  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 15', '15 - 19 years, Female - non-pregnant',  lv_u19_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 16', '20 - 24 years, Male',                   lv_u24_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 17', '20 - 24 years, Female - pregnant',      lv_u24_female_p  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 18', '20 - 24 years, Female - non-pregnant',  lv_u24_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 19', '25 - 29 years, Male',                   lv_u29_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 20', '25 - 29 years, Female - pregnant',      lv_u29_female_p  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 21', '25 - 29 years, Female - non-pregnant',  lv_u29_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 22', '30 - 34 years, Male',                   lv_u34_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 23', '30 - 34 years, Female - pregnant',      lv_u34_female_p  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 24', '30 - 34 years, Female - non-pregnant',  lv_u34_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 25', '35 - 39 years, Male',                   lv_u39_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 26', '35 - 39 years, Female - pregnant',      lv_u39_female_p  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 27', '35 - 39 years, Female - non-pregnant',  lv_u39_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 28', '40 - 44 years, Male',                   lv_u44_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 29', '40 - 44 years, Female - pregnant',      lv_u44_female_p  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 30', '40 - 44 years, Female - non-pregnant',  lv_u44_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 31', '45 - 49 years, Male',                   lv_u49_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 32', '45 - 49 years, Female - pregnant',      lv_u49_female_p  FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 33', '45 - 49 years, Female - non-pregnant',  lv_u49_female_np FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 34', '>= 50 years, Male',                     lv_o50_male      FROM pvls_agg
UNION ALL SELECT 'HIV_TX_PVLS_LV. 36', '>= 50 years, Female - non-pregnant',    lv_o50_female_np FROM pvls_agg;
END //

DELIMITER ;