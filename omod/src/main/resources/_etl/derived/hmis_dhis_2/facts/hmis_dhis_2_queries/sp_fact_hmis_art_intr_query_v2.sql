DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_art_intr_query_v2;

CREATE PROCEDURE sp_fact_hmis_art_intr_query_v2(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
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
                             antiretroviral_art_dispensed_dose_i AS dose_days,
                             pregnancy_status,
                             transferred_in_check_this_for_all_t AS transferred_in,
                             adherence
                      FROM tmp_hmis_follow_up),
         -- TX curr start
         tmp_latest_follow_up_start AS (SELECT client_id,
                                               follow_up_date,
                                               encounter_id,
                                               follow_up_status,
                                               treatment_end_date,
                                               art_start_date,
                                               regimen,
                                               dose_days,
                                               adherence,
                                               next_visit_date,
                                               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                        FROM FollowUp
                                        WHERE follow_up_status IS NOT NULL
                                          AND art_start_date IS NOT NULL
                                          AND follow_up_date <= DATE_ADD(REPORT_START_DATE, INTERVAL -1 DAY)),
         tx_curr_start AS (select *
                           from tmp_latest_follow_up_start
                           where row_num = 1
                             AND follow_up_status in ('Alive', 'Restart medication')
                             AND treatment_end_date >= DATE_ADD(REPORT_START_DATE, INTERVAL -1 DAY)),
         -- TX curr
         tmp_lost_follow_up AS (SELECT client_id,
                                       follow_up_date,
                                       encounter_id,
                                       follow_up_status,
                                       treatment_end_date,
                                       art_start_date,
                                       regimen,
                                       dose_days,
                                       adherence,
                                       next_visit_date,
                                       ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                FROM FollowUp
                                WHERE follow_up_status IS NOT NULL
                                  AND art_start_date IS NOT NULL
                                  AND follow_up_date <= REPORT_END_DATE),
         lost_follow_up as (select *
                            from tmp_lost_follow_up
                            where row_num = 1),
         tx_curr_end AS (select *
                         from tmp_lost_follow_up
                         where row_num = 1
                           AND follow_up_status in ('Alive', 'Restart medication')
                           AND treatment_end_date >= REPORT_END_DATE),

         tmp_first_follow_up as (SELECT client_id,
                                        follow_up_date,
                                        encounter_id,
                                        transferred_in,
                                        pregnancy_status,
                                        follow_up_status,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date , encounter_id ) AS row_num
                                 FROM FollowUp
                                 WHERE art_start_date IS NOT NULL),
         first_follow_up as (select * from tmp_first_follow_up where row_num = 1),

         tx_new_tmp as (select tmp_lost_follow_up.*
                        from tmp_lost_follow_up
                                 join first_follow_up on tmp_lost_follow_up.client_id = first_follow_up.client_id
                        where art_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                          AND (first_follow_up.transferred_in is null or first_follow_up.transferred_in != 'Yes')
                          AND first_follow_up.follow_up_status in ('Alive', 'Restart medication')
                          AND tmp_lost_follow_up.row_num = 1),
         tx_new as (select *
                    from tx_new_tmp),

         on_art as (select *
                    from tx_new
                    union ALL
                    select *
                    from tx_curr_start),
         interrupted_art as (select on_art.*,
                                    client.sex,
                                    client.date_of_birth,
                                    TIMESTAMPDIFF(YEAR, client.date_of_birth, REPORT_END_DATE)        as Age,
                                    lost_follow_up.follow_up_status                                   as lost_follow_up_status,
                                    lost_follow_up.follow_up_date                                     as lost_follow_up_date
                             from on_art
                                      join mamba_dim_client client on on_art.client_id = client.client_id
                                      join lost_follow_up
                                           on on_art.client_id = lost_follow_up.client_id
                             where on_art.client_id not in (select client_id from tx_curr_end)),
         intr_agg AS (
             SELECT
                 COUNT(*) AS total,
                 SUM(CASE WHEN TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) < 3  AND lost_follow_up_status NOT IN ('Transferred out', 'Stop all', 'Dead')                   THEN 1 ELSE 0 END) AS out1_total,
                 SUM(CASE WHEN TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) < 3  AND lost_follow_up_status NOT IN ('Transferred out', 'Stop all', 'Dead') AND Age < 15  AND sex = 'Male'   THEN 1 ELSE 0 END) AS out1_u15_male,
                 SUM(CASE WHEN TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) < 3  AND lost_follow_up_status NOT IN ('Transferred out', 'Stop all', 'Dead') AND Age < 15  AND sex = 'Female' THEN 1 ELSE 0 END) AS out1_u15_female,
                 SUM(CASE WHEN TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) < 3  AND lost_follow_up_status NOT IN ('Transferred out', 'Stop all', 'Dead') AND Age >= 15 AND sex = 'Male'   THEN 1 ELSE 0 END) AS out1_o15_male,
                 SUM(CASE WHEN TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) < 3  AND lost_follow_up_status NOT IN ('Transferred out', 'Stop all', 'Dead') AND Age >= 15 AND sex = 'Female' THEN 1 ELSE 0 END) AS out1_o15_female,
                 SUM(CASE WHEN TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) >= 3 AND lost_follow_up_status NOT IN ('Transferred out', 'Stop all', 'Dead')                   THEN 1 ELSE 0 END) AS out2_total,
                 SUM(CASE WHEN TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) >= 3 AND lost_follow_up_status NOT IN ('Transferred out', 'Stop all', 'Dead') AND Age < 15  AND sex = 'Male'   THEN 1 ELSE 0 END) AS out2_u15_male,
                 SUM(CASE WHEN TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) >= 3 AND lost_follow_up_status NOT IN ('Transferred out', 'Stop all', 'Dead') AND Age < 15  AND sex = 'Female' THEN 1 ELSE 0 END) AS out2_u15_female,
                 SUM(CASE WHEN TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) >= 3 AND lost_follow_up_status NOT IN ('Transferred out', 'Stop all', 'Dead') AND Age >= 15 AND sex = 'Male'   THEN 1 ELSE 0 END) AS out2_o15_male,
                 SUM(CASE WHEN TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) >= 3 AND lost_follow_up_status NOT IN ('Transferred out', 'Stop all', 'Dead') AND Age >= 15 AND sex = 'Female' THEN 1 ELSE 0 END) AS out2_o15_female,
                 SUM(CASE WHEN lost_follow_up_status = 'Transferred out'                                                                                                              THEN 1 ELSE 0 END) AS out3_total,
                 SUM(CASE WHEN lost_follow_up_status = 'Transferred out' AND Age < 15  AND sex = 'Male'                                                                              THEN 1 ELSE 0 END) AS out3_u15_male,
                 SUM(CASE WHEN lost_follow_up_status = 'Transferred out' AND Age < 15  AND sex = 'Female'                                                                            THEN 1 ELSE 0 END) AS out3_u15_female,
                 SUM(CASE WHEN lost_follow_up_status = 'Transferred out' AND Age >= 15 AND sex = 'Male'                                                                              THEN 1 ELSE 0 END) AS out3_o15_male,
                 SUM(CASE WHEN lost_follow_up_status = 'Transferred out' AND Age >= 15 AND sex = 'Female'                                                                            THEN 1 ELSE 0 END) AS out3_o15_female,
                 SUM(CASE WHEN lost_follow_up_status = 'Stop all'                                                                                                                    THEN 1 ELSE 0 END) AS out4_total,
                 SUM(CASE WHEN lost_follow_up_status = 'Stop all' AND Age < 15  AND sex = 'Male'                                                                                     THEN 1 ELSE 0 END) AS out4_u15_male,
                 SUM(CASE WHEN lost_follow_up_status = 'Stop all' AND Age < 15  AND sex = 'Female'                                                                                   THEN 1 ELSE 0 END) AS out4_u15_female,
                 SUM(CASE WHEN lost_follow_up_status = 'Stop all' AND Age >= 15 AND sex = 'Male'                                                                                     THEN 1 ELSE 0 END) AS out4_o15_male,
                 SUM(CASE WHEN lost_follow_up_status = 'Stop all' AND Age >= 15 AND sex = 'Female'                                                                                   THEN 1 ELSE 0 END) AS out4_o15_female,
                 SUM(CASE WHEN lost_follow_up_status = 'Dead'                                                                                                                        THEN 1 ELSE 0 END) AS out5_total,
                 SUM(CASE WHEN lost_follow_up_status = 'Dead' AND Age < 15  AND sex = 'Male'                                                                                         THEN 1 ELSE 0 END) AS out5_u15_male,
                 SUM(CASE WHEN lost_follow_up_status = 'Dead' AND Age < 15  AND sex = 'Female'                                                                                       THEN 1 ELSE 0 END) AS out5_u15_female,
                 SUM(CASE WHEN lost_follow_up_status = 'Dead' AND Age >= 15 AND sex = 'Male'                                                                                         THEN 1 ELSE 0 END) AS out5_o15_male,
                 SUM(CASE WHEN lost_follow_up_status = 'Dead' AND Age >= 15 AND sex = 'Female'                                                                                       THEN 1 ELSE 0 END) AS out5_o15_female
             FROM interrupted_art
         )

    SELECT 'HIV_ART_INTR' AS S_NO, 'Number of ART Clients that interrupted Treatment' AS Activity, total AS Value FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT',    'Number of ART Clients Interrupted treatment by outcome', total          FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.1',   'Lost after treatemnt < 3month',    COALESCE(out1_total,0)      FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.1. 1','< 15 years, Male',                 COALESCE(out1_u15_male,0)   FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.1. 2','< 15 years, Female',               COALESCE(out1_u15_female,0) FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.1. 3','>= 15 years, Male',                COALESCE(out1_o15_male,0)   FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.1. 4','>= 15 years, Female',              COALESCE(out1_o15_female,0) FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.2',   'Lost after treatement > 3month',   COALESCE(out2_total,0)      FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.2. 1','< 15 years, Male',                 COALESCE(out2_u15_male,0)   FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.2. 2','< 15 years, Female',               COALESCE(out2_u15_female,0) FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.2. 3','>= 15 years, Male',                COALESCE(out2_o15_male,0)   FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.2. 4','>= 15 years, Female',              COALESCE(out2_o15_female,0) FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.3',   'Transferred out',                  COALESCE(out3_total,0)      FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.3. 1','< 15 years, Male',                 COALESCE(out3_u15_male,0)   FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.3. 2','< 15 years, Female',               COALESCE(out3_u15_female,0) FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.3. 3','>= 15 years, Male',                COALESCE(out3_o15_male,0)   FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.3. 4','>= 15 years, Female',              COALESCE(out3_o15_female,0) FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.4',   'Refused (stopped) treatement',     COALESCE(out4_total,0)      FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.4. 1','< 15 years, Male',                 COALESCE(out4_u15_male,0)   FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.4. 2','< 15 years, Female',               COALESCE(out4_u15_female,0) FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.4. 3','>= 15 years, Male',                COALESCE(out4_o15_male,0)   FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.4. 4','>= 15 years, Female',              COALESCE(out4_o15_female,0) FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.5',   'Died',                             COALESCE(out5_total,0)      FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.5. 1','< 15 years, Male',                 COALESCE(out5_u15_male,0)   FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.5. 2','< 15 years, Female',               COALESCE(out5_u15_female,0) FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.5. 3','>= 15 years, Male',                COALESCE(out5_o15_male,0)   FROM intr_agg
    UNION ALL SELECT 'HIV_ART_INTR_OUT.5. 4','>= 15 years, Female',              COALESCE(out5_o15_female,0) FROM intr_agg;
END //

DELIMITER ;