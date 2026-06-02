DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_tb_scrn_query_v2;

CREATE PROCEDURE sp_fact_hmis_hiv_tb_scrn_query_v2(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
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
                             was_the_patient_screened_for_tuberc AS tb_screened,
                             tb_screening_date,
                             tb_treatment_status,
                             transferred_in_check_this_for_all_t AS transferred_in,
                             date_started_on_tuberculosis_prophy,
                             screening_test_result_tuberculosis   AS screening_result,
                             lf_lam_result
                      FROM tmp_hmis_follow_up),
         tmp_latest_follow_up as (SELECT client_id,
                                         follow_up_date                                                                             AS FollowupDate,
                                         encounter_id,
                                         follow_up_status,
                                         tb_screening_date,
                                         art_start_date,
                                         tb_screened,
                                         screening_result,
                                         lf_lam_result,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
                                    AND tb_screening_date is not null
                                 --   AND tb_screened = 'Yes'
                                    and tb_screening_date between REPORT_START_DATE AND REPORT_END_DATE
         ),
         latest_tb_screened_follow_up as (SELECT follow_up.client_id,
                                                 sex,
                                                 date_of_birth,
                                                 encounter_id,
                                                 follow_up_status,
                                                 tb_screening_date,
                                                 art_start_date,
                                                 tb_screened,
                                                 screening_result,
                                                 lf_lam_result,
                                                 mrn,
                                                 uan
                                          from tmp_latest_follow_up follow_up
                                                   inner join mamba_dim_client client on follow_up.client_id = client.client_id
                                          where row_num = 1),
         tx_new_tb_screened as (select *
                                from latest_tb_screened_follow_up
                                where art_start_date between REPORT_START_DATE AND REPORT_END_DATE
         ),
         prev_on_art_tb_screened as (select *
                                      from latest_tb_screened_follow_up
                                      where art_start_date < REPORT_START_DATE
                                     --   and follow_up_status in ('Alive', 'Restart medication')
         ),
         tb_scrn_agg AS (
             SELECT
                 COUNT(*) AS Value,
                 SUM(CASE WHEN age < 15  AND sex = 'Male'                                THEN 1 ELSE 0 END) AS u15_male,
                 SUM(CASE WHEN age < 15  AND sex = 'Female'                              THEN 1 ELSE 0 END) AS u15_female,
                 SUM(CASE WHEN age >= 15 AND sex = 'Male'                                THEN 1 ELSE 0 END) AS o15_male,
                 SUM(CASE WHEN age >= 15 AND sex = 'Female'                              THEN 1 ELSE 0 END) AS o15_female,
                 SUM(CASE WHEN screening_result = 'Positive'                             THEN 1 ELSE 0 END) AS pos_total,
                 SUM(CASE WHEN screening_result = 'Positive' AND age < 15  AND sex = 'Male'   THEN 1 ELSE 0 END) AS pos_u15_male,
                 SUM(CASE WHEN screening_result = 'Positive' AND age < 15  AND sex = 'Female' THEN 1 ELSE 0 END) AS pos_u15_female,
                 SUM(CASE WHEN screening_result = 'Positive' AND age >= 15 AND sex = 'Male'   THEN 1 ELSE 0 END) AS pos_o15_male,
                 SUM(CASE WHEN screening_result = 'Positive' AND age >= 15 AND sex = 'Female' THEN 1 ELSE 0 END) AS pos_o15_female
             FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age FROM tx_new_tb_screened) t
         ),
         prev_agg AS (
             SELECT
                 COUNT(*) AS total,
                 SUM(CASE WHEN age < 15  AND sex = 'Male'                                THEN 1 ELSE 0 END) AS u15_male,
                 SUM(CASE WHEN age < 15  AND sex = 'Female'                              THEN 1 ELSE 0 END) AS u15_female,
                 SUM(CASE WHEN age >= 15 AND sex = 'Male'                                THEN 1 ELSE 0 END) AS o15_male,
                 SUM(CASE WHEN age >= 15 AND sex = 'Female'                              THEN 1 ELSE 0 END) AS o15_female,
                 SUM(CASE WHEN screening_result = 'Positive'                             THEN 1 ELSE 0 END) AS pos_total,
                 SUM(CASE WHEN screening_result = 'Positive' AND age < 15  AND sex = 'Male'   THEN 1 ELSE 0 END) AS pos_u15_male,
                 SUM(CASE WHEN screening_result = 'Positive' AND age < 15  AND sex = 'Female' THEN 1 ELSE 0 END) AS pos_u15_female,
                 SUM(CASE WHEN screening_result = 'Positive' AND age >= 15 AND sex = 'Male'   THEN 1 ELSE 0 END) AS pos_o15_male,
                 SUM(CASE WHEN screening_result = 'Positive' AND age >= 15 AND sex = 'Female' THEN 1 ELSE 0 END) AS pos_o15_female
             FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age FROM prev_on_art_tb_screened) t
         )

    SELECT 'HIV_TB_SCRN' AS S_NO, 'Proportion of patients enrolled in HIV care who were screened for TB (FD)' AS Activity, COUNT(*) AS Value
    FROM latest_tb_screened_follow_up
    UNION ALL SELECT 'HIV_TB_SCRN.1',       'Number of NEWLY Enrolled ART clients who were screened for TB during the reporting period', Value          FROM tb_scrn_agg
    UNION ALL SELECT 'HIV_TB_SCRN.1. 1',    '< 15 years, Male',         COALESCE(u15_male,0)        FROM tb_scrn_agg
    UNION ALL SELECT 'HIV_TB_SCRN.1. 2',    '< 15 years, Female',       COALESCE(u15_female,0)      FROM tb_scrn_agg
    UNION ALL SELECT 'HIV_TB_SCRN.1. 3',    '>= 15 years, Male',        COALESCE(o15_male,0)        FROM tb_scrn_agg
    UNION ALL SELECT 'HIV_TB_SCRN.1. 4',    '>= 15 years, Female',      COALESCE(o15_female,0)      FROM tb_scrn_agg
    UNION ALL SELECT 'HIV_TB_SCRN_P',       'Screened Positive for TB', COALESCE(pos_total,0)       FROM tb_scrn_agg
    UNION ALL SELECT 'HIV_TB_SCRN_P. 1',    '< 15 years, Male',         COALESCE(pos_u15_male,0)    FROM tb_scrn_agg
    UNION ALL SELECT 'HIV_TB_SCRN_P. 2',    '< 15 years, Female',       COALESCE(pos_u15_female,0)  FROM tb_scrn_agg
    UNION ALL SELECT 'HIV_TB_SCRN_P. 3',    '>= 15 years, Male',        COALESCE(pos_o15_male,0)    FROM tb_scrn_agg
    UNION ALL SELECT 'HIV_TB_SCRN_P. 4',    '>= 15 years, Female',      COALESCE(pos_o15_female,0)  FROM tb_scrn_agg
    UNION ALL SELECT 'HIV_TB_SCRN_ART',     'Number of PLHIVs PREVIOUSLY on ART and screened for TB', Value  FROM prev_agg
    UNION ALL SELECT 'HIV_TB_SCRN_ART. 1',  '< 15 years, Male',         COALESCE(u15_male,0)        FROM prev_agg
    UNION ALL SELECT 'HIV_TB_SCRN_ART. 2',  '< 15 years, Female',       COALESCE(u15_female,0)      FROM prev_agg
    UNION ALL SELECT 'HIV_TB_SCRN_ART. 3',  '>= 15 years, Male',        COALESCE(o15_male,0)        FROM prev_agg
    UNION ALL SELECT 'HIV_TB_SCRN_ART. 4',  '>= 15 years, Female',      COALESCE(o15_female,0)      FROM prev_agg
    UNION ALL SELECT 'HIV_TB_SCRN_ART_P',   'Screened Positive for TB', COALESCE(pos_total,0)       FROM prev_agg
    UNION ALL SELECT 'HIV_TB_SCRN_ART_P. 1','< 15 years, Male',         COALESCE(pos_u15_male,0)    FROM prev_agg
    UNION ALL SELECT 'HIV_TB_SCRN_ART_P. 2','< 15 years, Female',       COALESCE(pos_u15_female,0)  FROM prev_agg
    UNION ALL SELECT 'HIV_TB_SCRN_ART_P. 3','>= 15 years, Male',        COALESCE(pos_o15_male,0)    FROM prev_agg
    UNION ALL SELECT 'HIV_TB_SCRN_ART_P. 4','>= 15 years, Female',      COALESCE(pos_o15_female,0)  FROM prev_agg;
END //

DELIMITER ;