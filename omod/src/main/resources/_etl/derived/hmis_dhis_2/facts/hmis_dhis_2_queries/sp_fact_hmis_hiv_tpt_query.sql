DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_tpt_query;

CREATE PROCEDURE  sp_fact_hmis_hiv_tpt_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
WITH FollowUp AS (select follow_up.encounter_id,
                         follow_up.client_id,
                         date_started_on_tuberculosis_prophy tpt_start_date,
                         date_completed_tuberculosis_prophyl tpt_completed_date,
                         follow_up_date_followup_      as    follow_up_date,
                         tb_prophylaxis_type,
                         art_antiretroviral_start_date as    art_start_date,
                         follow_up_status,
                         treatment_end_date
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
     -- TX curr
     tx_curr_all AS (SELECT client_id,
                            follow_up_date                                                                             AS FollowupDate,
                            encounter_id,
                            follow_up_status,
                            treatment_end_date,
                            tpt_start_date,
                            tpt_completed_date,
                            tb_prophylaxis_type,
                            art_start_date,
                            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                     FROM FollowUp
                     WHERE follow_up_status IS NOT NULL
                       AND art_start_date IS NOT NULL
                       AND follow_up_date <= REPORT_END_DATE),

     tx_curr AS (select *
                 from tx_curr_all
                 where row_num = 1
                   AND follow_up_status in ('Alive', 'Restart medication')
                   AND treatment_end_date >= REPORT_END_DATE),
     art_tpt as (select tx_curr.encounter_id,
                        tx_curr.client_id,
                        tx_curr.FollowupDate,
                        tx_curr.tpt_start_date,
                        tx_curr.tpt_completed_date,
                        client.sex,
                        tx_curr.tb_prophylaxis_type,
                        date_of_birth
                 from tx_curr
                          left join mamba_dim_client client on tx_curr.client_id = client.client_id
                 where row_num = 1
                   and art_start_date is not null
                   and tpt_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
     art_tpt_cr as (select tx_curr.encounter_id,
                           tx_curr.client_id,
                           tx_curr.FollowupDate,
                           tpt_start_date,
                           tpt_completed_date,
                           client.sex,
                           tb_prophylaxis_type,
                           date_of_birth,
                           mrn,
                           uan
                    from tx_curr
                             left join mamba_dim_client client on tx_curr.client_id = client.client_id
                    where row_num = 1
                      and art_start_date is not null
                      and  tpt_start_date BETWEEN fn_ethiopian_to_gregorian_calendar(date_add(fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL -12 MONTH)) AND
                        fn_ethiopian_to_gregorian_calendar(date_add(fn_gregorian_to_ethiopian_calendar(REPORT_END_DATE, 'Y-M-D'), INTERVAL -12 MONTH))
     ),
     tpt_agg AS (
         SELECT
             COUNT(*) AS total,
             SUM(CASE WHEN tb_prophylaxis_type = '6H'                                              THEN 1 ELSE 0 END) AS _6h_total,
             SUM(CASE WHEN tb_prophylaxis_type = '6H' AND age < 15  AND sex = 'Male'               THEN 1 ELSE 0 END) AS _6h_u15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '6H' AND age < 15  AND sex = 'Female'             THEN 1 ELSE 0 END) AS _6h_u15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '6H' AND age >= 15 AND sex = 'Male'               THEN 1 ELSE 0 END) AS _6h_o15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '6H' AND age >= 15 AND sex = 'Female'             THEN 1 ELSE 0 END) AS _6h_o15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP'                                             THEN 1 ELSE 0 END) AS _3hp_total,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP' AND age < 15  AND sex = 'Male'              THEN 1 ELSE 0 END) AS _3hp_u15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP' AND age < 15  AND sex = 'Female'            THEN 1 ELSE 0 END) AS _3hp_u15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP' AND age >= 15 AND sex = 'Male'              THEN 1 ELSE 0 END) AS _3hp_o15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP' AND age >= 15 AND sex = 'Female'            THEN 1 ELSE 0 END) AS _3hp_o15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR'                                             THEN 1 ELSE 0 END) AS _3hr_total,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR' AND age < 15  AND sex = 'Male'              THEN 1 ELSE 0 END) AS _3hr_u15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR' AND age < 15  AND sex = 'Female'            THEN 1 ELSE 0 END) AS _3hr_u15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR' AND age >= 15 AND sex = 'Male'              THEN 1 ELSE 0 END) AS _3hr_o15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR' AND age >= 15 AND sex = 'Female'            THEN 1 ELSE 0 END) AS _3hr_o15_female
         FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age FROM art_tpt) t
     ),
     tpt_cr_agg AS (
         SELECT
             -- CR.1: started (no completion filter)
             COUNT(*) AS cr_total,
             SUM(CASE WHEN tb_prophylaxis_type = '6H'                                                                       THEN 1 ELSE 0 END) AS cr_6h_total,
             SUM(CASE WHEN tb_prophylaxis_type = '6H'  AND age < 15  AND sex = 'Male'                                       THEN 1 ELSE 0 END) AS cr_6h_u15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '6H'  AND age < 15  AND sex = 'Female'                                     THEN 1 ELSE 0 END) AS cr_6h_u15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '6H'  AND age >= 15 AND sex = 'Male'                                       THEN 1 ELSE 0 END) AS cr_6h_o15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '6H'  AND age >= 15 AND sex = 'Female'                                     THEN 1 ELSE 0 END) AS cr_6h_o15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP'                                                                      THEN 1 ELSE 0 END) AS cr_3hp_total,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP' AND age < 15  AND sex = 'Male'                                       THEN 1 ELSE 0 END) AS cr_3hp_u15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP' AND age < 15  AND sex = 'Female'                                     THEN 1 ELSE 0 END) AS cr_3hp_u15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP' AND age >= 15 AND sex = 'Male'                                       THEN 1 ELSE 0 END) AS cr_3hp_o15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP' AND age >= 15 AND sex = 'Female'                                     THEN 1 ELSE 0 END) AS cr_3hp_o15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR'                                                                      THEN 1 ELSE 0 END) AS cr_3hr_total,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR' AND age < 15  AND sex = 'Male'                                       THEN 1 ELSE 0 END) AS cr_3hr_u15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR' AND age < 15  AND sex = 'Female'                                     THEN 1 ELSE 0 END) AS cr_3hr_u15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR' AND age >= 15 AND sex = 'Male'                                       THEN 1 ELSE 0 END) AS cr_3hr_o15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR' AND age >= 15 AND sex = 'Female'                                     THEN 1 ELSE 0 END) AS cr_3hr_o15_female,
             -- CR.2: completed
             SUM(CASE WHEN tpt_completed_date IS NOT NULL                                                                   THEN 1 ELSE 0 END) AS cr2_total,
             SUM(CASE WHEN tb_prophylaxis_type = '6H'  AND tpt_completed_date IS NOT NULL                                   THEN 1 ELSE 0 END) AS cr2_6h_total,
             SUM(CASE WHEN tb_prophylaxis_type = '6H'  AND age < 15  AND sex = 'Male'   AND tpt_completed_date IS NOT NULL  THEN 1 ELSE 0 END) AS cr2_6h_u15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '6H'  AND age < 15  AND sex = 'Female' AND tpt_completed_date IS NOT NULL  THEN 1 ELSE 0 END) AS cr2_6h_u15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '6H'  AND age >= 15 AND sex = 'Male'   AND tpt_completed_date IS NOT NULL  THEN 1 ELSE 0 END) AS cr2_6h_o15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '6H'  AND age >= 15 AND sex = 'Female' AND tpt_completed_date IS NOT NULL  THEN 1 ELSE 0 END) AS cr2_6h_o15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP' AND tpt_completed_date IS NOT NULL                                   THEN 1 ELSE 0 END) AS cr2_3hp_total,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP' AND age < 15  AND sex = 'Male'   AND tpt_completed_date IS NOT NULL  THEN 1 ELSE 0 END) AS cr2_3hp_u15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP' AND age < 15  AND sex = 'Female' AND tpt_completed_date IS NOT NULL  THEN 1 ELSE 0 END) AS cr2_3hp_u15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP' AND age >= 15 AND sex = 'Male'   AND tpt_completed_date IS NOT NULL  THEN 1 ELSE 0 END) AS cr2_3hp_o15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '3HP' AND age >= 15 AND sex = 'Female' AND tpt_completed_date IS NOT NULL  THEN 1 ELSE 0 END) AS cr2_3hp_o15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR' AND tpt_completed_date IS NOT NULL                                   THEN 1 ELSE 0 END) AS cr2_3hr_total,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR' AND age < 15  AND sex = 'Male'   AND tpt_completed_date IS NOT NULL  THEN 1 ELSE 0 END) AS cr2_3hr_u15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR' AND age < 15  AND sex = 'Female' AND tpt_completed_date IS NOT NULL  THEN 1 ELSE 0 END) AS cr2_3hr_u15_female,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR' AND age >= 15 AND sex = 'Male'   AND tpt_completed_date IS NOT NULL  THEN 1 ELSE 0 END) AS cr2_3hr_o15_male,
             SUM(CASE WHEN tb_prophylaxis_type = '3HR' AND age >= 15 AND sex = 'Female' AND tpt_completed_date IS NOT NULL  THEN 1 ELSE 0 END) AS cr2_3hr_o15_female
         FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age FROM art_tpt_cr) t
     )

SELECT 'HIV_ART_TPT.1' AS S_NO, 'Number of ART patients who started on a standard course of TB Preventive Treatment (TPT) in the reporting period' AS Activity, total AS Value FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_6H',      'Patients on 6H',         _6h_total        FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_6H. 1',   '< 15 years, Male',       _6h_u15_male     FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_6H. 2',   '< 15 years, Female',     _6h_u15_female   FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_6H. 3',   '>= 15 years, Male',      _6h_o15_male     FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_6H. 4',   '>= 15 years, Female',    _6h_o15_female   FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_3HP',     'Patients on 3HP',        _3hp_total       FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_3HP. 1',  '< 15 years, Male',       _3hp_u15_male    FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_3HP. 2',  '< 15 years, Female',     _3hp_u15_female  FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_3HP. 3',  '>= 15 years, Male',      _3hp_o15_male    FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_3HP. 4',  '>= 15 years, Female',    _3hp_o15_female  FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_3HR',     'Patients on 3HR',        _3hr_total       FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_3HR. 1',  '< 15 years, Male',       _3hr_u15_male    FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_3HR. 2',  '< 15 years, Female',     _3hr_u15_female  FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_3HR. 3',  '>= 15 years, Male',      _3hr_o15_male    FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_3HR. 4',  '>= 15 years, Female',    _3hr_o15_female  FROM tpt_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1',    'Number of ART patients who were initiated on any course of TPT 12 months before the reporting period', cr_total          FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.1',  'Patients on 6H 12 months prior to the reporting period',  cr_6h_total      FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.1. 1', '< 15 years, Male',    cr_6h_u15_male   FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.1. 2', '< 15 years, Female',  cr_6h_u15_female FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.1. 3', '>= 15 years, Male',   cr_6h_o15_male   FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.1. 4', '>= 15 years, Female', cr_6h_o15_female FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.2',  'Patients on 3HP 12 months prior to the reporting period', cr_3hp_total     FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.2. 1', '< 15 years, Male',    cr_3hp_u15_male  FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.2. 2', '< 15 years, Female',  cr_3hp_u15_female FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.2. 3', '>= 15 years, Male',   cr_3hp_o15_male  FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.2. 4', '>= 15 years, Female', cr_3hp_o15_female FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.3',  'Patients on 3HR 12 months prior to the reporting period', cr_3hr_total     FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.3. 1', '< 15 years, Male',    cr_3hr_u15_male  FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.3. 2', '< 15 years, Female',  cr_3hr_u15_female FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.3. 3', '>= 15 years, Male',   cr_3hr_o15_male  FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.1.3. 4', '>= 15 years, Female', cr_3hr_o15_female FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2',    'Number of ART patients who started TPT 12 months prior to the reproting period that completed a full course of therapy', cr2_total FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.1',  'Patients who completed 6H',  cr2_6h_total      FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.1. 1', '< 15 years, Male',    cr2_6h_u15_male   FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.1. 2', '< 15 years, Female',  cr2_6h_u15_female FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.1. 3', '>= 15 years, Male',   cr2_6h_o15_male   FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.1. 4', '>= 15 years, Female', cr2_6h_o15_female FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.2',  'Patients who completed 3HP', cr2_3hp_total     FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.2. 1', '< 15 years, Male',    cr2_3hp_u15_male  FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.2. 2', '< 15 years, Female',  cr2_3hp_u15_female FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.2. 3', '>= 15 years, Male',   cr2_3hp_o15_male  FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.2. 4', '>= 15 years, Female', cr2_3hp_o15_female FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.3',  'Patients who completed 3HR', cr2_3hr_total     FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.3. 1', '< 15 years, Male',    cr2_3hr_u15_male  FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.3. 2', '< 15 years, Female',  cr2_3hr_u15_female FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.3. 3', '>= 15 years, Male',   cr2_3hr_o15_male  FROM tpt_cr_agg
UNION ALL SELECT 'HIV_ART_TPT_CR.2.3. 4', '>= 15 years, Female', cr2_3hr_o15_female FROM tpt_cr_agg;
END //

DELIMITER ;