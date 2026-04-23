DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_dsd_query_v2;

CREATE PROCEDURE sp_fact_hmis_hiv_dsd_query_v2(IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUp AS (SELECT encounter_id,
                             client_id                     AS PatientId,
                             follow_up_status,
                             follow_up_date_followup_      AS follow_up_date,
                             art_antiretroviral_start_date AS art_start_date,
                             treatment_end_date,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child    breast_feeding_status,
                             pregnancy_status,
                             dsd_category,
                             assessment_date
                      FROM tmp_hmis_follow_up),
         -- TX curr
         tx_curr_all AS (SELECT PatientId,
                                follow_up_date                                                                             AS FollowupDate,
                                encounter_id,
                                follow_up_status,
                                treatment_end_date,
                                ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                         FROM FollowUp
                         WHERE follow_up_status IS NOT NULL
                           AND art_start_date IS NOT NULL
                           AND follow_up_date <= REPORT_END_DATE
         ),

         tx_curr AS (select *
                     from tx_curr_all
                     where row_num = 1
                       AND follow_up_status in ('Alive', 'Restart medication')
                       AND treatment_end_date >= REPORT_END_DATE),
         dsd as (select tx_curr.*,
                        client.sex,
                        client.date_of_birth,
                        pregnancy_status,
                        regimen,
                        dsd_category
                 from FollowUp
                          inner join tx_curr on FollowUp.encounter_id = tx_curr.encounter_id
                          left join mamba_dim_client client on tx_curr.PatientId = client.client_id
                 where dsd_category is not null
                   and assessment_date <= REPORT_END_DATE
                 and (
                     dsd_category = '3MMD'
                         or
                     (dsd_category = 'Appointment spacing model / 6MMD'
                     AND TIMESTAMPDIFF(YEAR, client.date_of_birth, REPORT_END_DATE) >= 15)
                     or
                     (dsd_category = 'Fast track antiretroviral refill'
                         AND TIMESTAMPDIFF(YEAR, client.date_of_birth, REPORT_END_DATE) >= 15)
                     or
                     (dsd_category = 'Health extension professional led community'
                         AND TIMESTAMPDIFF(YEAR, client.date_of_birth, REPORT_END_DATE) >= 15)
                     or
                     (dsd_category = 'Community based group model by peer'
                         AND TIMESTAMPDIFF(YEAR, client.date_of_birth, REPORT_END_DATE) >= 15)
                     or
                     (dsd_category = 'DSD for adolescent')
                    or
                     (dsd_category = 'DSD for key populations')
                         or
                    (dsd_category = 'DSD for maternal child health')
                    or
                     (dsd_category = 'Other')
                    or
                     (dsd_category = 'Advanced HIV disease model')
                     )
         ),
         dsd_percentage AS (SELECT 'HIV_TX_DSD'                                                                   AS S_NO,
                                   'Proportion of PLHIV currently on differentiated service Delivery model (DSD)' AS Activity,
                                   CASE
                                       WHEN (SELECT COUNT(*) FROM tx_curr) = 0 THEN
                                           '0%'
                                       ELSE
                                           CONCAT(CAST(ROUND(
                                                   (CAST((SELECT COUNT(*) FROM dsd) AS REAL) * 100.0) /
                                                   (SELECT COUNT(*) FROM tx_curr)
                                               , 2) AS CHAR), '%')
                                       END                                                                        AS Value
                            FROM (SELECT 1) AS dummy -- Added a dummy table to ensure the CTE returns at least one row
         ),
         dsd_agg AS (
             SELECT
                 SUM(CASE WHEN dsd_category = '3MMD'                                                                                                                    THEN 1 ELSE 0 END) AS _3mmd_total,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age < 1  AND sex = 'Male'                                                                                      THEN 1 ELSE 0 END) AS _3mmd_u1_m,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age < 1  AND sex = 'Female'                                                                                    THEN 1 ELSE 0 END) AS _3mmd_u1_f,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age BETWEEN 1  AND 4  AND sex = 'Male'                                                                         THEN 1 ELSE 0 END) AS _3mmd_u4_m,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age BETWEEN 1  AND 4  AND sex = 'Female'                                                                       THEN 1 ELSE 0 END) AS _3mmd_u4_f,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age BETWEEN 5  AND 9  AND sex = 'Male'                                                                         THEN 1 ELSE 0 END) AS _3mmd_u9_m,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age BETWEEN 5  AND 9  AND sex = 'Female'                                                                       THEN 1 ELSE 0 END) AS _3mmd_u9_f,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age BETWEEN 10 AND 14 AND sex = 'Male'                                                                         THEN 1 ELSE 0 END) AS _3mmd_u14_m,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age BETWEEN 10 AND 14 AND sex = 'Female'                                                                       THEN 1 ELSE 0 END) AS _3mmd_u14_f,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age BETWEEN 15 AND 19 AND sex = 'Male'                                                                         THEN 1 ELSE 0 END) AS _3mmd_u19_m,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age BETWEEN 15 AND 19 AND sex = 'Female'                                                                       THEN 1 ELSE 0 END) AS _3mmd_u19_f,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age BETWEEN 20 AND 24 AND sex = 'Male'                                                                         THEN 1 ELSE 0 END) AS _3mmd_u24_m,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age BETWEEN 20 AND 24 AND sex = 'Female'                                                                       THEN 1 ELSE 0 END) AS _3mmd_u24_f,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age BETWEEN 25 AND 49 AND sex = 'Male'                                                                         THEN 1 ELSE 0 END) AS _3mmd_u49_m,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age BETWEEN 25 AND 49 AND sex = 'Female'                                                                       THEN 1 ELSE 0 END) AS _3mmd_u49_f,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age >= 50 AND sex = 'Male'                                                                                     THEN 1 ELSE 0 END) AS _3mmd_o50_m,
                 SUM(CASE WHEN dsd_category = '3MMD' AND age >= 50 AND sex = 'Female'                                                                                   THEN 1 ELSE 0 END) AS _3mmd_o50_f,
                 SUM(CASE WHEN dsd_category = 'Appointment spacing model / 6MMD' AND age >= 15                                                                          THEN 1 ELSE 0 END) AS _asm_total,
                 SUM(CASE WHEN dsd_category = 'Appointment spacing model / 6MMD' AND age BETWEEN 15 AND 19 AND sex = 'Male'                                             THEN 1 ELSE 0 END) AS _asm_u19_m,
                 SUM(CASE WHEN dsd_category = 'Appointment spacing model / 6MMD' AND age BETWEEN 15 AND 19 AND sex = 'Female'                                           THEN 1 ELSE 0 END) AS _asm_u19_f,
                 SUM(CASE WHEN dsd_category = 'Appointment spacing model / 6MMD' AND age BETWEEN 20 AND 24 AND sex = 'Male'                                             THEN 1 ELSE 0 END) AS _asm_u24_m,
                 SUM(CASE WHEN dsd_category = 'Appointment spacing model / 6MMD' AND age BETWEEN 20 AND 24 AND sex = 'Female'                                           THEN 1 ELSE 0 END) AS _asm_u24_f,
                 SUM(CASE WHEN dsd_category = 'Appointment spacing model / 6MMD' AND age BETWEEN 25 AND 49 AND sex = 'Male'                                             THEN 1 ELSE 0 END) AS _asm_u49_m,
                 SUM(CASE WHEN dsd_category = 'Appointment spacing model / 6MMD' AND age BETWEEN 25 AND 49 AND sex = 'Female'                                           THEN 1 ELSE 0 END) AS _asm_u49_f,
                 SUM(CASE WHEN dsd_category = 'Appointment spacing model / 6MMD' AND age >= 50 AND sex = 'Male'                                                         THEN 1 ELSE 0 END) AS _asm_o50_m,
                 SUM(CASE WHEN dsd_category = 'Appointment spacing model / 6MMD' AND age >= 50 AND sex = 'Female'                                                       THEN 1 ELSE 0 END) AS _asm_o50_f,
                 SUM(CASE WHEN dsd_category = 'Fast track antiretroviral refill' AND age >= 15                                                                          THEN 1 ELSE 0 END) AS _ftar_total,
                 SUM(CASE WHEN dsd_category = 'Fast track antiretroviral refill' AND age BETWEEN 15 AND 19 AND sex = 'Male'                                             THEN 1 ELSE 0 END) AS _ftar_u19_m,
                 SUM(CASE WHEN dsd_category = 'Fast track antiretroviral refill' AND age BETWEEN 15 AND 19 AND sex = 'Female'                                           THEN 1 ELSE 0 END) AS _ftar_u19_f,
                 SUM(CASE WHEN dsd_category = 'Fast track antiretroviral refill' AND age BETWEEN 20 AND 24 AND sex = 'Male'                                             THEN 1 ELSE 0 END) AS _ftar_u24_m,
                 SUM(CASE WHEN dsd_category = 'Fast track antiretroviral refill' AND age BETWEEN 20 AND 24 AND sex = 'Female'                                           THEN 1 ELSE 0 END) AS _ftar_u24_f,
                 SUM(CASE WHEN dsd_category = 'Fast track antiretroviral refill' AND age BETWEEN 25 AND 49 AND sex = 'Male'                                             THEN 1 ELSE 0 END) AS _ftar_u49_m,
                 SUM(CASE WHEN dsd_category = 'Fast track antiretroviral refill' AND age BETWEEN 25 AND 49 AND sex = 'Female'                                           THEN 1 ELSE 0 END) AS _ftar_u49_f,
                 SUM(CASE WHEN dsd_category = 'Fast track antiretroviral refill' AND age >= 50 AND sex = 'Male'                                                         THEN 1 ELSE 0 END) AS _ftar_o50_m,
                 SUM(CASE WHEN dsd_category = 'Fast track antiretroviral refill' AND age >= 50 AND sex = 'Female'                                                       THEN 1 ELSE 0 END) AS _ftar_o50_f,
                 SUM(CASE WHEN dsd_category = 'Health extension professional led community' AND age >= 15                                                               THEN 1 ELSE 0 END) AS _cag_total,
                 SUM(CASE WHEN dsd_category = 'Health extension professional led community' AND age BETWEEN 15 AND 19 AND sex = 'Male'                                  THEN 1 ELSE 0 END) AS _cag_u19_m,
                 SUM(CASE WHEN dsd_category = 'Health extension professional led community' AND age BETWEEN 15 AND 19 AND sex = 'Female'                                THEN 1 ELSE 0 END) AS _cag_u19_f,
                 SUM(CASE WHEN dsd_category = 'Health extension professional led community' AND age BETWEEN 20 AND 24 AND sex = 'Male'                                  THEN 1 ELSE 0 END) AS _cag_u24_m,
                 SUM(CASE WHEN dsd_category = 'Health extension professional led community' AND age BETWEEN 20 AND 24 AND sex = 'Female'                                THEN 1 ELSE 0 END) AS _cag_u24_f,
                 SUM(CASE WHEN dsd_category = 'Health extension professional led community' AND age BETWEEN 25 AND 49 AND sex = 'Male'                                  THEN 1 ELSE 0 END) AS _cag_u49_m,
                 SUM(CASE WHEN dsd_category = 'Health extension professional led community' AND age BETWEEN 25 AND 49 AND sex = 'Female'                                THEN 1 ELSE 0 END) AS _cag_u49_f,
                 SUM(CASE WHEN dsd_category = 'Health extension professional led community' AND age >= 50 AND sex = 'Male'                                              THEN 1 ELSE 0 END) AS _cag_o50_m,
                 SUM(CASE WHEN dsd_category = 'Health extension professional led community' AND age >= 50 AND sex = 'Female'                                            THEN 1 ELSE 0 END) AS _cag_o50_f,
                 SUM(CASE WHEN dsd_category = 'Community based group model by peer' AND age >= 15                                                                       THEN 1 ELSE 0 END) AS _pcad_total,
                 SUM(CASE WHEN dsd_category = 'Community based group model by peer' AND age BETWEEN 15 AND 19 AND sex = 'Male'                                          THEN 1 ELSE 0 END) AS _pcad_u19_m,
                 SUM(CASE WHEN dsd_category = 'Community based group model by peer' AND age BETWEEN 15 AND 19 AND sex = 'Female'                                        THEN 1 ELSE 0 END) AS _pcad_u19_f,
                 SUM(CASE WHEN dsd_category = 'Community based group model by peer' AND age BETWEEN 20 AND 24 AND sex = 'Male'                                          THEN 1 ELSE 0 END) AS _pcad_u24_m,
                 SUM(CASE WHEN dsd_category = 'Community based group model by peer' AND age BETWEEN 20 AND 24 AND sex = 'Female'                                        THEN 1 ELSE 0 END) AS _pcad_u24_f,
                 SUM(CASE WHEN dsd_category = 'Community based group model by peer' AND age BETWEEN 25 AND 49 AND sex = 'Male'                                          THEN 1 ELSE 0 END) AS _pcad_u49_m,
                 SUM(CASE WHEN dsd_category = 'Community based group model by peer' AND age BETWEEN 25 AND 49 AND sex = 'Female'                                        THEN 1 ELSE 0 END) AS _pcad_u49_f,
                 SUM(CASE WHEN dsd_category = 'Community based group model by peer' AND age >= 50 AND sex = 'Male'                                                      THEN 1 ELSE 0 END) AS _pcad_o50_m,
                 SUM(CASE WHEN dsd_category = 'Community based group model by peer' AND age >= 50 AND sex = 'Female'                                                    THEN 1 ELSE 0 END) AS _pcad_o50_f,
                 SUM(CASE WHEN dsd_category = 'DSD for adolescent'                                                                                                      THEN 1 ELSE 0 END) AS adol_total,
                 SUM(CASE WHEN dsd_category = 'DSD for key populations'                                                                                                 THEN 1 ELSE 0 END) AS kp_total,
                 SUM(CASE WHEN dsd_category = 'DSD for maternal child health'                                                                                           THEN 1 ELSE 0 END) AS mch_total,
                 SUM(CASE WHEN dsd_category = 'Other'                                                                                                                   THEN 1 ELSE 0 END) AS other_total,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model'                                                                                              THEN 1 ELSE 0 END) AS _ahdcm_total,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age < 1  AND sex = 'Male'                                                                THEN 1 ELSE 0 END) AS _ahdcm_u1_m,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age < 1  AND sex = 'Female'                                                              THEN 1 ELSE 0 END) AS _ahdcm_u1_f,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age BETWEEN 1  AND 4  AND sex = 'Male'                                                   THEN 1 ELSE 0 END) AS _ahdcm_u4_m,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age BETWEEN 1  AND 4  AND sex = 'Female'                                                 THEN 1 ELSE 0 END) AS _ahdcm_u4_f,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age BETWEEN 5  AND 9  AND sex = 'Male'                                                   THEN 1 ELSE 0 END) AS _ahdcm_u9_m,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age BETWEEN 5  AND 9  AND sex = 'Female'                                                 THEN 1 ELSE 0 END) AS _ahdcm_u9_f,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age BETWEEN 10 AND 14 AND sex = 'Male'                                                   THEN 1 ELSE 0 END) AS _ahdcm_u14_m,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age BETWEEN 10 AND 14 AND sex = 'Female'                                                 THEN 1 ELSE 0 END) AS _ahdcm_u14_f,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age BETWEEN 15 AND 19 AND sex = 'Male'                                                   THEN 1 ELSE 0 END) AS _ahdcm_u19_m,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age BETWEEN 15 AND 19 AND sex = 'Female'                                                 THEN 1 ELSE 0 END) AS _ahdcm_u19_f,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age BETWEEN 20 AND 24 AND sex = 'Male'                                                   THEN 1 ELSE 0 END) AS _ahdcm_u24_m,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age BETWEEN 20 AND 24 AND sex = 'Female'                                                 THEN 1 ELSE 0 END) AS _ahdcm_u24_f,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age BETWEEN 25 AND 49 AND sex = 'Male'                                                   THEN 1 ELSE 0 END) AS _ahdcm_u49_m,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age BETWEEN 25 AND 49 AND sex = 'Female'                                                 THEN 1 ELSE 0 END) AS _ahdcm_u49_f,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age >= 50 AND sex = 'Male'                                                               THEN 1 ELSE 0 END) AS _ahdcm_o50_m,
                 SUM(CASE WHEN dsd_category = 'Advanced HIV disease model' AND age >= 50 AND sex = 'Female'                                                             THEN 1 ELSE 0 END) AS _ahdcm_o50_f
             FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age FROM dsd) t
         )
    SELECT S_NO, Activity, Value FROM dsd_percentage
    UNION ALL SELECT 'HIV_TX_3MMD',        'Total number of clients on 3MMD', COALESCE(_3mmd_total,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 1',  '< 1 year, Male',           COALESCE(_3mmd_u1_m,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 2',  '< 1 year, Female',          COALESCE(_3mmd_u1_f,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 3',  '1 - 4 years, Male',         COALESCE(_3mmd_u4_m,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 4',  '1 - 4 years, Female',       COALESCE(_3mmd_u4_f,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 5',  '5 - 9 years, Male',         COALESCE(_3mmd_u9_m,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 6',  '5 - 9 years, Female',       COALESCE(_3mmd_u9_f,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 7',  '10 - 14 years, Male',       COALESCE(_3mmd_u14_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 8',  '10 - 14 years, Female',     COALESCE(_3mmd_u14_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 9',  '15 - 19 years, Male',       COALESCE(_3mmd_u19_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 10', '15 - 19 years, Female',     COALESCE(_3mmd_u19_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 11', '20 - 24 years, Male',       COALESCE(_3mmd_u24_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 12', '20 - 24 years, Female',     COALESCE(_3mmd_u24_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 13', '25 - 49 years, Male',       COALESCE(_3mmd_u49_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 14', '25 - 49 years, Female',     COALESCE(_3mmd_u49_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 15', '>= 50 years, Male',         COALESCE(_3mmd_o50_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_3MMD. 16', '>= 50 years, Female',       COALESCE(_3mmd_o50_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_ASM',      'Total number of clients on ASM(6MMD)', COALESCE(_asm_total,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_ASM. 1',   '15 - 19 years, Male',       COALESCE(_asm_u19_m,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_ASM. 2',   '15 - 19 years, Female',     COALESCE(_asm_u19_f,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_ASM. 3',   '20 - 24 years, Male',       COALESCE(_asm_u24_m,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_ASM. 4',   '20 - 24 years, Female',     COALESCE(_asm_u24_f,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_ASM. 5',   '25 - 49 years, Male',       COALESCE(_asm_u49_m,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_ASM. 6',   '25 - 49 years, Female',     COALESCE(_asm_u49_f,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_ASM. 7',   '>= 50 years, Male',         COALESCE(_asm_o50_m,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_ASM. 8',   '>= 50 years, Female',       COALESCE(_asm_o50_f,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_FTAR',     'Total number of clients on FTAR',       COALESCE(_ftar_total,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_FTAR. 1',  '15 - 19 years, Male',       COALESCE(_ftar_u19_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_FTAR. 2',  '15 - 19 years, Female',     COALESCE(_ftar_u19_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_FTAR. 3',  '20 - 24 years, Male',       COALESCE(_ftar_u24_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_FTAR. 4',  '20 - 24 years, Female',     COALESCE(_ftar_u24_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_FTAR. 5',  '25 - 49 years, Male',       COALESCE(_ftar_u49_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_FTAR. 6',  '25 - 49 years, Female',     COALESCE(_ftar_u49_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_FTAR. 7',  '>= 50 years, Male',         COALESCE(_ftar_o50_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_FTAR. 8',  '>= 50 years, Female',       COALESCE(_ftar_o50_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_CAG',      'Total number of clients on CAG',        COALESCE(_cag_total,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_CAG. 1',   '15 - 19 years, Male',       COALESCE(_cag_u19_m,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_CAG. 2',   '15 - 19 years, Female',     COALESCE(_cag_u19_f,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_CAG. 3',   '20 - 24 years, Male',       COALESCE(_cag_u24_m,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_CAG. 4',   '20 - 24 years, Female',     COALESCE(_cag_u24_f,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_CAG. 5',   '25 - 49 years, Male',       COALESCE(_cag_u49_m,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_CAG. 6',   '25 - 49 years, Female',     COALESCE(_cag_u49_f,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_CAG. 7',   '>= 50 years, Male',         COALESCE(_cag_o50_m,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_CAG. 8',   '>= 50 years, Female',       COALESCE(_cag_o50_f,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_PCAD',     'Total number of clients on PCAD',      COALESCE(_pcad_total,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_PCAD. 1',  '15 - 19 years, Male',       COALESCE(_pcad_u19_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_PCAD. 2',  '15 - 19 years, Female',     COALESCE(_pcad_u19_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_PCAD. 3',  '20 - 24 years, Male',       COALESCE(_pcad_u24_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_PCAD. 4',  '20 - 24 years, Female',     COALESCE(_pcad_u24_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_PCAD. 5',  '25 - 49 years, Male',       COALESCE(_pcad_u49_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_PCAD. 6',  '25 - 49 years, Female',     COALESCE(_pcad_u49_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_PCAD. 7',  '>= 50 years, Male',         COALESCE(_pcad_o50_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_PCAD. 8',  '>= 50 years, Female',       COALESCE(_pcad_o50_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_Adolescent DSD.', 'Total number of clients on Adolescent DSD', COALESCE(adol_total,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_KP DSD.',  'Total number of clients on KP DSD',     COALESCE(kp_total,0)    FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_MCH DSD.', 'Total number of clients on MCH DSD',    COALESCE(mch_total,0)   FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_ other types of DSD.', 'Total number of clients on other types of DSD', COALESCE(other_total,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM',    'Total number of clients on "Advanced HIV Disease Care Model"', COALESCE(_ahdcm_total,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 1',  '< 1 year, Male',           COALESCE(_ahdcm_u1_m,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 2',  '< 1 year, Female',         COALESCE(_ahdcm_u1_f,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 3',  '1 - 4 years, Male',        COALESCE(_ahdcm_u4_m,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 4',  '1 - 4 years, Female',      COALESCE(_ahdcm_u4_f,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 5',  '5 - 9 years, Male',        COALESCE(_ahdcm_u9_m,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 6',  '5 - 9 years, Female',      COALESCE(_ahdcm_u9_f,0)  FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 7',  '10 - 14 years, Male',      COALESCE(_ahdcm_u14_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 8',  '10 - 14 years, Female',    COALESCE(_ahdcm_u14_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 9',  '15 - 19 years, Male',      COALESCE(_ahdcm_u19_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 10', '15 - 19 years, Female',    COALESCE(_ahdcm_u19_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 11', '20 - 24 years, Male',      COALESCE(_ahdcm_u24_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 12', '20 - 24 years, Female',    COALESCE(_ahdcm_u24_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 13', '25 - 49 years, Male',      COALESCE(_ahdcm_u49_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 14', '25 - 49 years, Female',    COALESCE(_ahdcm_u49_f,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 15', '>= 50 years, Male',        COALESCE(_ahdcm_o50_m,0) FROM dsd_agg
    UNION ALL SELECT 'HIV_TX_AHDCM. 16', '>= 50 years, Female',      COALESCE(_ahdcm_o50_f,0) FROM dsd_agg;
END //

DELIMITER ;