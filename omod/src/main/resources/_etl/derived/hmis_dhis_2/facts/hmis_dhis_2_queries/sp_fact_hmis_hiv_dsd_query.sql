DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_dsd_query;

CREATE PROCEDURE sp_fact_hmis_hiv_dsd_query(IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id           AS PatientId,
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
         )
    SELECT S_NO,
           Activity,
           Value
    FROM dsd_percentage
-- Total number of clients on 3MMD
    UNION ALl
    SELECT 'HIV_TX_3MMD'                     AS S_NO,
           'Total number of clients on 3MMD' as Activity,
           COUNT(*)                          as Value
    FROM dsd
    where dsd_category = '3MMD'
-- 1 Less than  1 year, Male
    UNION ALL
    SELECT 'HIV_TX_3MMD. 1'            AS S_NO,
           '< 1 year, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND dsd_category = '3MMD'
      AND sex = 'Male'
-- 2 Less than  1 year, Female
    UNION ALL
    SELECT 'HIV_TX_3MMD. 2'              AS S_NO,
           '< 1 year, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND dsd_category = '3MMD'
      AND sex = 'Female'
-- 3 1 - 4 years, Male
    UNION ALL
    SELECT 'HIV_TX_3MMD. 3'      AS S_NO,
           '1 - 4 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND dsd_category = '3MMD'
      AND sex = 'Male'
-- 4 1 - 4 years, Female
    UNION ALL
    SELECT 'HIV_TX_3MMD. 4'        AS S_NO,
           '1 - 4 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND dsd_category = '3MMD'
      AND sex = 'Female'
-- 5 5 - 9 years, Male
    UNION ALL
    SELECT 'HIV_TX_3MMD. 5'      AS S_NO,
           '5 - 9 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND dsd_category = '3MMD'
      AND sex = 'Male'
-- 6 5 - 9 years, Female
    UNION ALL
    SELECT 'HIV_TX_3MMD. 6'        AS S_NO,
           '5 - 9 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND dsd_category = '3MMD'
      AND sex = 'Female'
-- 7 10 - 14 years, Male
    UNION ALL
    SELECT 'HIV_TX_3MMD. 7'        AS S_NO,
           '10 - 14 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND dsd_category = '3MMD'
      AND sex = 'Male'
-- 8 10 - 14 years, Female
    UNION ALL
    SELECT 'HIV_TX_3MMD. 8'          AS S_NO,
           '10 - 14 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND dsd_category = '3MMD'
      AND sex = 'Female'
-- 9 15 - 19 years, Male
    UNION ALL
    SELECT 'HIV_TX_3MMD. 9'        AS S_NO,
           '15 - 19 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND dsd_category = '3MMD'
      AND sex = 'Male'
-- 10 15 - 19 years, Female
    UNION ALL
    SELECT 'HIV_TX_3MMD. 10'         AS S_NO,
           '15 - 19 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND dsd_category = '3MMD'
      AND sex = 'Female'
-- 11 20 - 24 years, Male
    UNION ALL
    SELECT 'HIV_TX_3MMD. 11'       AS S_NO,
           '20 - 24 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND dsd_category = '3MMD'
      AND sex = 'Male'
-- 12 20 - 24 years, Female
    UNION ALL
    SELECT 'HIV_TX_3MMD. 12'         AS S_NO,
           '20 - 24 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND dsd_category = '3MMD'
      AND sex = 'Female'
-- 13 25 - 49 years, Male
    UNION ALL
    SELECT 'HIV_TX_3MMD. 13'       AS S_NO,
           '25 - 49 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 49
      AND dsd_category = '3MMD'
      AND sex = 'Male'
-- 14 25 - 49 years, Female
    UNION ALL
    SELECT 'HIV_TX_3MMD. 14'         AS S_NO,
           '25 - 49 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 49
      AND dsd_category = '3MMD'
      AND sex = 'Female'
-- 15 Greater than or equal to  50 years, Male
    UNION ALL
    SELECT 'HIV_TX_3MMD. 15'                            AS S_NO,
           '>= 50 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND dsd_category = '3MMD'
      AND sex = 'Male'
-- 16 Greater than or equal to  50 years, Female
    UNION ALL
    SELECT 'HIV_TX_3MMD. 16'                              AS S_NO,
           '>= 50 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND dsd_category = '3MMD'
      AND sex = 'Female'
-- Total number of clients on ASM(6MMD)
    UNION ALl
    SELECT 'HIV_TX_ASM'                           AS S_NO,
           'Total number of clients on ASM(6MMD)' as Activity,
           COUNT(*)                               as Value
    FROM dsd
    where dsd_category = 'Appointment spacing model / 6MMD'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
-- 1 15 - 19 years, Male
    UNION ALL
    SELECT 'HIV_TX_ASM. 1'         AS S_NO,
           '15 - 19 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND dsd_category = 'Appointment spacing model / 6MMD'
      AND sex = 'Male'
-- 2 15 - 19 years, Female
    UNION ALL
    SELECT 'HIV_TX_ASM. 2'           AS S_NO,
           '15 - 19 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND dsd_category = 'Appointment spacing model / 6MMD'
      AND sex = 'Female'
-- 3 20 - 24 years, Male
    UNION ALL
    SELECT 'HIV_TX_ASM. 3'         AS S_NO,
           '20 - 24 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND dsd_category = 'Appointment spacing model / 6MMD'
      AND sex = 'Male'
-- 4 20 - 24 years, Female
    UNION ALL
    SELECT 'HIV_TX_ASM. 4'           AS S_NO,
           '20 - 24 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND dsd_category = 'Appointment spacing model / 6MMD'
      AND sex = 'Female'
-- 5 25 - 49 years, Male
    UNION ALL
    SELECT 'HIV_TX_ASM. 5'         AS S_NO,
           '25 - 49 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 49
      AND dsd_category = 'Appointment spacing model / 6MMD'
      AND sex = 'Male'
-- 6 25 - 49 years, Female
    UNION ALL
    SELECT 'HIV_TX_ASM. 6'           AS S_NO,
           '25 - 49 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 49
      AND dsd_category = 'Appointment spacing model / 6MMD'
      AND sex = 'Female'
-- 7 Greater than or equal to  50 years, Male
    UNION ALL
    SELECT 'HIV_TX_ASM. 7'                              AS S_NO,
           '>= 50 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND dsd_category = 'Appointment spacing model / 6MMD'
      AND sex = 'Male'
-- 8 Greater than or equal to  50 years, Female
    UNION ALL
    SELECT 'HIV_TX_ASM. 8'                                AS S_NO,
           '>= 50 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND dsd_category = 'Appointment spacing model / 6MMD'
      AND sex = 'Female'
-- Total number of clients on FTAR
    UNION ALl
    SELECT 'HIV_TX_FTAR'                     AS S_NO,
           'Total number of clients on FTAR' as Activity,
           COUNT(*)                          as Value
    FROM dsd
    where dsd_category = 'Fast track antiretroviral refill'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
-- 1 15 - 19 years, Male
    UNION ALL
    SELECT 'HIV_TX_FTAR. 1'        AS S_NO,
           '15 - 19 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND dsd_category = 'Fast track antiretroviral refill'
      AND sex = 'Male'
-- 2 15 - 19 years, Female
    UNION ALL
    SELECT 'HIV_TX_FTAR. 2'          AS S_NO,
           '15 - 19 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND dsd_category = 'Fast track antiretroviral refill'
      AND sex = 'Female'
-- 3 20 - 24 years, Male
    UNION ALL
    SELECT 'HIV_TX_FTAR. 3'        AS S_NO,
           '20 - 24 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND dsd_category = 'Fast track antiretroviral refill'
      AND sex = 'Male'
-- 4 20 - 24 years, Female
    UNION ALL
    SELECT 'HIV_TX_FTAR. 4'          AS S_NO,
           '20 - 24 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND dsd_category = 'Fast track antiretroviral refill'
      AND sex = 'Female'
-- 5 25 - 49 years, Male
    UNION ALL
    SELECT 'HIV_TX_FTAR. 5'        AS S_NO,
           '25 - 49 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 49
      AND dsd_category = 'Fast track antiretroviral refill'
      AND sex = 'Male'
-- 6 25 - 49 years, Female
    UNION ALL
    SELECT 'HIV_TX_FTAR. 6'          AS S_NO,
           '25 - 49 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 49
      AND dsd_category = 'Fast track antiretroviral refill'
      AND sex = 'Female'
-- 7 Greater than or equal to  50 years, Male
    UNION ALL
    SELECT 'HIV_TX_FTAR. 7'                             AS S_NO,
           '>= 50 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND dsd_category = 'Fast track antiretroviral refill'
      AND sex = 'Male'
-- 8 Greater than or equal to  50 years, Female
    UNION ALL
    SELECT 'HIV_TX_FTAR. 8'                               AS S_NO,
           '>= 50 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND dsd_category = 'Fast track antiretroviral refill'
      AND sex = 'Female'
-- Total number of clients on CAG
    UNION ALl
    SELECT 'HIV_TX_CAG'                     AS S_NO,
           'Total number of clients on CAG' as Activity,
           COUNT(*)                         as Value
    FROM dsd
    where dsd_category = 'Health extension professional led community'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
-- 1 15 - 19 years, Male
    UNION ALL
    SELECT 'HIV_TX_CAG. 1'         AS S_NO,
           '15 - 19 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND dsd_category = 'Health extension professional led community'
      AND sex = 'Male'
-- 2 15 - 19 years, Female
    UNION ALL
    SELECT 'HIV_TX_CAG. 2'           AS S_NO,
           '15 - 19 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND dsd_category = 'Health extension professional led community'
      AND sex = 'Female'
-- 3 20 - 24 years, Male
    UNION ALL
    SELECT 'HIV_TX_CAG. 3'         AS S_NO,
           '20 - 24 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND dsd_category = 'Health extension professional led community'
      AND sex = 'Male'
-- 4 20 - 24 years, Female
    UNION ALL
    SELECT 'HIV_TX_CAG. 4'           AS S_NO,
           '20 - 24 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND dsd_category = 'Health extension professional led community'
      AND sex = 'Female'
-- 5 25 - 49 years, Male
    UNION ALL
    SELECT 'HIV_TX_CAG. 5'         AS S_NO,
           '25 - 49 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 49
      AND dsd_category = 'Health extension professional led community'
      AND sex = 'Male'
-- 6 25 - 49 years, Female
    UNION ALL
    SELECT 'HIV_TX_CAG. 6'           AS S_NO,
           '25 - 49 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 49
      AND dsd_category = 'Health extension professional led community'
      AND sex = 'Female'
-- 7 Greater than or equal to  50 years, Male
    UNION ALL
    SELECT 'HIV_TX_CAG. 7'                              AS S_NO,
           '>= 50 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND dsd_category = 'Health extension professional led community'
      AND sex = 'Male'
-- 8 Greater than or equal to  50 years, Female
    UNION ALL
    SELECT 'HIV_TX_CAG. 8'                                AS S_NO,
           '>= 50 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND dsd_category = 'Health extension professional led community'
      AND sex = 'Female'
-- Total number of clients on PCAD
    UNION ALl
    SELECT 'HIV_TX_PCAD'                     AS S_NO,
           'Total number of clients on PCAD' as Activity,
           COUNT(*)                          as Value
    FROM dsd
    where dsd_category = 'Community based group model by peer'
      AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
-- 1 15 - 19 years, Male
    UNION ALL
    SELECT 'HIV_TX_PCAD. 1'        AS S_NO,
           '15 - 19 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND dsd_category = 'Community based group model by peer'
      AND sex = 'Male'
-- 2 15 - 19 years, Female
    UNION ALL
    SELECT 'HIV_TX_PCAD. 2'          AS S_NO,
           '15 - 19 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND dsd_category = 'Community based group model by peer'
      AND sex = 'Female'
-- 3 20 - 24 years, Male
    UNION ALL
    SELECT 'HIV_TX_PCAD. 3'        AS S_NO,
           '20 - 24 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND dsd_category = 'Community based group model by peer'
      AND sex = 'Male'
-- 4 20 - 24 years, Female
    UNION ALL
    SELECT 'HIV_TX_PCAD. 4'          AS S_NO,
           '20 - 24 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND dsd_category = 'Community based group model by peer'
      AND sex = 'Female'
-- 5 25 - 49 years, Male
    UNION ALL
    SELECT 'HIV_TX_PCAD. 5'        AS S_NO,
           '25 - 49 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 49
      AND dsd_category = 'Community based group model by peer'
      AND sex = 'Male'
-- 6 25 - 49 years, Female
    UNION ALL
    SELECT 'HIV_TX_PCAD. 6'          AS S_NO,
           '25 - 49 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 49
      AND dsd_category = 'Community based group model by peer'
      AND sex = 'Female'
-- 7 Greater than or equal to  50 years, Male
    UNION ALL
    SELECT 'HIV_TX_PCAD. 7'                             AS S_NO,
           '>= 50 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND dsd_category = 'Community based group model by peer'
      AND sex = 'Male'
-- 8 Greater than or equal to  50 years, Female
    UNION ALL
    SELECT 'HIV_TX_PCAD. 8'                               AS S_NO,
           '>= 50 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND dsd_category = 'Community based group model by peer'
      AND sex = 'Female'
-- Total number of clients on Adolescent DSD
    UNION ALl
    SELECT 'HIV_TX_Adolescent DSD.'                    AS S_NO,
           'Total number of clients on Adolescent DSD' as Activity,
           COUNT(*)                                    as Value
    FROM dsd
    where dsd_category = 'DSD for adolescent'
-- Total number of clients on KP DSD
    UNION ALl
    SELECT 'HIV_TX_KP DSD.'                    AS S_NO,
           'Total number of clients on KP DSD' as Activity,
           COUNT(*)                            as Value
    FROM dsd
    where dsd_category = 'DSD for key populations'
-- Total number of clients on MCH DSD
    UNION ALl
    SELECT 'HIV_TX_MCH DSD.'                    AS S_NO,
           'Total number of clients on MCH DSD' as Activity,
           COUNT(*)                             as Value
    FROM dsd
    where dsd_category = 'DSD for maternal child health'
-- Total number of clients on other types of DSD
    UNION ALl
    SELECT 'HIV_TX_ other types of DSD.'                   AS S_NO,
           'Total number of clients on other types of DSD' as Activity,
           COUNT(*)                                        as Value
    FROM dsd
    where dsd_category = 'Other'


-- Total number of clients on "Advanced HIV Disease Care Model"
    UNION ALl
    SELECT 'HIV_TX_AHDCM'                                                 AS S_NO,
           'Total number of clients on "Advanced HIV Disease Care Model"' as Activity,
           COUNT(*)                                                       as Value
    FROM dsd
    where dsd_category = 'Advanced HIV disease model'
-- 1 Less than  1 year, Male
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 1'           AS S_NO,
           '< 1 year, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Male'
-- 2 Less than  1 year, Female
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 2'             AS S_NO,
           '< 1 year, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Female'
-- 3 1 - 4 years, Male
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 3'     AS S_NO,
           '1 - 4 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Male'
-- 4 1 - 4 years, Female
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 4'       AS S_NO,
           '1 - 4 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Female'
-- 5 5 - 9 years, Male
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 5'     AS S_NO,
           '5 - 9 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Male'
-- 6 5 - 9 years, Female
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 6'       AS S_NO,
           '5 - 9 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Female'
-- 7 10 - 14 years, Male
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 7'       AS S_NO,
           '10 - 14 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Male'
-- 8 10 - 14 years, Female
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 8'         AS S_NO,
           '10 - 14 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Female'
-- 9 15 - 19 years, Male
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 9'       AS S_NO,
           '15 - 19 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Male'
-- 10 15 - 19 years, Female
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 10'        AS S_NO,
           '15 - 19 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Female'
-- 11 20 - 24 years, Male
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 11'      AS S_NO,
           '20 - 24 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Male'
-- 12 20 - 24 years, Female
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 12'        AS S_NO,
           '20 - 24 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Female'
-- 13 25 - 49 years, Male
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 13'      AS S_NO,
           '25 - 49 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 49
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Male'
-- 14 25 - 49 years, Female
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 14'        AS S_NO,
           '25 - 49 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 49
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Female'
-- 15 Greater than or equal to  50 years, Male
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 15'                           AS S_NO,
           '>= 50 years, Male' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Male'
-- 16 Greater than or equal to  50 years, Female
    UNION ALL
    SELECT 'HIV_TX_AHDCM. 16'                             AS S_NO,
           '>= 50 years, Female' as Activity,
           COUNT(*)
    FROM dsd
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
      AND dsd_category = 'Advanced HIV disease model'
      AND sex = 'Female';
END //

DELIMITER ;