DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_tb_scrn_query;

CREATE PROCEDURE sp_fact_hmis_hiv_tb_scrn_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
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
                         was_the_patient_screened_for_tuberc as tb_screened,
                         tb_screening_date,
                         tb_treatment_status,
                         transferred_in_check_this_for_all_t as transferred_in,
                         date_started_on_tuberculosis_prophy,
                         screening_test_result_tuberculosis screening_result,
                         lf_lam_result
                  FROM mamba_flat_encounter_follow_up follow_up
                           JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                ON follow_up.encounter_id = follow_up_1.encounter_id
                           JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                ON follow_up.encounter_id = follow_up_2.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                     ON follow_up.encounter_id = follow_up_3.encounter_id),
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
                                AND tb_screened = 'Yes'
                                AND follow_up_date <= REPORT_END_DATE),
     latest_tb_screened_follow_up as (SELECT follow_up.client_id,
                                             sex,
                                             date_of_birth,
                                             encounter_id,
                                             follow_up_status,
                                             tb_screening_date,
                                             art_start_date,
                                             tb_screened,
                                             screening_result,
                                             lf_lam_result
                                      from tmp_latest_follow_up follow_up
                                               inner join mamba_dim_client client on follow_up.client_id = client.client_id
                                      where row_num = 1),
     tx_new_tb_screened as (select *
                            from latest_tb_screened_follow_up
                            where art_start_date between REPORT_START_DATE AND REPORT_END_DATE
                              and tb_screening_date between REPORT_START_DATE AND REPORT_END_DATE),
     prev_art_new_tb_screened as (select *
                                  from latest_tb_screened_follow_up
                                  where art_start_date < REPORT_START_DATE
                                    and tb_screening_date between REPORT_START_DATE AND REPORT_END_DATE
                                    and follow_up_status in ('Alive', 'Restart medication')
     )
SELECT 'HIV_TB_SCRN'                                                               AS S_NO,
       'Proportion of patients enrolled in HIV care who were screened for TB (FD)' as Activity,
       COUNT(*)                                                                    as Value
FROM tx_new_tb_screened

-- Negative result Positive
-- HIV_TB_SCRN.1
UNION ALL
SELECT 'HIV_TB_SCRN.1'                                                                             AS S_NO,
       'Number of NEWLY Enrolled ART clients who were screened for TB during the reporting period' as Activity,
       COUNT(*)                                                                                    as Value
FROM tx_new_tb_screened
-- Less than  15 years, Male
UNION ALL
SELECT 'HIV_TB_SCRN.1.1'            AS S_NO,
       '< 15 years, Male' as Activity,
       COUNT(*)                    as Value
FROM tx_new_tb_screened
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  AND sex = 'Male'
-- Less than  15 years, Female
UNION ALL
SELECT 'HIV_TB_SCRN.1.2'            AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*)                    as Value
FROM tx_new_tb_screened
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  AND sex = 'Female'
-- Greater than or equal to  15 years, Male
UNION ALL
SELECT 'HIV_TB_SCRN.1.3'            AS S_NO,
       '>= 15 years, Male' as Activity,
       COUNT(*)                    as Value
FROM tx_new_tb_screened
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  AND sex = 'Male'
-- Greater than or equal to  15 years, Female
UNION ALL
SELECT 'HIV_TB_SCRN.1.4'            AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*)                    as Value
FROM tx_new_tb_screened
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  AND sex = 'Female'
-- Screened Positive for TB
UNION ALL
SELECT 'HIV_TB_SCRN_P'            AS S_NO,
       'Screened Positive for TB' as Activity,
       COUNT(*)                    as Value
FROM tx_new_tb_screened
WHERE screening_result='Positive'
-- < 15 years, Male
UNION ALL
SELECT 'HIV_TB_SCRN_P. 1'            AS S_NO,
       '< 15 years, Male' as Activity,
       COUNT(*)                    as Value
FROM tx_new_tb_screened
WHERE screening_result='Positive'
  AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  AND sex='Male'
-- < 15 years, Female
UNION ALL
SELECT 'HIV_TB_SCRN_P. 2'            AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*)                    as Value
FROM tx_new_tb_screened
WHERE screening_result='Positive'
  AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  AND sex='Female'

-- >= 15 years, Male
UNION ALL
SELECT 'HIV_TB_SCRN_P. 3'            AS S_NO,
       '>= 15 years, Male' as Activity,
       COUNT(*)                    as Value
FROM tx_new_tb_screened
WHERE screening_result='Positive'
  AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  AND sex='Male'
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_TB_SCRN_P. 4'            AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*)                    as Value
FROM tx_new_tb_screened
WHERE screening_result='Positive'
  AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  AND sex='Female'

-- Number of PLHIVs PREVIOUSLY on ART and screened for TB
UNION ALL
SELECT 'HIV_TB_SCRN_ART'            AS S_NO,
       'Number of PLHIVs PREVIOUSLY on ART and screened for TB' as Activity,
       COUNT(*)                    as Value
FROM prev_art_new_tb_screened
-- < 15 years, Male
UNION ALL
SELECT 'HIV_TB_SCRN_ART. 1'            AS S_NO,
       '< 15 years, Male' as Activity,
       COUNT(*)                    as Value
FROM prev_art_new_tb_screened
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  AND sex='Male'
-- < 15 years, Female
UNION ALL
SELECT 'HIV_TB_SCRN_ART. 2'            AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*)                    as Value
FROM prev_art_new_tb_screened
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  AND sex='Female'
-- >= 15 years, Male
UNION ALL
SELECT 'HIV_TB_SCRN_ART. 3'            AS S_NO,
       '>= 15 years, Male' as Activity,
       COUNT(*)                    as Value
FROM prev_art_new_tb_screened
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  AND sex='Male'
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_TB_SCRN_ART. 4'            AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*)                    as Value
FROM prev_art_new_tb_screened
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  AND sex='Female'
-- Screened Positive for TB
UNION ALL
SELECT 'HIV_TB_SCRN_ART_P'            AS S_NO,
       'Screened Positive for TB' as Activity,
       COUNT(*)                    as Value
FROM prev_art_new_tb_screened
WHERE screening_result='Positive'
-- < 15 years, Male
UNION ALL
SELECT 'HIV_TB_SCRN_ART_P. 1'            AS S_NO,
       '< 15 years, Male' as Activity,
       COUNT(*)                    as Value
FROM prev_art_new_tb_screened
WHERE screening_result='Positive'
  AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  AND sex='Male'
-- < 15 years, Female
UNION ALL
SELECT 'HIV_TB_SCRN_ART_P. 2'            AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*)                    as Value
FROM prev_art_new_tb_screened
WHERE screening_result='Positive'
  AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  AND sex='Female'

-- >= 15 years, Male
UNION ALL
SELECT 'HIV_TB_SCRN_ART_P. 3'            AS S_NO,
       '>= 15 years, Male' as Activity,
       COUNT(*)                    as Value
FROM prev_art_new_tb_screened
WHERE screening_result='Positive'
  AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  AND sex='Male'
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_TB_SCRN_ART_P. 4'            AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*)                    as Value
FROM prev_art_new_tb_screened
WHERE screening_result='Positive'
  AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  AND sex='Female';
END //

DELIMITER ;