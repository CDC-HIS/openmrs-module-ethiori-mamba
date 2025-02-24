DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_prep_query;

CREATE PROCEDURE  sp_fact_hmis_hiv_tpt_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
WITH FollowUp AS (select follow_up.encounter_id,
                         follow_up.client_id,
                         date_started_on_tuberculosis_prophy tpt_start_date,
                         date_completed_tuberculosis_prophyl tpt_completed_date,
                         follow_up_date_followup_ as         follow_up_date,
                         tb_prophylaxis_type,
                         art_antiretroviral_start_date as art_start_date
                  from mamba_flat_encounter_follow_up follow_up
                           left join mamba_flat_encounter_follow_up_1 follow_up_1
                                     on follow_up.encounter_id = follow_up_1.encounter_id
                           left join mamba_flat_encounter_follow_up_2 follow_up_2
                                     on follow_up.encounter_id = follow_up_2.encounter_id
                           left join mamba_flat_encounter_follow_up_3 follow_up_3
                                     on follow_up.encounter_id = follow_up_3.encounter_id
                           left join mamba_flat_encounter_follow_up_4 follow_up_4
                                     on follow_up.encounter_id = follow_up_4.encounter_id),
     tmp_art_tpt as (select *,
                            ROW_NUMBER() over (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) as row_num
                     from FollowUp
                     where tpt_start_date is not null
     ),
     art_tpt as (select tmp_art_tpt.encounter_id,
                        tmp_art_tpt.client_id,
                        follow_up_date,
                        tpt_start_date,
                        tpt_completed_date,
                        client.sex,
                        tb_prophylaxis_type,
                        date_of_birth
                 from tmp_art_tpt
                          left join mamba_dim_client client on tmp_art_tpt.client_id=client.client_id
                 where row_num = 1
                   and art_start_date is not null
                   and tpt_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
     ),
     art_tpt_cr as (
         select tmp_art_tpt.encounter_id,
                tmp_art_tpt.client_id,
                follow_up_date,
                tpt_start_date,
                tpt_completed_date,
                client.sex,
                tb_prophylaxis_type,
                date_of_birth
         from tmp_art_tpt
                  left join mamba_dim_client client on tmp_art_tpt.client_id=client.client_id
         where row_num = 1
           and art_start_date is not null
           and tpt_start_date BETWEEN DATE_ADD(REPORT_START_DATE, INTERVAL -12 MONTH ) AND REPORT_START_DATE
     )
-- Number of ART patients who started on a standard course of TB Preventive Treatment (TPT) in the reporting period
SELECT 'HIV_ART_TPT.1'                                                                       AS S_NO,
       'Number of ART patients who started on a standard course of TB Preventive Treatment (TPT) in the reporting period' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
-- Patients on 6H
UNION ALL
SELECT 'HIV_ART_TPT_6H'                                                                       AS S_NO,
       'Patients on 6H' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='6H'
-- < 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_6H. 1'                                                                       AS S_NO,
       '< 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='6H'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Male'
-- < 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_6H. 2'                                                                       AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='6H'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Female'

-- >= 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_6H. 3'                                                                       AS S_NO,
       '>= 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='6H'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Male'
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_6H. 4'                                                                       AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='6H'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Female'

-- Patients on 3HP
UNION ALL
SELECT 'HIV_ART_TPT_3HP'                                                                       AS S_NO,
       'Patients on 3HP' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='3HP'
-- < 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_3HP. 1'                                                                       AS S_NO,
       '< 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='3HP'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Male'
-- < 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_3HP. 2'                                                                       AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='3HP'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Female'

-- >= 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_3HP. 3'                                                                       AS S_NO,
       '>= 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='3HP'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Male'
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_3HP. 4'                                                                       AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='3HP'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Female'

-- Patients on 3HR
UNION ALL
SELECT 'HIV_ART_TPT_3HR'                                                                       AS S_NO,
       'Patients on 3HR' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='3HR'
-- < 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_3HR. 1'                                                                       AS S_NO,
       '< 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='3HR'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Male'
-- < 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_3HR. 2'                                                                       AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='3HR'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Female'

-- >= 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_3HR. 3'                                                                       AS S_NO,
       '>= 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='3HR'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Male'
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_3HR. 4'                                                                       AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt
WHERE tb_prophylaxis_type='3HR'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Female'
-- Number of ART patients who started on a standard course of TB Preventive Treatment (TPT) in the reporting period
UNION ALL
SELECT 'HIV_ART_TPT_CR.1'                                                                       AS S_NO,
       'Number of ART patients who were initiated on any course of TPT 12 months before the reporting period' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
-- Patients on 6H 12 months prior to the reporting period
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.1'                                                                       AS S_NO,
       'Patients on 6H 12 months prior to the reporting period' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='6H'
-- < 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.1. 1'                                                                       AS S_NO,
       '< 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='6H'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Male'
-- < 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.1. 2'                                                                       AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='6H'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Female'

-- >= 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.1. 3'                                                                       AS S_NO,
       '>= 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='6H'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Male'
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.1. 4'                                                                       AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='6H'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Female'

-- Patients on 3HP 12 months prior to the reporting period
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.2'                                                                       AS S_NO,
       'Patients on 3HP 12 months prior to the reporting period' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HP'
-- < 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.2. 1'                                                                       AS S_NO,
       '< 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HP'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Male'
-- < 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.2. 2'                                                                       AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HP'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Female'

-- >= 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.2. 3'                                                                       AS S_NO,
       '>= 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HP'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Male'
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.2. 4'                                                                       AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HP'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Female'
-- Patients on 3HR 12 months prior to the reporting period
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.3'                                                                       AS S_NO,
       'Patients on 3HR 12 months prior to the reporting period' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HR'
-- < 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.3 1'                                                                       AS S_NO,
       '< 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HR'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Male'
-- < 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.3 2'                                                                       AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HR'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Female'

-- >= 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.3 3'                                                                       AS S_NO,
       '>= 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HR'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Male'
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_CR.1.3 4'                                                                       AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HR'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Female'
-- Number of ART patients who started TPT 12 months prior to the reproting period that completed a full course of therapy
UNION ALL
SELECT 'HIV_ART_TPT_CR.2'                                                                       AS S_NO,
       'Number of ART patients who started TPT 12 months prior to the reproting period that completed a full course of therapy' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tpt_completed_date is not null
-- Patients who completed 6H
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.1'                                                                       AS S_NO,
       'Patients who completed 6H' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='6H'
  AND tpt_completed_date is not null
-- < 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.1. 1'                                                                       AS S_NO,
       '< 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='6H'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Male'
  and tpt_completed_date is not null
-- < 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.1. 2'                                                                       AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='6H'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Female'
  and tpt_completed_date is not null
-- >= 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.1. 3'                                                                       AS S_NO,
       '>= 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='6H'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Male'
  and tpt_completed_date is not null
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.1. 4'                                                                       AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='6H'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Female'
  and tpt_completed_date is not null
-- Patients who completed 3HP
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.2'                                                                       AS S_NO,
       'Patients who completed 3HP' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HP'
  and tpt_completed_date is not null
-- < 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.2. 1'                                                                       AS S_NO,
       '< 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HP'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Male'
  and tpt_completed_date is not null
-- < 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.2. 2'                                                                       AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HP'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Female'
  and tpt_completed_date is not null
-- >= 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.2. 3'                                                                       AS S_NO,
       '>= 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HP'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Male'
  and tpt_completed_date is not null
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.2. 4'                                                                       AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HP'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Female'
  and tpt_completed_date is not null
-- Patients who completed 3HR
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.3'                                                                       AS S_NO,
       'Patients who completed 3HR' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HR'
  and tpt_completed_date is not null
-- < 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.3 1'                                                                       AS S_NO,
       '< 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HR'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Male'
  and tpt_completed_date is not null
-- < 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.3 2'                                                                       AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HR'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15
  and sex='Female'
  and tpt_completed_date is not null
-- >= 15 years, Male
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.3 3'                                                                       AS S_NO,
       '>= 15 years, Male' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HR'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Male'
  and tpt_completed_date is not null
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_ART_TPT_CR.2.3 4'                                                                       AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*)                                                                                as Value
FROM art_tpt_cr
WHERE tb_prophylaxis_type='3HR'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15
  and sex='Female'
  and tpt_completed_date is not null;
END //

DELIMITER ;