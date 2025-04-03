DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_fp_query;

CREATE PROCEDURE sp_fact_hmis_hiv_fp_query(IN REPORT_END_DATE DATE)
BEGIN

WITH FollowUp as (select follow_up.encounter_id,
                         follow_up.client_id,
                         method_of_family_planning,
                         on_family_planning,
                         follow_up_date_followup_,
                         pregnancy_status,
                         follow_up_status,
                         treatment_end_date,
                         art_antiretroviral_start_date art_start_date
                  from mamba_flat_encounter_follow_up follow_up
                           left join mamba_flat_encounter_follow_up_1 follow_up_1
                                     on follow_up.encounter_id = follow_up_1.encounter_id
                           left join mamba_flat_encounter_follow_up_2 follow_up_2
                                     on follow_up.encounter_id = follow_up_2.encounter_id
                           left join mamba_flat_encounter_follow_up_3 follow_up_3
                                     on follow_up.encounter_id = follow_up_3.encounter_id
                           left join mamba_flat_encounter_follow_up_4 follow_up_4
                                     on follow_up.encounter_id = follow_up_4.encounter_id),
     -- TX curr
     tx_curr_all AS (SELECT client_id,
                            follow_up_date_followup_                                                                             AS FollowupDate,
                            encounter_id,
                            follow_up_status,
                            treatment_end_date,
                            method_of_family_planning,
                            on_family_planning,
                            pregnancy_status,
                            follow_up_date_followup_,
                            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS row_num
                     FROM FollowUp
                     WHERE follow_up_status IS NOT NULL
                       AND art_start_date IS NOT NULL
                       AND follow_up_date_followup_ <= REPORT_END_DATE),

     tx_curr AS (select *
                 from tx_curr_all
                 where row_num = 1
                   AND follow_up_status in ('Alive', 'Restart medication')
                   AND treatment_end_date >= REPORT_END_DATE),
     tmp_fp as (select encounter_id,
                       client_id,
                       method_of_family_planning,
                       on_family_planning,
                       pregnancy_status,
                       fn_gregorian_to_ethiopian_calendar(follow_up_date_followup_, 'Y-M-D')                                as follow_up_date,
                       ROW_NUMBER() over (PARTITION BY client_id order by follow_up_date_followup_ DESC ,encounter_id DESC) as row_num
                from tx_curr
                where on_family_planning = 'Yes'
                  and method_of_family_planning != 'Sexual abstinence'),
    fp as (select tmp_fp.*,
    client.sex,
    client.date_of_birth,
    patient_name,
    mrn
from tmp_fp
    join mamba_dim_client client on tmp_fp.client_id = client.client_id
    join tx_curr on tmp_fp.client_id = tx_curr.client_id
where tmp_fp.row_num = 1),
    family_planning_wo_age as (select *
from fp
where sex = 'Female'
  and (pregnancy_status = 'No' or pregnancy_status is null)),
    family_planning as (select *
from fp
where sex = 'Female'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 49
  and (pregnancy_status = 'No' or pregnancy_status is null))
-- Number of non-pregnant women living with HIV on ART aged 15-49 reporting the use of any method of modern family planning by age
SELECT 'HIV_ART_FP'                                                                                                                      AS S_NO,
       'Number of non-pregnant women living with HIV on ART aged 15-49 reporting the use of any method of modern family planning by age' as Activity,
       COUNT(*)                                                                                                                          as Value
FROM family_planning
-- 10 - 14 years
UNION ALL
SELECT 'HIV_ART_FP. 1' AS S_NO,
       '10 - 14 years' as Activity,
       COUNT(*)        as Value
FROM family_planning_wo_age
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
-- 15 - 19 years
UNION ALL
SELECT 'HIV_ART_FP. 2' AS S_NO,
       '15 - 19 years' as Activity,
       COUNT(*)        as Value
FROM family_planning
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
-- 20 - 24 years
UNION ALL
SELECT 'HIV_ART_FP. 3' AS S_NO,
       '20 - 24 years' as Activity,
       COUNT(*)        as Value
FROM family_planning
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
-- 25 - 29 years
UNION ALL
SELECT 'HIV_ART_FP. 4' AS S_NO,
       '25 - 29 years' as Activity,
       COUNT(*)        as Value
FROM family_planning
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
-- 30 - 49 years
UNION ALL
SELECT 'HIV_ART_FP. 5' AS S_NO,
       '30 - 49 years' as Activity,
       COUNT(*)        as Value
FROM family_planning
WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 49

-- Number of non-pregnant women living with HIV on ART aged 15-49 reporting the use of any method of modern family planning by method
UNION ALL
SELECT 'HIV_ART_FP_MET'                                                                                                                     AS S_NO,
       'Number of non-pregnant women living with HIV on ART aged 15-49 reporting the use of any method of modern family planning by method' as Activity,
       COUNT(*)                                                                                                                             as Value
FROM family_planning
-- Oral contraceptives
UNION ALL
SELECT 'HIV_ART_FP_MET. 1'   AS S_NO,
       'Oral contraceptives' as Activity,
       COUNT(*)              as Value
FROM family_planning
WHERE method_of_family_planning = 'Oral contraception'
-- Injectables
UNION ALL
SELECT 'HIV_ART_FP_MET. 2' AS S_NO,
       'Injectables'       as Activity,
       COUNT(*)            as Value
FROM family_planning
WHERE method_of_family_planning = 'Injectable contraceptives'
-- Implants
UNION ALL
SELECT 'HIV_ART_FP_MET. 3' AS S_NO,
       'Implants'          as Activity,
       COUNT(*)            as Value
FROM family_planning
WHERE method_of_family_planning = 'Implantable contraceptive (unspecified type)'
-- IUCD
UNION ALL
SELECT 'HIV_ART_FP_MET. 4' AS S_NO,
       'IUCD'              as Activity,
       COUNT(*)            as Value
FROM family_planning
WHERE method_of_family_planning = 'Intrauterine device'
-- Others
UNION ALL
SELECT 'HIV_ART_FP_MET. 5' AS S_NO,
       'Others'            as Activity,
       COUNT(*)            as Value
FROM family_planning
WHERE method_of_family_planning not in
    ('Oral contraception', 'Injectable contraceptives', 'Implantable contraceptive (unspecified type)','Intrauterine device');
END //

DELIMITER ;