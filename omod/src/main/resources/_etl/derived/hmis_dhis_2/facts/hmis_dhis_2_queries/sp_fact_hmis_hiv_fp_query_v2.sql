DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_fp_query_v2;

CREATE PROCEDURE sp_fact_hmis_hiv_fp_query_v2(IN REPORT_END_DATE DATE)
BEGIN

WITH FollowUp AS (SELECT encounter_id,
                         client_id,
                         method_of_family_planning,
                         on_family_planning,
                         follow_up_date_followup_,
                         pregnancy_status,
                         follow_up_status,
                         treatment_end_date,
                         art_antiretroviral_start_date AS art_start_date
                  FROM tmp_hmis_follow_up),
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
                       ROW_NUMBER() over (PARTITION BY client_id order by follow_up_date_followup_ DESC ,encounter_id DESC) as row_num
                from tx_curr
                where method_of_family_planning != 'None'
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
  and (pregnancy_status = 'No' or pregnancy_status is null)),
    fp_agg AS (
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN age BETWEEN 15 AND 19 THEN 1 ELSE 0 END) AS u19,
            SUM(CASE WHEN age BETWEEN 20 AND 24 THEN 1 ELSE 0 END) AS u24,
            SUM(CASE WHEN age BETWEEN 25 AND 29 THEN 1 ELSE 0 END) AS u29,
            SUM(CASE WHEN age BETWEEN 30 AND 49 THEN 1 ELSE 0 END) AS u49,
            SUM(CASE WHEN method_of_family_planning = 'Oral contraception'                                                                                        THEN 1 ELSE 0 END) AS oral,
            SUM(CASE WHEN method_of_family_planning = 'Injectable contraceptives'                                                                                 THEN 1 ELSE 0 END) AS injectable,
            SUM(CASE WHEN method_of_family_planning = 'Implantable contraceptive (unspecified type)'                                                              THEN 1 ELSE 0 END) AS implant,
            SUM(CASE WHEN method_of_family_planning = 'Intrauterine device'                                                                                       THEN 1 ELSE 0 END) AS iucd,
            SUM(CASE WHEN method_of_family_planning NOT IN ('Oral contraception','Injectable contraceptives','Implantable contraceptive (unspecified type)','Intrauterine device') THEN 1 ELSE 0 END) AS other
        FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age FROM family_planning) t
    )
SELECT 'HIV_ART_FP_AGE' AS S_NO, 'Number of non-pregnant women living with HIV on ART aged 15-49 reporting the use of any method of modern family planning by age' AS Activity, total AS Value FROM fp_agg
UNION ALL SELECT 'HIV_ART_FP_AGE. 1', '10 - 14 years', COUNT(*) FROM family_planning_wo_age WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14
UNION ALL SELECT 'HIV_ART_FP_AGE. 2', '15 - 19 years', COALESCE(u19,0)   FROM fp_agg
UNION ALL SELECT 'HIV_ART_FP_AGE. 3', '20 - 24 years', COALESCE(u24,0)   FROM fp_agg
UNION ALL SELECT 'HIV_ART_FP_AGE. 4', '25 - 29 years', COALESCE(u29,0)   FROM fp_agg
UNION ALL SELECT 'HIV_ART_FP_AGE. 5', '30 - 49 years', COALESCE(u49,0)   FROM fp_agg
UNION ALL SELECT 'HIV_ART_FP_MET',    'Number of non-pregnant women living with HIV on ART aged 15-49 reporting the use of any method of modern family planning by method', total FROM fp_agg
UNION ALL SELECT 'HIV_ART_FP_MET. 1', 'Oral contraceptives', COALESCE(oral,0)       FROM fp_agg
UNION ALL SELECT 'HIV_ART_FP_MET. 2', 'Injectables',         COALESCE(injectable,0)  FROM fp_agg
UNION ALL SELECT 'HIV_ART_FP_MET. 3', 'Implants',            COALESCE(implant,0)     FROM fp_agg
UNION ALL SELECT 'HIV_ART_FP_MET. 4', 'IUCD',                COALESCE(iucd,0)        FROM fp_agg
UNION ALL SELECT 'HIV_ART_FP_MET. 5', 'Others',              COALESCE(other,0)       FROM fp_agg;
END //

DELIMITER ;