DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_phliv_tsp_query;

CREATE PROCEDURE sp_fact_hmis_phliv_tsp_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

WITH FollowUp as (select follow_up.encounter_id,
                         follow_up.client_id,
                         nutritional_supplements_provided,
                         eats_nutritious_foods,
                         nutritional_screening_result,
                         follow_up_date_followup_,
                         art_antiretroviral_start_date   as art_start_date,
                         pregnancy_status,
                         follow_up_status,
                         treatment_end_date,
                         coalesce(nutritional_status_of_adult,
                                  nutritional_status_of_older_child_a,
                                  weight_for_age_status) as nutritional_screening_status
                  from mamba_flat_encounter_follow_up follow_up
                           left join mamba_flat_encounter_follow_up_1 follow_up_1
                                     on follow_up.encounter_id = follow_up_1.encounter_id
                           left join mamba_flat_encounter_follow_up_2 follow_up_2
                                     on follow_up.encounter_id = follow_up_2.encounter_id
                           left join mamba_flat_encounter_follow_up_3 follow_up_3
                                     on follow_up.encounter_id = follow_up_3.encounter_id
                           left join mamba_flat_encounter_follow_up_4 follow_up_4
                                     on follow_up.encounter_id = follow_up_4.encounter_id),
     tx_curr_all AS (SELECT client_id,
                            follow_up_date_followup_ AS FollowupDate,
                            encounter_id,
                            follow_up_status,
                            treatment_end_date,
                            ROW_NUMBER()                OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) AS row_num
                     FROM FollowUp
                     WHERE follow_up_status IS NOT NULL
                       AND art_start_date IS NOT NULL
                       AND follow_up_date_followup_ <= REPORT_END_DATE),

     tx_curr AS (select *
                 from tx_curr_all
                 where row_num = 1
                   AND follow_up_status in ('Alive', 'Restart medication')
                   AND treatment_end_date > REPORT_END_DATE),
     tmp_screened_for_nutrition as (select encounter_id,
                                           client_id,
                                           nutritional_supplements_provided,
                                           eats_nutritious_foods,
                                           nutritional_screening_result,
                                           follow_up_date_followup_,
                                           pregnancy_status,
                                           nutritional_screening_status,
                                           ROW_NUMBER() over (PARTITION BY client_id order by follow_up_date_followup_ DESC ,encounter_id DESC ) as row_num
                                    from FollowUp
                                    where nutritional_screening_result is not null
                                      and follow_up_date_followup_ BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                                      and art_start_date is not null),
     screened_for_nutrition as (select tmp_screened_for_nutrition.*,
                                       client.date_of_birth,
                                       client.sex
                                from tmp_screened_for_nutrition
                                         join tx_curr on tmp_screened_for_nutrition.client_id = tx_curr.client_id
                                         join mamba_dim_client client
                                              on tmp_screened_for_nutrition.client_id = client.client_id
                                where tmp_screened_for_nutrition.row_num = 1),
     percentage_calc as (SELECT 'HIV_PLHIV_TSP'                                                                                                         AS S_NO,
                                'Proportion of clinically undernourished People Living with HIV (PLHIV) who received therapeutic or supplementary food' AS Activity,
                                CAST(ROUND((SELECT COUNT(*)
                                            FROM screened_for_nutrition
                                            where nutritional_screening_result = 'Undernourished'
                                              and (eats_nutritious_foods = 'Yes' or
                                                   nutritional_supplements_provided is not null)) *
                                           100.0 /
                                           (SELECT COUNT(*)
                                            FROM screened_for_nutrition
                                            where nutritional_screening_result = 'Undernourished'), 2) AS CHAR)
    AS Value
FROM screened_for_nutrition
    LIMIT 1)
-- Number of individuals receiving Pre-Exposure Prophylaxis
SELECT S_NO,
       Activity,
       Value
FROM percentage_calc
-- Number of PLHIV who were assessed/screened for malnutrition
UNION ALL
SELECT 'HIV_PLHIV_TSP.1'                                             AS S_NO,
       'Number of PLHIV who were assessed/screened for malnutrition' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
-- < 15 years, Male
UNION ALL
SELECT 'HIV_PLHIV_TSP.1. 1' AS S_NO,
       '< 15 years, Male'   as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
WHERE TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE)
    < 15
  AND sex = 'Male'
-- < 15 years, Female
UNION ALL
SELECT 'HIV_PLHIV_TSP.1. 2' AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
WHERE TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE)
    < 15
  AND sex = 'Female'
-- >= 15 years, Male
UNION ALL
SELECT 'HIV_PLHIV_TSP.1. 3' AS S_NO,
       '>= 15 years, Male'  as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
WHERE TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE) >= 15
  AND sex = 'Male'
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_PLHIV_TSP.1. 4'  AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
WHERE TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE) >= 15
  AND sex = 'Female'
-- Number of PLHIV that were nutritionally assessed and found to be clinically undernourished (disaggregated by Age, Sex and Pregnancy)
UNION ALL
SELECT 'HIV_PLHIV_NUT'                                                                                                                        AS S_NO,
       'Number of PLHIV that were nutritionally assessed and found to be clinically undernourished (disaggregated by Age, Sex and Pregnancy)' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
-- Total MAM
UNION ALL
SELECT 'HIV_PLHIV_NUT_MAM' AS S_NO,
       'Total MAM'         as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and nutritional_screening_status in ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)'
    , 'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
-- < 15 years, Male
UNION ALL
SELECT 'HIV_PLHIV_NUT_MAM. 1' AS S_NO,
       '< 15 years, Male'     as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and nutritional_screening_status in ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)'
    , 'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE)
    < 15
  and sex = 'Male'
-- < 15 years, Female
UNION ALL
SELECT 'HIV_PLHIV_NUT_MAM. 2' AS S_NO,
       '< 15 years, Female'   as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and nutritional_screening_status in ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)'
    , 'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE)
    < 15
  and sex = 'Female'
-- >= 15 years, Male
UNION ALL
SELECT 'HIV_PLHIV_NUT_MAM. 3' AS S_NO,
       '>= 15 years, Male'    as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and nutritional_screening_status in ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)'
    , 'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE) >= 15
  and sex = 'Male'
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_PLHIV_NUT_MAM. 4' AS S_NO,
       '>= 15 years, Female'  as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and nutritional_screening_status in ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)'
    , 'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE) >= 15
  and sex = 'Female'
-- Total SAM
UNION ALL
SELECT 'HIV_PLHIV_NUT_SAM' AS S_NO,
       'Total SAM'         as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and nutritional_screening_status in ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
-- < 15 years, Male
UNION ALL
SELECT 'HIV_PLHIV_NUT_SAM. 1' AS S_NO,
       '< 15 years, Male'     as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and nutritional_screening_status in ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE)
    < 15
  and sex = 'Male'
-- < 15 years, Female
UNION ALL
SELECT 'HIV_PLHIV_NUT_SAM. 2' AS S_NO,
       '< 15 years, Female'   as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and nutritional_screening_status in ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE)
    < 15
  and sex = 'Female'
-- >= 15 years, Male
UNION ALL
SELECT 'HIV_PLHIV_NUT_SAM. 3' AS S_NO,
       '>= 15 years, Male'    as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and nutritional_screening_status in ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE) >= 15
  and sex = 'Male'
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_PLHIV_NUT_SAM. 4' AS S_NO,
       '>= 15 years, Female'  as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and nutritional_screening_status in ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE) >= 15
  and sex = 'Female'
-- Clinically undernourished PLHIV who received therapeutic or supplementary food (disaggregated by age, sex and pregnancy status)
UNION ALL
SELECT 'HIV_PLHIV_SUP'                                                                                                                   AS S_NO,
       'Clinically undernourished PLHIV who received therapeutic or supplementary food (disaggregated by age, sex and pregnancy status)' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
-- Total MAM who received therapeutic or supplementary food
UNION ALL
SELECT 'HIV_PLHIV_SUP.1'                                          AS S_NO,
       'Total MAM who received therapeutic or supplementary food' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
  and nutritional_screening_status in ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)'
    , 'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
-- < 15 years, Male
UNION ALL
SELECT 'HIV_PLHIV_SUP.1. 1' AS S_NO,
       '< 15 years, Male'   as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
  and nutritional_screening_status in ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)'
    , 'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE)
    < 15
  and sex = 'Male'
-- < 15 years, Female - pregnant
UNION ALL
SELECT 'HIV_PLHIV_SUP.1. 2'            AS S_NO,
       '< 15 years, Female - pregnant' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
  and nutritional_screening_status in ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)'
    , 'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE)
    < 15
  and sex = 'Female'
  and pregnancy_status = 'Yes'
-- < 15 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_PLHIV_SUP.1. 3'                AS S_NO,
       '< 15 years, Female - non-pregnant' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
  and nutritional_screening_status in ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)'
    , 'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE)
    < 15
  and sex = 'Female'
  and (pregnancy_status = 'No'
   or pregnancy_status is null)
-- >= 15 years, Male
UNION ALL
SELECT 'HIV_PLHIV_SUP.1. 4' AS S_NO,
       '>= 15 years, Male'  as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
  and nutritional_screening_status in ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)'
    , 'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE) >= 15
  and sex = 'Male'
-- >= 15 years, Female - pregnant
UNION ALL
SELECT 'HIV_PLHIV_SUP.1. 5'             AS S_NO,
       '>= 15 years, Female - pregnant' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
  and nutritional_screening_status in ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)'
    , 'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE) >= 15
  and sex = 'Female'
  and pregnancy_status = 'Yes'
-- >= 15 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_PLHIV_SUP.1. 6'                 AS S_NO,
       '>= 15 years, Female - non-pregnant' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
  and nutritional_screening_status in ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)'
    , 'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE) >= 15
  and sex = 'Female'
  and (pregnancy_status = 'No'
   or pregnancy_status is null)


-- Total SAM who received therapeutic or supplementary food
UNION ALL
SELECT 'HIV_PLHIV_SUP.2'                                          AS S_NO,
       'Total SAM who received therapeutic or supplementary food' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
  and nutritional_screening_status in ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
-- < 15 years, Male
UNION ALL
SELECT 'HIV_PLHIV_SUP.2. 1' AS S_NO,
       '< 15 years, Male'   as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
  and nutritional_screening_status in ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE)
    < 15
  and sex = 'Male'
-- < 15 years, Female - pregnant
UNION ALL
SELECT 'HIV_PLHIV_SUP.2. 2'            AS S_NO,
       '< 15 years, Female - pregnant' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
  and nutritional_screening_status in ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE)
    < 15
  and sex = 'Female'
  and pregnancy_status = 'Yes'
-- < 15 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_PLHIV_SUP.2. 3'                AS S_NO,
       '< 15 years, Female - non-pregnant' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
  and nutritional_screening_status in ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE)
    < 15
  and sex = 'Female'
  and (pregnancy_status = 'No'
   or pregnancy_status is null)
-- >= 15 years, Male
UNION ALL
SELECT 'HIV_PLHIV_SUP.2. 4' AS S_NO,
       '>= 15 years, Male'  as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
  and nutritional_screening_status in ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE) >= 15
  and sex = 'Male'
-- >= 15 years, Female - pregnant
UNION ALL
SELECT 'HIV_PLHIV_SUP.2. 5'             AS S_NO,
       '>= 15 years, Female - pregnant' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
  and nutritional_screening_status in ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE) >= 15
  and sex = 'Female'
  and pregnancy_status = 'Yes'
-- >= 15 years, Female - non-pregnant
UNION ALL
SELECT 'HIV_PLHIV_SUP.2. 6'                 AS S_NO,
       '>= 15 years, Female - non-pregnant' as Activity,
       COUNT(*) as Value
FROM screened_for_nutrition
where nutritional_screening_result = 'Undernourished'
  and (eats_nutritious_foods = 'Yes'
   or nutritional_supplements_provided is not null)
  and nutritional_screening_status in ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
  and TIMESTAMPDIFF(YEAR
    , date_of_birth
    , REPORT_END_DATE) >= 15
  and sex = 'Female'
  and (pregnancy_status = 'No'
   or pregnancy_status is null);
END //

DELIMITER ;