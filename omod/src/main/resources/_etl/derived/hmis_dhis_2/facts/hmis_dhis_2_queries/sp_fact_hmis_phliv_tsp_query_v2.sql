DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_phliv_tsp_query_v2;

CREATE PROCEDURE sp_fact_hmis_phliv_tsp_query_v2(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

WITH FollowUp AS (SELECT encounter_id,
                         client_id,
                         nutritional_supplements_provided,
                         eats_nutritious_foods,
                         nutritional_screening_result,
                         follow_up_date_followup_,
                         art_antiretroviral_start_date                  AS art_start_date,
                         pregnancy_status,
                         follow_up_status,
                         treatment_end_date,
                         COALESCE(nutritional_status_of_adult,
                                  nutritional_status_of_older_child_a,
                                  weight_for_age_status)                AS nutritional_screening_status
                  FROM tmp_hmis_follow_up),
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
                   AND treatment_end_date >= REPORT_END_DATE),
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
     screened_agg AS (
         SELECT COUNT(*)                                                                                                                                                                             AS total,
                SUM(CASE WHEN age < 15 AND sex = 'Male'   THEN 1 ELSE 0 END)                                                                                                                        AS u15_male,
                SUM(CASE WHEN age < 15 AND sex = 'Female' THEN 1 ELSE 0 END)                                                                                                                        AS u15_female,
                SUM(CASE WHEN age >= 15 AND sex = 'Male'  THEN 1 ELSE 0 END)                                                                                                                        AS o15_male,
                SUM(CASE WHEN age >= 15 AND sex = 'Female' THEN 1 ELSE 0 END)                                                                                                                       AS o15_female,
                -- undernourished totals
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished' THEN 1 ELSE 0 END)                                                                                                     AS und_total,
                -- MAM
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND nutritional_screening_status IN ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)',
                                                                'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
                          THEN 1 ELSE 0 END)                                                                                                                                                         AS mam_total,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND nutritional_screening_status IN ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)',
                                                                'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
                           AND age < 15 AND sex = 'Male'   THEN 1 ELSE 0 END)                                                                                                                       AS mam_u15_male,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND nutritional_screening_status IN ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)',
                                                                'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
                           AND age < 15 AND sex = 'Female' THEN 1 ELSE 0 END)                                                                                                                       AS mam_u15_female,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND nutritional_screening_status IN ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)',
                                                                'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
                           AND age >= 15 AND sex = 'Male'  THEN 1 ELSE 0 END)                                                                                                                       AS mam_o15_male,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND nutritional_screening_status IN ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)',
                                                                'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
                           AND age >= 15 AND sex = 'Female' THEN 1 ELSE 0 END)                                                                                                                      AS mam_o15_female,
                -- SAM
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND nutritional_screening_status IN ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
                          THEN 1 ELSE 0 END)                                                                                                                                                         AS sam_total,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND nutritional_screening_status IN ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
                           AND age < 15 AND sex = 'Male'   THEN 1 ELSE 0 END)                                                                                                                       AS sam_u15_male,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND nutritional_screening_status IN ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
                           AND age < 15 AND sex = 'Female' THEN 1 ELSE 0 END)                                                                                                                       AS sam_u15_female,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND nutritional_screening_status IN ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
                           AND age >= 15 AND sex = 'Male'  THEN 1 ELSE 0 END)                                                                                                                       AS sam_o15_male,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND nutritional_screening_status IN ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
                           AND age >= 15 AND sex = 'Female' THEN 1 ELSE 0 END)                                                                                                                      AS sam_o15_female,
                -- supplementary food total
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                          THEN 1 ELSE 0 END)                                                                                                                                                         AS sup_total,
                -- MAM + food
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                           AND nutritional_screening_status IN ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)',
                                                                'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
                          THEN 1 ELSE 0 END)                                                                                                                                                         AS sup_mam_total,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                           AND nutritional_screening_status IN ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)',
                                                                'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
                           AND age < 15 AND sex = 'Male'   THEN 1 ELSE 0 END)                                                                                                                       AS sup_mam_u15_male,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                           AND nutritional_screening_status IN ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)',
                                                                'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
                           AND age < 15 AND sex = 'Female' AND pregnancy_status = 'Yes' THEN 1 ELSE 0 END)                                                                                          AS sup_mam_u15_female_p,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                           AND nutritional_screening_status IN ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)',
                                                                'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
                           AND age < 15 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END)                                                             AS sup_mam_u15_female_np,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                           AND nutritional_screening_status IN ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)',
                                                                'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
                           AND age >= 15 AND sex = 'Male'  THEN 1 ELSE 0 END)                                                                                                                       AS sup_mam_o15_male,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                           AND nutritional_screening_status IN ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)',
                                                                'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
                           AND age >= 15 AND sex = 'Female' AND pregnancy_status = 'Yes' THEN 1 ELSE 0 END)                                                                                         AS sup_mam_o15_female_p,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                           AND nutritional_screening_status IN ('Malnutrition of mild degree (Gomez: 75% to Less than 90% of Standard Weight)',
                                                                'Malnutrition of moderate degree (Gomez: 60% to Less than 75% of Standard Weight)')
                           AND age >= 15 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END)                                                            AS sup_mam_o15_female_np,
                -- SAM + food
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                           AND nutritional_screening_status IN ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
                          THEN 1 ELSE 0 END)                                                                                                                                                         AS sup_sam_total,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                           AND nutritional_screening_status IN ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
                           AND age < 15 AND sex = 'Male'   THEN 1 ELSE 0 END)                                                                                                                       AS sup_sam_u15_male,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                           AND nutritional_screening_status IN ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
                           AND age < 15 AND sex = 'Female' AND pregnancy_status = 'Yes' THEN 1 ELSE 0 END)                                                                                          AS sup_sam_u15_female_p,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                           AND nutritional_screening_status IN ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
                           AND age < 15 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END)                                                             AS sup_sam_u15_female_np,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                           AND nutritional_screening_status IN ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
                           AND age >= 15 AND sex = 'Male'  THEN 1 ELSE 0 END)                                                                                                                       AS sup_sam_o15_male,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                           AND nutritional_screening_status IN ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
                           AND age >= 15 AND sex = 'Female' AND pregnancy_status = 'Yes' THEN 1 ELSE 0 END)                                                                                         AS sup_sam_o15_female_p,
                SUM(CASE WHEN nutritional_screening_result = 'Undernourished'
                           AND (eats_nutritious_foods = 'Yes' OR nutritional_supplements_provided IS NOT NULL)
                           AND nutritional_screening_status IN ('Severe protein-calorie malnutrition (Gomez: Less than 60% of Standard Weight)')
                           AND age >= 15 AND sex = 'Female' AND (pregnancy_status = 'No' OR pregnancy_status IS NULL) THEN 1 ELSE 0 END)                                                            AS sup_sam_o15_female_np
         FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age FROM screened_for_nutrition) t
     ),
     percentage_calc AS (SELECT 'HIV_PLHIV_TSP'                                                                                                         AS S_NO,
                                'Proportion of clinically undernourished People Living with HIV (PLHIV) who received therapeutic or supplementary food' AS Activity,
                                CASE
                                    WHEN (SELECT und_total FROM screened_agg) = 0 THEN
                                        '0%'
                                    ELSE
                                        CONCAT(CAST(ROUND(
                                                (CAST((SELECT sup_total FROM screened_agg) AS REAL) * 100.0) /
                                                (SELECT und_total FROM screened_agg)
                                            , 2) AS CHAR), '%')
                                    END                                                                                                                 AS Value
FROM (SELECT 1) AS dummy
    )

SELECT S_NO, Activity, Value FROM percentage_calc
UNION ALL SELECT 'HIV_PLHIV_TSP.1',      'Number of PLHIV who were assessed/screened for malnutrition',                                                                       COALESCE(total,0)              FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_TSP.1. 1',   '< 15 years, Male',                                                                                                                  COALESCE(u15_male,0)           FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_TSP.1. 2',   '< 15 years, Female',                                                                                                                COALESCE(u15_female,0)         FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_TSP.1. 3',   '>= 15 years, Male',                                                                                                                 COALESCE(o15_male,0)           FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_TSP.1. 4',   '>= 15 years, Female',                                                                                                               COALESCE(o15_female,0)         FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_NUT',         'Number of PLHIV that were nutritionally assessed and found to be clinically undernourished (disaggregated by Age, Sex and Pregnancy)', COALESCE(und_total,0)       FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_NUT_MAM',    'Total MAM',                                                                                                                          COALESCE(mam_total,0)          FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_NUT_MAM. 1', '< 15 years, Male',                                                                                                                  COALESCE(mam_u15_male,0)       FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_NUT_MAM. 2', '< 15 years, Female',                                                                                                                COALESCE(mam_u15_female,0)     FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_NUT_MAM. 3', '>= 15 years, Male',                                                                                                                 COALESCE(mam_o15_male,0)       FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_NUT_MAM. 4', '>= 15 years, Female',                                                                                                               COALESCE(mam_o15_female,0)     FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_NUT_SAM',    'Total SAM',                                                                                                                          COALESCE(sam_total,0)          FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_NUT_SAM. 1', '< 15 years, Male',                                                                                                                  COALESCE(sam_u15_male,0)       FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_NUT_SAM. 2', '< 15 years, Female',                                                                                                                COALESCE(sam_u15_female,0)     FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_NUT_SAM. 3', '>= 15 years, Male',                                                                                                                 COALESCE(sam_o15_male,0)       FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_NUT_SAM. 4', '>= 15 years, Female',                                                                                                               COALESCE(sam_o15_female,0)     FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP',         'Clinically undernourished PLHIV who received therapeutic or supplementary food (disaggregated by age, sex and pregnancy status)',   COALESCE(sup_total,0)          FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP.1',       'Total MAM who received therapeutic or supplementary food',                                                                          COALESCE(sup_mam_total,0)      FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP.1. 1',   '< 15 years, Male',                                                                                                                  COALESCE(sup_mam_u15_male,0)   FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP.1. 2',   '< 15 years, Female - pregnant',                                                                                                     COALESCE(sup_mam_u15_female_p,0)  FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP.1. 3',   '< 15 years, Female - non-pregnant',                                                                                                 COALESCE(sup_mam_u15_female_np,0) FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP.1. 4',   '>= 15 years, Male',                                                                                                                 COALESCE(sup_mam_o15_male,0)   FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP.1. 5',   '>= 15 years, Female - pregnant',                                                                                                    COALESCE(sup_mam_o15_female_p,0)  FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP.1. 6',   '>= 15 years, Female - non-pregnant',                                                                                                COALESCE(sup_mam_o15_female_np,0) FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP.2',       'Total SAM who received therapeutic or supplementary food',                                                                          COALESCE(sup_sam_total,0)      FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP.2. 1',   '< 15 years, Male',                                                                                                                  COALESCE(sup_sam_u15_male,0)   FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP.2. 2',   '< 15 years, Female - pregnant',                                                                                                     COALESCE(sup_sam_u15_female_p,0)  FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP.2. 3',   '< 15 years, Female - non-pregnant',                                                                                                 COALESCE(sup_sam_u15_female_np,0) FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP.2. 4',   '>= 15 years, Male',                                                                                                                 COALESCE(sup_sam_o15_male,0)   FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP.2. 5',   '>= 15 years, Female - pregnant',                                                                                                    COALESCE(sup_sam_o15_female_p,0)  FROM screened_agg
UNION ALL SELECT 'HIV_PLHIV_SUP.2. 6',   '>= 15 years, Female - non-pregnant',                                                                                                COALESCE(sup_sam_o15_female_np,0) FROM screened_agg;
END //

DELIMITER ;
