DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_cxca_scrn_query_v2;

CREATE PROCEDURE  sp_fact_hmis_cxca_scrn_query_v2(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
WITH FollowUp AS (SELECT encounter_id,
                         client_id,
                         cervical_cancer_screening_status          AS cx_ca_screening_status,
                         cervical_cancer_screening_method_strategy AS screening_type,
                         via_screening_result,
                         hpv_dna_screening_result,
                         follow_up_date_followup_                  AS follow_up_date,
                         date_hpv_test_was_done                    AS hpv_date,
                         date_visual_inspection_of_the_cervi       AS via_date,
                         hpv_dna_result_received_date              AS hpv_received_date,
                         is_the_client_screened_in_this_facility
                  FROM tmp_hmis_follow_up),
     tmp_cx_screened as (select *,
                                ROW_NUMBER() over (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) as row_num
                         from FollowUp
                         where cx_ca_screening_status = 'Cervical cancer screening performed'),
     cx_screened as (select tmp_cx_screened.*,
                            client.date_of_birth,
                            client.sex
                     from tmp_cx_screened
                              left join mamba_dim_client client on tmp_cx_screened.client_id=client.client_id
                     where row_num = 1
                       and (hpv_received_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE or
                            (via_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                               --  AND screening_type != 'Human Papillomavirus test'
                                )
                         )
                     and is_the_client_screened_in_this_facility = 'Yes'
                     ),
     scrn_agg AS (
         SELECT
             COUNT(*) AS Value,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)'  THEN 1 ELSE 0 END) AS via_total,
             SUM(CASE WHEN screening_type = 'Human Papillomavirus test'                               THEN 1 ELSE 0 END) AS hpv_total,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result = 'VIA negative'                                                                                               THEN 1 ELSE 0 END) AS via_normal_total,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result = 'VIA negative'                                                              AND age BETWEEN 15 AND 19 THEN 1 ELSE 0 END) AS via_normal_u19,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result = 'VIA negative'                                                              AND age BETWEEN 20 AND 24 THEN 1 ELSE 0 END) AS via_normal_u24,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result = 'VIA negative'                                                              AND age BETWEEN 25 AND 29 THEN 1 ELSE 0 END) AS via_normal_u29,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result = 'VIA negative'                                                              AND age BETWEEN 30 AND 49 THEN 1 ELSE 0 END) AS via_normal_u49,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result = 'VIA negative'                                                              AND age >= 50              THEN 1 ELSE 0 END) AS via_normal_o50,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result IN ('VIA positive: non-eligible for cryo/thermo-coagula','VIA positive: eligible for cryo/thermo-coagula')                  THEN 1 ELSE 0 END) AS via_precancer_total,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result IN ('VIA positive: non-eligible for cryo/thermo-coagula','VIA positive: eligible for cryo/thermo-coagula') AND age BETWEEN 15 AND 19 THEN 1 ELSE 0 END) AS via_precancer_u19,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result IN ('VIA positive: non-eligible for cryo/thermo-coagula','VIA positive: eligible for cryo/thermo-coagula') AND age BETWEEN 20 AND 24 THEN 1 ELSE 0 END) AS via_precancer_u24,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result IN ('VIA positive: non-eligible for cryo/thermo-coagula','VIA positive: eligible for cryo/thermo-coagula') AND age BETWEEN 25 AND 29 THEN 1 ELSE 0 END) AS via_precancer_u29,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result IN ('VIA positive: non-eligible for cryo/thermo-coagula','VIA positive: eligible for cryo/thermo-coagula') AND age BETWEEN 30 AND 49 THEN 1 ELSE 0 END) AS via_precancer_u49,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result IN ('VIA positive: non-eligible for cryo/thermo-coagula','VIA positive: eligible for cryo/thermo-coagula') AND age >= 50              THEN 1 ELSE 0 END) AS via_precancer_o50,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result = 'suspected cervical cancer'                                                                                               THEN 1 ELSE 0 END) AS via_cancer_total,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result = 'suspected cervical cancer'                                                 AND age BETWEEN 15 AND 19 THEN 1 ELSE 0 END) AS via_cancer_u19,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result = 'suspected cervical cancer'                                                 AND age BETWEEN 20 AND 24 THEN 1 ELSE 0 END) AS via_cancer_u24,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result = 'suspected cervical cancer'                                                 AND age BETWEEN 25 AND 29 THEN 1 ELSE 0 END) AS via_cancer_u29,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result = 'suspected cervical cancer'                                                 AND age BETWEEN 30 AND 49 THEN 1 ELSE 0 END) AS via_cancer_u49,
             SUM(CASE WHEN screening_type = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND via_screening_result = 'suspected cervical cancer'                                                 AND age >= 50              THEN 1 ELSE 0 END) AS via_cancer_o50,
             SUM(CASE WHEN screening_type = 'Human Papillomavirus test' AND hpv_dna_screening_result = 'Positive'                             THEN 1 ELSE 0 END) AS hpv_pos_total,
             SUM(CASE WHEN screening_type = 'Human Papillomavirus test' AND hpv_dna_screening_result = 'Positive' AND age BETWEEN 15 AND 19  THEN 1 ELSE 0 END) AS hpv_pos_u19,
             SUM(CASE WHEN screening_type = 'Human Papillomavirus test' AND hpv_dna_screening_result = 'Positive' AND age BETWEEN 20 AND 24  THEN 1 ELSE 0 END) AS hpv_pos_u24,
             SUM(CASE WHEN screening_type = 'Human Papillomavirus test' AND hpv_dna_screening_result = 'Positive' AND age BETWEEN 25 AND 29  THEN 1 ELSE 0 END) AS hpv_pos_u29,
             SUM(CASE WHEN screening_type = 'Human Papillomavirus test' AND hpv_dna_screening_result = 'Positive' AND age BETWEEN 30 AND 49  THEN 1 ELSE 0 END) AS hpv_pos_u49,
             SUM(CASE WHEN screening_type = 'Human Papillomavirus test' AND hpv_dna_screening_result = 'Positive' AND age >= 50               THEN 1 ELSE 0 END) AS hpv_pos_o50
         FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age FROM cx_screened) t
     )
SELECT 'HIV_CXCA_SCRN.1'    AS S_NO, 'Cervical Cancer screening by type of test'   AS Activity, Value              FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.1. 1',   'Screened by VIA',                                     COALESCE(via_total,0)          FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.1. 2',   'Screened by HPV DNA',                                 COALESCE(hpv_total,0)          FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2',      'VIA Screening Result',                                 COALESCE(via_total,0)          FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.1',    'Normal cervix:',                                       COALESCE(via_normal_total,0)   FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.1. 1', '15 - 19 years',                                       COALESCE(via_normal_u19,0)     FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.1. 2', '20 - 24 years',                                       COALESCE(via_normal_u24,0)     FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.1. 3', '25 - 29 years',                                       COALESCE(via_normal_u29,0)     FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.1. 4', '30 - 49 years',                                       COALESCE(via_normal_u49,0)     FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.1. 5', '>= 50 years',                                         COALESCE(via_normal_o50,0)     FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.3',    'Precancerous Lesion:',                                 COALESCE(via_precancer_total,0) FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.3. 1', '15 - 19 years',                                       COALESCE(via_precancer_u19,0)  FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.3. 2', '20 - 24 years',                                       COALESCE(via_precancer_u24,0)  FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.3. 3', '25 - 29 years',                                       COALESCE(via_precancer_u29,0)  FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.3. 4', '30 - 49 years',                                       COALESCE(via_precancer_u49,0)  FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.3. 5', '>= 50 years',                                         COALESCE(via_precancer_o50,0)  FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.4',    'Suspecious cancerous Lesion:',                         COALESCE(via_cancer_total,0)   FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.4. 1', '15 - 19 years',                                       COALESCE(via_cancer_u19,0)     FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.4. 2', '20 - 24 years',                                       COALESCE(via_cancer_u24,0)     FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.4. 3', '25 - 29 years',                                       COALESCE(via_cancer_u29,0)     FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.4. 4', '30 - 49 years',                                       COALESCE(via_cancer_u49,0)     FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.4. 5', '>= 50 years',                                         COALESCE(via_cancer_o50,0)     FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.5',    'HPV DNA test positive:',                               COALESCE(hpv_pos_total,0)      FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.5. 1', '15 - 19 years',                                       COALESCE(hpv_pos_u19,0)        FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.5. 2', '20 - 24 years',                                       COALESCE(hpv_pos_u24,0)        FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.5. 3', '25 - 29 years',                                       COALESCE(hpv_pos_u29,0)        FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.5. 4', '30 - 49 years',                                       COALESCE(hpv_pos_u49,0)        FROM scrn_agg
UNION ALL SELECT 'HIV_CXCA_SCRN.2.5. 5', '>= 50 years',                                         COALESCE(hpv_pos_o50,0)        FROM scrn_agg;
END //

DELIMITER ;