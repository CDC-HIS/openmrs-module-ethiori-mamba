DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_cxca_rx_query_v2;

CREATE PROCEDURE  sp_fact_hmis_cxca_rx_query_v2(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
WITH FollowUp AS (SELECT encounter_id,
                         client_id,
                         cervical_cancer_screening_status               AS cx_ca_screening_status,
                         cervical_cancer_screening_method_strategy      AS screening_type,
                         via_screening_result,
                         hpv_dna_screening_result,
                         follow_up_date_followup_                       AS follow_up_date,
                         date_hpv_test_was_done                         AS hpv_date,
                         date_visual_inspection_of_the_cervi            AS via_date,
                         treatment_of_precancerous_lesions_of_the_cervix AS treatment_of_precancerous_lesions,
                         treatment_start_date
                  FROM tmp_hmis_follow_up),
     tmp_cx_rx as (select *,
                          ROW_NUMBER() over (PARTITION BY client_id ORDER BY treatment_start_date DESC, encounter_id DESC) as row_num
                   from FollowUp
                   where treatment_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),

     cx_rx as (select tmp_cx_rx.*,
                      client.date_of_birth,
                      client.sex
               from tmp_cx_rx
                        left join mamba_dim_client client on tmp_cx_rx.client_id = client.client_id
               where row_num = 1),
     rx_agg AS (
         SELECT
             COUNT(*) AS Value,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Cryosurgery of lesion of cervix'                            THEN 1 ELSE 0 END) AS cryo_total,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Cryosurgery of lesion of cervix'          AND age BETWEEN 15 AND 19 THEN 1 ELSE 0 END) AS cryo_u19,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Cryosurgery of lesion of cervix'          AND age BETWEEN 20 AND 24 THEN 1 ELSE 0 END) AS cryo_u24,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Cryosurgery of lesion of cervix'          AND age BETWEEN 25 AND 29 THEN 1 ELSE 0 END) AS cryo_u29,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Cryosurgery of lesion of cervix'          AND age BETWEEN 30 AND 49 THEN 1 ELSE 0 END) AS cryo_u49,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Cryosurgery of lesion of cervix'          AND age >= 50              THEN 1 ELSE 0 END) AS cryo_o50,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Loop electrosurgical excision procedure of cervix'                 THEN 1 ELSE 0 END) AS leep_total,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Loop electrosurgical excision procedure of cervix' AND age BETWEEN 15 AND 19 THEN 1 ELSE 0 END) AS leep_u19,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Loop electrosurgical excision procedure of cervix' AND age BETWEEN 20 AND 24 THEN 1 ELSE 0 END) AS leep_u24,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Loop electrosurgical excision procedure of cervix' AND age BETWEEN 25 AND 29 THEN 1 ELSE 0 END) AS leep_u29,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Loop electrosurgical excision procedure of cervix' AND age BETWEEN 30 AND 49 THEN 1 ELSE 0 END) AS leep_u49,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Loop electrosurgical excision procedure of cervix' AND age >= 50              THEN 1 ELSE 0 END) AS leep_o50,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Thermocauterization of cervix'                                     THEN 1 ELSE 0 END) AS thermo_total,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Thermocauterization of cervix'            AND age BETWEEN 15 AND 19 THEN 1 ELSE 0 END) AS thermo_u19,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Thermocauterization of cervix'            AND age BETWEEN 20 AND 24 THEN 1 ELSE 0 END) AS thermo_u24,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Thermocauterization of cervix'            AND age BETWEEN 25 AND 29 THEN 1 ELSE 0 END) AS thermo_u29,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Thermocauterization of cervix'            AND age BETWEEN 30 AND 49 THEN 1 ELSE 0 END) AS thermo_u49,
             SUM(CASE WHEN treatment_of_precancerous_lesions = 'Thermocauterization of cervix'            AND age >= 50              THEN 1 ELSE 0 END) AS thermo_o50
         FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age FROM cx_rx) t
     )
SELECT 'HIV_CXCA_RX'  AS S_NO, 'Treatment of precancerous cervical lesion'        AS Activity, Value        FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.1',   'Treatment with Cryotherapy',                  COALESCE(cryo_total,0)   FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.1. 1','15 - 19 years',                               COALESCE(cryo_u19,0)     FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.1. 2','20 - 24 years',                               COALESCE(cryo_u24,0)     FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.1. 3','25 - 29 years',                               COALESCE(cryo_u29,0)     FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.1. 4','30 - 49 years',                               COALESCE(cryo_u49,0)     FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.1. 5','>= 50 years',                                 COALESCE(cryo_o50,0)     FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.2',   'Treatment with LEEP',                          COALESCE(leep_total,0)   FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.2. 1','15 - 19 years',                               COALESCE(leep_u19,0)     FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.2. 2','20 - 24 years',                               COALESCE(leep_u24,0)     FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.2. 3','25 - 29 years',                               COALESCE(leep_u29,0)     FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.2. 4','30 - 49 years',                               COALESCE(leep_u49,0)     FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.2. 5','>= 50 years',                                 COALESCE(leep_o50,0)     FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.3',   'Treatment with Thermal Ablation/Thermocoagulation', COALESCE(thermo_total,0) FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.3. 1','15 - 19 years',                               COALESCE(thermo_u19,0)   FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.3. 2','20 - 24 years',                               COALESCE(thermo_u24,0)   FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.3. 3','25 - 29 years',                               COALESCE(thermo_u29,0)   FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.3. 4','30 - 49 years',                               COALESCE(thermo_u49,0)   FROM rx_agg
UNION ALL SELECT 'HIV_CXCA_RX.3. 5','>= 50 years',                                 COALESCE(thermo_o50,0)   FROM rx_agg;
END //

DELIMITER ;