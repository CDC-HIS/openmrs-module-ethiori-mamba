DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_cxca_rx_query;

CREATE PROCEDURE  sp_fact_hmis_cxca_rx_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
WITH FollowUp as (select follow_up.encounter_id,
                         follow_up.client_id,
                         cervical_cancer_screening_status          as cx_ca_screening_status,
                         cervical_cancer_screening_method_strategy as screening_type,
                         via_screening_result,
                         hpv_dna_screening_result,
                         follow_up_date_followup_                  as follow_up_date,
                         date_hpv_test_was_done                    as hpv_date,
                         date_visual_inspection_of_the_cervi       as via_date,
                         treatment_of_precancerous_lesions_of_the_cervix as treatment_of_precancerous_lesions,
                         treatment_start_date
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
     tmp_cx_rx as (select *,
                          ROW_NUMBER() over (PARTITION BY client_id ORDER BY treatment_start_date DESC, encounter_id DESC) as row_num
                   from FollowUp
                   where treatment_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),

     cx_rx as (select tmp_cx_rx.*,
                      client.date_of_birth,
                      client.sex
               from tmp_cx_rx
                        left join mamba_dim_client client on tmp_cx_rx.client_id = client.client_id
               where row_num = 1)
--  Cervical Cancer screening by type of test
SELECT 'HIV_CXCA_RX'                     AS S_NO,
       'Treatment of precancerous cervical lesion' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
--  Treatment with Cryotherapy
UNION ALL
SELECT 'HIV_CXCA_RX.1'                     AS S_NO,
       'Treatment with Cryotherapy' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Cryosurgery of lesion of cervix'
--  15 - 19 years
UNION ALL
SELECT 'HIV_CXCA_RX.1. 1'                     AS S_NO,
       '15 - 19 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Cryosurgery of lesion of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
--  20 - 24 years
UNION ALL
SELECT 'HIV_CXCA_RX.1. 2'                     AS S_NO,
       '20 - 24 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Cryosurgery of lesion of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
--  25 - 29 years
UNION ALL
SELECT 'HIV_CXCA_RX.1. 3'                     AS S_NO,
       '25 - 29 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Cryosurgery of lesion of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
--  30 - 49 years
UNION ALL
SELECT 'HIV_CXCA_RX.1. 4'                     AS S_NO,
       '30 - 49 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Cryosurgery of lesion of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 49
--  >= 50 years
UNION ALL
SELECT 'HIV_CXCA_RX.1. 5'                     AS S_NO,
       '>= 50 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Cryosurgery of lesion of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
--  Treatment with LEEP
UNION ALL
SELECT 'HIV_CXCA_RX.2'                     AS S_NO,
       'Treatment with LEEP' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Loop electrosurgical excision procedure of cervix'
--  15 - 19 years
UNION ALL
SELECT 'HIV_CXCA_RX.2. 1'                     AS S_NO,
       '15 - 19 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Loop electrosurgical excision procedure of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
--  20 - 24 years
UNION ALL
SELECT 'HIV_CXCA_RX.2. 2'                     AS S_NO,
       '20 - 24 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Loop electrosurgical excision procedure of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
--  25 - 29 years
UNION ALL
SELECT 'HIV_CXCA_RX.2. 3'                     AS S_NO,
       '25 - 29 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Loop electrosurgical excision procedure of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
--  30 - 49 years
UNION ALL
SELECT 'HIV_CXCA_RX.2. 4'                     AS S_NO,
       '30 - 49 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Loop electrosurgical excision procedure of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 49
--  >= 50 years
UNION ALL
SELECT 'HIV_CXCA_RX.2. 5'                     AS S_NO,
       '>= 50 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Loop electrosurgical excision procedure of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
--  Treatment with Thermal Ablation/Thermocoagulation
UNION ALL
SELECT 'HIV_CXCA_RX.3'                     AS S_NO,
       'Treatment with Thermal Ablation/Thermocoagulation' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Thermocauterization of cervix'
--  15 - 19 years
UNION ALL
SELECT 'HIV_CXCA_RX.3. 1'                     AS S_NO,
       '15 - 19 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Thermocauterization of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
--  20 - 24 years
UNION ALL
SELECT 'HIV_CXCA_RX.3. 2'                     AS S_NO,
       '20 - 24 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Thermocauterization of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
--  25 - 29 years
UNION ALL
SELECT 'HIV_CXCA_RX.3. 3'                     AS S_NO,
       '25 - 29 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Thermocauterization of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
--  30 - 49 years
UNION ALL
SELECT 'HIV_CXCA_RX.3. 4'                     AS S_NO,
       '30 - 49 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Thermocauterization of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 49
--  >= 50 years
UNION ALL
SELECT 'HIV_CXCA_RX.3. 5'                     AS S_NO,
       '>= 50 years' as Activity,
       COUNT(*)                          as Value
FROM cx_rx
where treatment_of_precancerous_lesions = 'Thermocauterization of cervix'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50;
END //

DELIMITER ;