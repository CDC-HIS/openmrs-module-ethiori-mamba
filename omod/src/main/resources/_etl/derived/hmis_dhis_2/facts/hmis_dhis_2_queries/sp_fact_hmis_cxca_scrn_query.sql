DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_cxca_scrn_query;

CREATE PROCEDURE  sp_fact_hmis_cxca_scrn_query(
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
                         hpv_dna_result_received_date          as hpv_received_date
                  FROM mamba_flat_encounter_follow_up follow_up
                           LEFT JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                     ON follow_up.encounter_id = follow_up_1.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                     ON follow_up.encounter_id = follow_up_2.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                     ON follow_up.encounter_id = follow_up_3.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                     ON follow_up.encounter_id = follow_up_4.encounter_id),
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
                            (via_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE AND screening_type != 'Human Papillomavirus test')
                         )
                     )
--  Cervical Cancer screening by type of test
SELECT 'HIV_CXCA_SCRN.1'                     AS S_NO,
       'Cervical Cancer screening by type of test' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
--  Cervical Cancer screening by type of test
UNION ALL
SELECT 'HIV_CXCA_SCRN.1. 1'                     AS S_NO,
       'Screened by VIA' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
--  Screened by HPV DNA
UNION ALL
SELECT 'HIV_CXCA_SCRN.1. 2'                     AS S_NO,
       'Screened by HPV DNA' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Human Papillomavirus test'
--  VIA Screening Result
UNION ALL
SELECT 'HIV_CXCA_SCRN.2'                     AS S_NO,
       'VIA Screening Result' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'

--  Normal cervix:
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.1'                     AS S_NO,
       'Normal cervix:' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and via_screening_result='VIA negative'
--  15 - 19 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.1. 1'                     AS S_NO,
       '15 - 19 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and via_screening_result='VIA negative'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
--  20 - 24 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.1. 2'                     AS S_NO,
       '20 - 24 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and via_screening_result='VIA negative'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
--  25 - 29 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.1. 3'                     AS S_NO,
       '25 - 29 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and via_screening_result='VIA negative'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
--  30 - 49 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.1. 4'                     AS S_NO,
       '30 - 49 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and via_screening_result='VIA negative'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 49
--  >= 50 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.1. 5'                     AS S_NO,
       '>= 50 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and via_screening_result='VIA negative'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50

--  Precancerous Lesion:
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.3'                     AS S_NO,
       'Precancerous Lesion:' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and (via_screening_result='VIA positive: non-eligible for cryo/thermo-coagula'
   or via_screening_result='VIA positive: eligible for cryo/thermo-coagula')
--  15 - 19 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.3. 1'                     AS S_NO,
       '15 - 19 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and (via_screening_result='VIA positive: non-eligible for cryo/thermo-coagula'
   or via_screening_result='VIA positive: eligible for cryo/thermo-coagula')
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
--  20 - 24 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.3. 2'                     AS S_NO,
       '20 - 24 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and (via_screening_result='VIA positive: non-eligible for cryo/thermo-coagula'
   or via_screening_result='VIA positive: eligible for cryo/thermo-coagula')
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
--  25 - 29 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.3. 3'                     AS S_NO,
       '25 - 29 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and (via_screening_result='VIA positive: non-eligible for cryo/thermo-coagula'
   or via_screening_result='VIA positive: eligible for cryo/thermo-coagula')
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
--  30 - 49 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.3. 4'                     AS S_NO,
       '30 - 49 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and (via_screening_result='VIA positive: non-eligible for cryo/thermo-coagula'
   or via_screening_result='VIA positive: eligible for cryo/thermo-coagula')
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 49
--  >= 50 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.3. 5'                     AS S_NO,
       '>= 50 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and (via_screening_result='VIA positive: non-eligible for cryo/thermo-coagula'
   or via_screening_result='VIA positive: eligible for cryo/thermo-coagula')
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50
--  Suspecious cancerous Lesion:
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.4'                     AS S_NO,
       'Suspecious cancerous Lesion:' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and via_screening_result='suspected cervical cancer'
--  15 - 19 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.4. 1'                     AS S_NO,
       '15 - 19 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and via_screening_result='suspected cervical cancer'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
--  20 - 24 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.4. 2'                     AS S_NO,
       '20 - 24 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and via_screening_result='suspected cervical cancer'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
--  25 - 29 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.4. 3'                     AS S_NO,
       '25 - 29 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and via_screening_result='suspected cervical cancer'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
--  30 - 49 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.4. 4'                     AS S_NO,
       '30 - 49 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and via_screening_result='suspected cervical cancer'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 49
--  >= 50 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.4. 5'                     AS S_NO,
       '>= 50 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)'
  and via_screening_result='suspected cervical cancer'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50

--  HPV DNA test positive:
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.5'                     AS S_NO,
       'HPV DNA test positive:' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Human Papillomavirus test'
  and hpv_dna_screening_result='Positive'
--  15 - 19 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.5. 1'                     AS S_NO,
       '15 - 19 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Human Papillomavirus test'
  and hpv_dna_screening_result='Positive'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
--  20 - 24 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.5. 2'                     AS S_NO,
       '20 - 24 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Human Papillomavirus test'
  and hpv_dna_screening_result='Positive'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24
--  25 - 29 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.5. 3'                     AS S_NO,
       '25 - 29 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Human Papillomavirus test'
  and hpv_dna_screening_result='Positive'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29
--  30 - 49 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.5. 4'                     AS S_NO,
       '30 - 49 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Human Papillomavirus test'
  and hpv_dna_screening_result='Positive'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 49
--  >= 50 years
UNION ALL
SELECT 'HIV_CXCA_SCRN.2.5. 5'                     AS S_NO,
       '>= 50 years' as Activity,
       COUNT(*)                          as Value
FROM cx_screened
where screening_type='Human Papillomavirus test'
  and hpv_dna_screening_result='Positive'
  and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50;
END //

DELIMITER ;