DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_cxca_scrn_query;

CREATE PROCEDURE sp_fact_line_list_cxca_scrn_query(  IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
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
                         cytology_sample_collection_date           as cytology_date,
                         follow_up_status,
                         treatment_end_date,
                         antiretroviral_art_dispensed_dose_i       as dose_days,
                         art_antiretroviral_start_date             as art_start_date,
                         cytology_result,
                         purpose_for_visit_cervical_screening      as visit_type,
                         hpv_dna_result_received_date              as hpv_received_date,
                         date_cytology_result_received             as cytology_received_date
                  FROM mamba_flat_encounter_follow_up follow_up
                           LEFT JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                     ON follow_up.encounter_id = follow_up_1.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                     ON follow_up.encounter_id = follow_up_2.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                     ON follow_up.encounter_id = follow_up_3.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                     ON follow_up.encounter_id = follow_up_4.encounter_id),
     tmp_latest_follow_up AS (SELECT client_id,
                                     follow_up_date,
                                     encounter_id,
                                     follow_up_status,
                                     treatment_end_date,
                                     dose_days,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND art_start_date IS NOT NULL
         --  AND follow_up_date <= ?
     ),

     currently_on_art AS (select *
                          from tmp_latest_follow_up
                          where row_num = 1
                            AND follow_up_status in ('Alive', 'Restart medication')),
     tmp_cx_screened as (select FollowUp.*,
                                ROW_NUMBER() over (PARTITION BY FollowUp.client_id ORDER BY via_date DESC,hpv_received_date DESC,cytology_received_date DESC , FollowUp.encounter_id DESC) as row_num
                         from FollowUp
                                  join currently_on_art on FollowUp.client_id = currently_on_art.client_id
                         where cx_ca_screening_status = 'Cervical cancer screening performed'
                           and ((via_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                                    -- AND screening_type != 'Human Papillomavirus test'
                                    ) OR
                                (hpv_received_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE) OR (cytology_received_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE))),
     cx_screened as (select tmp_cx_screened.*,
                            CASE
                                WHEN screening_type = 'Human Papillomavirus test' and
                                     hpv_dna_screening_result = 'Negative' then 'Cervical Cancer screen: Negative'
                                WHEN screening_type = 'Human Papillomavirus test' and
                                     hpv_dna_screening_result = 'Positive' then 'Cervical Cancer screen: Positive'
                                    #  WHEN screening_type='Human Papillomavirus test' and hpv_dna_screening_result ='Unknown' then 'Cervical Cancer screen: Suspected Cancer'

                                WHEN screening_type = 'Visual Inspection of the Cervix
with Acetic Acid (VIA)' and via_screening_result = 'VIA negative' then 'Cervical Cancer screen: Negative'
                                WHEN screening_type = 'Visual Inspection of the Cervix
with Acetic Acid (VIA)' and
                                     (via_screening_result = 'VIA positive: eligible for cryo/thermo-coagulation' or
                                      via_screening_result = 'VIA positive: eligible for cryo/thermo-coagula')
                                    then 'Cervical Cancer screen: Positive'
                                WHEN screening_type = 'Visual Inspection of the Cervix
with Acetic Acid (VIA)' and via_screening_result = 'VIA positive: non-eligible for cryo/thermo-coagulation'
                                    then 'Cervical Cancer screen: Positive'
                                WHEN screening_type = 'Visual Inspection of the Cervix
with Acetic Acid (VIA)' and via_screening_result = 'Suspicious for cervical cancer'
                                    then 'Cervical Cancer screen: Suspected Cancer'
                                    #            WHEN screening_type='Visual Inspection of the Cervix with Acetic Acid (VIA)' and via_screening_result = 'Unknown screening result'
                                    #                then 'Cervical Cancer screen: Suspected Cancer'
                                WHEN screening_type = 'Cytology' and
                                     (cytology_result = 'Negative' OR cytology_result = 'ASCUS')
                                    THEN 'Cervical Cancer screen: Negative'
                                WHEN screening_type = 'Cytology' and cytology_result = '> Ascus'
                                    THEN 'Cervical Cancer screen: Positive'
                                END                                             AS screening_result,
                            client.uan,
                            client.mrn,
                            client.sex,
                            client.date_of_birth,
                            (SELECT datim_agegroup
                             from mamba_dim_agegroup
                             where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) = age) as fine_age_group,
                            (SELECT normal_agegroup
                             from mamba_dim_agegroup
                             where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) = age) as coarse_age_group
                     from tmp_cx_screened
                              join mamba_dim_client client
                                   on tmp_cx_screened.client_id = client.client_id
                     where row_num = 1
                       and TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) >= 15
                       and not (screening_type = 'Human Papillomavirus test' and hpv_dna_screening_result = 'Positive' and via_screening_result is null )
     )
select * from cx_screened;

END //

DELIMITER ;