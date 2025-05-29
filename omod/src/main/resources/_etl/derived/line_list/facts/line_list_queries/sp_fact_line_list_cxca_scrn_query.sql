DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_cxca_scrn_query;

CREATE PROCEDURE sp_fact_line_list_cxca_scrn_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUp as (select follow_up.encounter_id,
                             follow_up.client_id,
                             cervical_cancer_screening_status          as cx_ca_screening_status,
                             cervical_cancer_screening_method_strategy as screening_method,
                             purpose_for_visit_cervical_screening      as type_of_screening,
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
                             date_cytology_result_received             as cytology_received_date,
                             date_counseling_given,
                             ready_for_cervical_cancer_screening,
                             date_linked_to_cervical_cancer_scre,
                             colposcopy_of_cervix_findings,
                             colposcopy_exam_date,
                             biopsy_result,
                             biopsy_result_received_date,
                             biopsy_sample_collected_date,
                             treatment_of_precancerous_lesions_of_the_cervix,
                             next_visit_date,
                             next_follow_up_screening_date,
                             regimen,
                             adherence,
                             referral_or_linkage_status,
                             referred_to_higher_level_facility,
                             reason_for_referral_cacx,
                             date_client_arrived_in_the_referred,
                             date_client_served_in_the_referred_,
                             hpv_subtype,
                             treatment_start_date,
                             date_patient_referred_out
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
                                    (hpv_received_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE) OR
                                    (cytology_received_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE))),
         cx_screened as (select tmp_cx_screened.*,
                                CASE


                                    #  WHEN screening_type='Human Papillomavirus test' and hpv_dna_screening_result ='Unknown' then 'Cervical Cancer screen: Suspected Cancer'

                                    WHEN screening_method = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' and
                                         via_screening_result = 'VIA negative' then 'Cervical Cancer screen: Negative'
                                    WHEN screening_method = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' and
                                         (via_screening_result = 'VIA positive: eligible for cryo/thermo-coagulation' or
                                          via_screening_result = 'VIA positive: eligible for cryo/thermo-coagula' or
                                          via_screening_result =
                                          'VIA positive: non-eligible for cryo/thermo-coagulation')
                                        then 'Cervical Cancer screen: Positive'
                                    WHEN screening_method = 'Visual Inspection of the Cervix with Acetic Acid (VIA)' and
                                         via_screening_result = 'Suspicious for cervical cancer' or
                                         via_screening_result = 'suspected cervical cancer'
                                        then 'Cervical Cancer screen: Suspected Cancer'

                                    WHEN screening_method = 'Human Papillomavirus test' and
                                         hpv_dna_screening_result = 'Negative result'
                                        then 'Cervical Cancer screen: Negative'
                                    WHEN screening_method = 'Human Papillomavirus test' and
                                         hpv_dna_screening_result = 'Positive'
                                        and
                                         (via_screening_result = 'VIA positive: eligible for cryo/thermo-coagulation' or
                                          via_screening_result = 'VIA positive: eligible for cryo/thermo-coagula' or
                                          via_screening_result =
                                          'VIA positive: non-eligible for cryo/thermo-coagulation')
                                        then 'Cervical Cancer screen: Positive'
                                    WHEN screening_method = 'Human Papillomavirus test' and
                                         hpv_dna_screening_result = 'Positive'
                                        and via_screening_result = 'VIA negative'
                                        then 'Cervical Cancer screen: Negative'
                                    WHEN screening_method = 'Cytology' and
                                         (cytology_result = 'Negative' OR cytology_result = 'ASCUS')
                                        THEN 'Cervical Cancer screen: Negative'
                                    WHEN screening_method = 'Cytology' and cytology_result = '> Ascus'
                                        THEN 'Cervical Cancer screen: Positive'
                                    END AS screening_result,
                                client.uan,
                                client.mrn,
                                client.patient_uuid,
                                client.sex,
                                client.date_of_birth,
                                client.patient_name
                         from tmp_cx_screened
                                  join mamba_dim_client client
                                       on tmp_cx_screened.client_id = client.client_id
                         where row_num = 1
                           and TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) >= 15
             -- and not (screening_type = 'Human Papillomavirus test' and hpv_dna_screening_result = 'Positive' and via_screening_result is null )
         )
    select patient_name                                        as `Patient Name`,
           patient_uuid                                        as `UUID`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) as `Current Age`,
           Sex,
           follow_up_date                                      as `Follow-Up Date`,
           follow_up_date                                      as `Follow-Up Date EC.`,
           art_start_date                                      as `Art Start Date`,
           art_start_date                                      as `Art Start Date EC.`,
           date_counseling_given                               as `Date Counseled For CCA`,
           date_counseling_given                               as `Date Counseled For CCA EC.`,
           ready_for_cervical_cancer_screening                 as `Date Accepted CxCa Screening`,
           ready_for_cervical_cancer_screening                 as `Date Accepted CxCa Screening EC.`,
           date_linked_to_cervical_cancer_scre                 as `Date Linked to CxCa Screening Unit`,
           date_linked_to_cervical_cancer_scre                 as `Date Linked to CxCa Screening Unit EC.`,
           type_of_screening                                   as `Type of Screening`,
           screening_method                                    as `Screening Strategy`,
           via_screening_result                                as `VIA Screening Result`,
           via_date                                            as `VIA Screening Date`,
           via_date                                            as `VIA Screening Date EC.`,
           hpv_dna_screening_result                            as `HPV DNA Screening Result`,
           hpv_date                                            as `HPV DNA Sample Collection Date`,
           hpv_date                                            as `HPV DNA Sample Collection Date EC.`,
           hpv_subtype                                            `HPV Subtype`,
           hpv_received_date                                   as `HPV DNA Result Received Date`,
           hpv_received_date                                   as `HPV DNA Result Received Date EC.`,
           cytology_result                                     as `Cytology Result`,
           cytology_date                                       as `Cytology Sample Collection Date`,
           cytology_date                                       as `Cytology Sample Collection Date EC.`,
           cytology_received_date                              as `Cytology Result Received Date`,
           cytology_received_date                              as `Cytology Result Received Date EC.`,
           colposcopy_of_cervix_findings                       as `Colposcopy Exam Finding`,
           colposcopy_exam_date                                as `Colposcopy Exam Date`,
           colposcopy_exam_date                                as `Colposcopy Exam Date EC.`,
           biopsy_result                                       as `Biopsy Result`,
           biopsy_sample_collected_date                        as `Biopsy Sample Collection Date`,
           biopsy_sample_collected_date                        as `Biopsy Sample Collection Date EC.`,
           biopsy_result_received_date                         as `Biopsy Result Received Date`,
           biopsy_result_received_date                         as `Biopsy Result Received Date EC.`,
           treatment_of_precancerous_lesions_of_the_cervix     as `Treatment for Precancerous Lesion`,
# Treatment for Confirmed CxCa
           treatment_start_date                                as `Date Treatment Received`,
           treatment_start_date                                as `Date Treatment Received EC.`,
           next_follow_up_screening_date                       as `Next Follow-up Screening Date`,
           next_follow_up_screening_date                       as `Next Follow-up Screening Date EC.`,
           referral_or_linkage_status                          as `Referral/ Linkage Status`,
           reason_for_referral_cacx                            as `Reason for Referral`,
           date_patient_referred_out                           as `Date of Referral to other HF`,
           date_patient_referred_out                           as `Date of Referral to other HF EC.`,
# Referral Confirmed Date EC.
# Feedback from the other HF
           date_client_served_in_the_referred_                 as `Date Client Served in the referred HF`,
           date_client_served_in_the_referred_                 as `Date Client Served in the referred HF EC.`,
           follow_up_status                                    as `Latest Follow-Up Status`,
           regimen                                             as `Latest Regimen`,
           dose_days                                           as `Latest ARV Dose Days`,
           adherence                                              `Latest Adherence`,
           next_visit_date                                     as `Next Visit Date`,
           next_visit_date                                     as `Next Visit Date EC.`,
           treatment_end_date                                  as `Treatment End Date`,
           treatment_end_date                                  as `Treatment End Date EC.`

    from cx_screened;

END //

DELIMITER ;