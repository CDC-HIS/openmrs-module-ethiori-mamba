WITH FollowUP AS (SELECT follow_up.encounter_id,
                         follow_up.client_id,
                         follow_up_date_followup_                        as follow_up_date,
                         follow_up_status,
                         treatment_end_date                              as art_end_date,
                         hpv_dna_result_received_date,
                         date_cytology_result_received,
                         next_follow_up_screening_date                   AS ccs_next_date,
                         cervical_cancer_screening_status                AS screening_status,
                         hpv_dna_screening_result                        AS ccs_hpv_result,
                         cytology_result                                 AS cytology_result,
                         via_screening_result                            AS ccs_via_result,
                         via_done_,
                         date_visual_inspection_of_the_cervi             AS date_via_result,
                         treatment_start_date                            AS ccs_treat_received_date,
                         colposcopy_of_cervix_findings                   AS colposcopy_exam_finding,
                         colposcopy_exam_date,
                         purpose_for_visit_cervical_screening            as screening_type,
                         cervical_cancer_screening_method_strategy       as screening_method,
                         hpv_subtype,
                         date_hpv_test_was_done,
                         cytology_sample_collection_date,
                         biopsy_sample_collected_date,
                         biopsy_result_received_date,
                         biopsy_result,
                         treatment_of_precancerous_lesions_of_the_cervix as CCS_Precancerous_Treat,
                         confirmed_cervical_cancer_cases_bas,
                         referral_or_linkage_status,
                         reason_for_referral_cacx,
                         date_client_served_in_the_referred_,
                         date_client_arrived_in_the_referred,
                         date_patient_referred_out,
                         prep_offered,
                         weight_text_,
                         art_antiretroviral_start_date                   as art_start_date,
                         next_visit_date,
                         regimen,
                         antiretroviral_art_dispensed_dose_i             as dose_days,
                         pre_test_counselling_for_cervical_c                CCaCounsellingGiven,
                         ready_for_cervical_cancer_screening                Accepted,
                         eligible_for_cxca_screening,
                         reason_for_not_being_eligible,
                         other_reason_for_not_being_eligible_for_cxca
                  FROM mamba_flat_encounter_follow_up follow_up
                           left join mamba_flat_encounter_follow_up_1 follow_up_1
                                     on follow_up.encounter_id = follow_up_1.encounter_id
                           left join mamba_flat_encounter_follow_up_2 follow_up_2
                                     on follow_up.encounter_id = follow_up_2.encounter_id
                           left join mamba_flat_encounter_follow_up_3 follow_up_3
                                     on follow_up.encounter_id = follow_up_3.encounter_id
                           left join mamba_flat_encounter_follow_up_4 follow_up_4
                                     on follow_up.encounter_id = follow_up_4.encounter_id),
     tmp_latest_follow_up AS (select encounter_id,
                                     FollowUP.client_id,
                                     sex,
                                     date_of_birth,
                                     follow_up_date,
                                     follow_up_status,
                                     eligible_for_cxca_screening,
                                     reason_for_not_being_eligible,
                                     other_reason_for_not_being_eligible_for_cxca,
                                     ccs_via_result,
                                     screening_status,
                                     ROW_NUMBER() OVER (PARTITION BY FollowUP.client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              from FollowUP
                                       join mamba_dim_client client on FollowUP.client_id = client.client_id
                              where follow_up_date <= ?),
     latest_follow_up as (select *
                          from tmp_latest_follow_up
                          where row_num = 1
                            and follow_up_status NOT IN ('Dead', 'Transferred out')
                            and sex = 'Female'
                            and TIMESTAMPDIFF(YEAR, date_of_birth, ?) BETWEEN 25 AND 65),

     tmp_prev_screening as (select *,
                                   ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS scrn_row_num
                            from tmp_latest_follow_up
                            where screening_status = 'Cervical cancer screening performed'),
     prev_screening as (select * from tmp_prev_screening where scrn_row_num = 1),
     cx_base_clients as (select latest_follow_up.*,
                                prev_screening.follow_up_date as previous_screening_follow_up_date,
                                CASE
                                    WHEN prev_screening.client_id IS NULL AND
                                         latest_follow_up.eligible_for_cxca_screening = 'Yes'
                                        THEN 'Eligible'
                                    WHEN prev_screening.client_id IS NULL AND
                                         latest_follow_up.eligible_for_cxca_screening = 'No'
                                        THEN
                                        CONCAT('Not Eligible (',
                                               COALESCE(latest_follow_up.reason_for_not_being_eligible,
                                                        latest_follow_up.other_reason_for_not_being_eligible_for_cxca),
                                               ')')
                                    WHEN prev_screening.client_id IS NULL AND
                                         latest_follow_up.eligible_for_cxca_screening IS NULL
                                        THEN 'Eligible Never Screened/Assessed'
                                    WHEN prev_screening.client_id IS NOT NULL AND
                                         prev_screening.ccs_via_result = 'Negative result'
                                        THEN 'Eligible (Ne Rescreening)'
                                    WHEN prev_screening.client_id IS NOT NULL AND
                                         (prev_screening.ccs_via_result =
                                          'VIA positive: eligible for cryo/thermo-coagulation' or
                                          prev_screening.ccs_via_result =
                                          'VIA positive: eligible for cryo/thermo-coagula' or
                                          prev_screening.ccs_via_result =
                                          'VIA positive: non-eligible for cryo/thermo-coagulation' or
                                          prev_screening.ccs_via_result = 'Suspicious for cervical cancer' or
                                          prev_screening.ccs_via_result = 'suspected cervical cancer')
                                        THEN 'Eligible (Ne Rescreening)'
                                    END                       AS EligibilityStatus


                         from latest_follow_up
                                  left join prev_screening on prev_screening.client_id = latest_follow_up.client_id)

