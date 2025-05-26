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
                         ready_for_cervical_cancer_screening                Accepted
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
                              client_id,
                              follow_up_date ,
                              follow_up_status,
                              ROW_NUMBER()      OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                       from FollowUP
                       where follow_up_date <= REPORT_END_DATE),
     latest_follow_up as (select * from tmp_latest_follow_up where row_num = 1 and follow_up_status NOT IN ('Dead','Transferred out')),

     tmp_prev_screening as (select FollowUP.encounter_id,
                                   FollowUP.client_id,
                                   FollowUP.follow_up_date ,
                                   FollowUP.follow_up_status,
                                   ROW_NUMBER()      OVER (PARTITION BY FollowUP.client_id ORDER BY FollowUP.follow_up_date DESC, FollowUP.encounter_id DESC) AS row_num
                            from FollowUP
                            join tmp_latest_follow_up on FollowUP.client_id = tmp_latest_follow_up.client_id
                            where   screening_status = 'Cervical cancer screening performed'
     ),
    prev_screening as ( select * from tmp_prev_screening where row_num=1),
    cx_base_clients as (
        select latest_follow_up.*,
               prev_screening.follow_up_date as previous_screening_follow_up_date
        from latest_follow_up
        left join prev_screening on prev_screening.client_id = latest_follow_up.client_id
    )

