WITH FollowUP AS (SELECT follow_up.encounter_id,
                         follow_up.client_id,
                         follow_up_date_followup_                        as follow_up_date,
                         follow_up_status,
                         treatment_end_date                              as art_end_date,
                         hpv_dna_result_received_date,
                         date_cytology_result_received                   as cytology_result_date,
                         next_follow_up_screening_date                   AS ccs_next_date,
                         cervical_cancer_screening_status                AS screening_status,
                         hpv_dna_screening_result                        AS ccs_hpv_result,
                         cytology_result                                 AS cytology_result,
                         via_screening_result                            AS ccs_via_result,
                         via_done_,
                         date_visual_inspection_of_the_cervi             AS via_date,
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
                                     via_date,
                                     ccs_treat_received_date,
                                     date_patient_referred_out,
                                     hpv_dna_result_received_date,
                                     cytology_result,
                                     cytology_sample_collection_date,
                                     cytology_result_date,
                                     ccs_hpv_result,
                                     screening_method,
                                     screening_type,
                                     ccs_next_date,
                                     colposcopy_exam_finding,
                                     hpv_subtype,
                                     date_hpv_test_was_done,
                                     colposcopy_exam_date,
                                     biopsy_sample_collected_date,
                                     biopsy_result_received_date,
                                     biopsy_result,
                                     CCS_Precancerous_Treat,
                                     confirmed_cervical_cancer_cases_bas,
                                     REFERRAL_OR_LINKAGE_STATUS,
                                     reason_for_referral_cacx,
                                     date_client_arrived_in_the_referred,
                                     date_client_served_in_the_referred_,
                                     CCaCounsellingGiven,
                                     Accepted,
                                     ROW_NUMBER() OVER (PARTITION BY FollowUP.client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              from FollowUP
                                       join mamba_dim_client client on FollowUP.client_id = client.client_id
                              where follow_up_date <= '2024-01-01'),
     latest_follow_up as (select *
                          from tmp_latest_follow_up
                          where row_num = 1
                            and follow_up_status NOT IN ('Dead', 'Transferred out')
                            and sex = 'Female'
                            and TIMESTAMPDIFF(YEAR, date_of_birth, '2024-01-01') BETWEEN 25 AND 65),

     tmp_prev_screening as (select *,
                                   ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS scrn_row_num
                            from tmp_latest_follow_up
                            where screening_status = 'Cervical cancer screening performed'),
     prev_screening as (select * from tmp_prev_screening where scrn_row_num = 1),
     cx_base_clients as (select latest_follow_up.*,
                                prev_screening.follow_up_date as previous_screening_follow_up_date,
                                CASE
                                    -- No previous screening and eligible for cxca screening
                                    WHEN prev_screening.client_id IS NULL AND
                                         latest_follow_up.eligible_for_cxca_screening = 'Yes'
                                        THEN 'Eligible Labeled by User'
                                    -- No previous screening and not eligible for cxca screening
                                    WHEN prev_screening.client_id IS NULL AND
                                         latest_follow_up.eligible_for_cxca_screening = 'No'
                                        THEN
                                        CONCAT('Not Eligible ',
                                               COALESCE(latest_follow_up.reason_for_not_being_eligible,
                                                        latest_follow_up.other_reason_for_not_being_eligible_for_cxca))
                                    -- No previous screening and eligibility not documented
                                    WHEN prev_screening.client_id IS NULL AND
                                         latest_follow_up.eligible_for_cxca_screening IS NULL
                                        THEN 'Eligible Never Screened/Assessed'

                                    -- VIA Negative and prev via date > 730 days
                                    WHEN (TIMESTAMPDIFF(DAY, prev_screening.via_date, '2024-01-01')) > 730 AND
                                         prev_screening.ccs_via_result = 'VIA negative'
                                        THEN 'Eligible Needs Re-Screening'

                                    -- VIA Positive or suspicious 365 days since treatment or referral
                                    WHEN (prev_screening.ccs_via_result =
                                          'VIA positive: eligible for cryo/thermo-coagula' or
                                          prev_screening.ccs_via_result =
                                          'VIA positive: non-eligible for cryo/thermo-coagula' or
                                          prev_screening.ccs_via_result = 'suspected cervical cancer')
#                                         AND (prev_screening.ccs_treat_received_date <= '2024-01-01' or
#                                              prev_screening.date_patient_referred_out <= '2024-01-01')
                                        AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                            '2024-01-01')) > 365 or
                                             (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                            '2024-01-01')) > 365)
                                        THEN 'Eligible Post Treatment/Referral Follow-Up'

                                    -- VIA Positive or suspicious and no treatment and not referred
                                    WHEN (prev_screening.ccs_via_result =
                                          'VIA positive: eligible for cryo/thermo-coagula' or
                                          prev_screening.ccs_via_result =
                                          'VIA positive: non-eligible for cryo/thermo-coagula' or
                                          prev_screening.ccs_via_result = 'suspected cervical cancer')
                                        AND (prev_screening.ccs_treat_received_date is null
                                            or prev_screening.ccs_treat_received_date > '2024-01-01')
                                             and
                                         (prev_screening.date_patient_referred_out is null
                                             or prev_screening.date_patient_referred_out > '2024-01-01')
                                        THEN 'Eligible Treatment Not Received or Not Referred'

                                    -- VIA Unknown screening result
                                    WHEN prev_screening.ccs_via_result = 'Unknown'
                                        THEN 'Eligible Unknown VIA Screening Result'

                                    -- HPV negative and previous hpv date > 1095
                                    WHEN
                                         -- prev_screening.ccs_via_result is null and
                                         prev_screening.ccs_hpv_result = 'Negative result' and
                                         TIMESTAMPDIFF(DAY, prev_screening.hpv_dna_result_received_date,
                                                       '2024-01-01') > 1095
                                        THEN 'Eligible Needs Re-Screening'
                                    -- HPV Positive and no via done
                                    WHEN
                                         -- prev_screening.ccs_via_result is null and
                                         prev_screening.ccs_hpv_result = 'Positive'
                                        THEN 'Eligible Needs VIA Triage'
                                    -- HPV date unknown
                                    WHEN
                                         -- prev_screening.ccs_via_result is null and
                                         prev_screening.ccs_hpv_result = 'Unknown'
                                        THEN 'Eligible Unknown HPV Screening Result'
                                    -- Cytology negative result
                                    WHEN
                                         -- prev_screening.ccs_via_result is null and
                                         -- prev_screening.ccs_hpv_result is null and
                                         prev_screening.cytology_result = 'Negative result' and
                                         TIMESTAMPDIFF(DAY, prev_screening.cytology_result_date, '2024-01-01') > 1095
                                        THEN 'Eligible Needs Re-Screening'
                                    -- Cytology ASCUS, hpv positive, colposcopy normal
                                    WHEN
                                         -- prev_screening.ccs_via_result is null and
                                         prev_screening.ccs_hpv_result = 'Positive' and
                                         prev_screening.cytology_result =
                                         'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                        and prev_screening.colposcopy_exam_finding = 'Normal'
                                        and  TIMESTAMPDIFF(DAY, prev_screening.colposcopy_exam_date, '2024-01-01') > 1095
                                        THEN 'Eligible Needs Re-Screening'
                                    -- Cytology ASCUS, hpv positive, colposcopy low/high and 365 days since treatment or referral
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.ccs_hpv_result = 'Positive' and
                                        prev_screening.cytology_result =
                                        'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                            and (prev_screening.colposcopy_exam_finding = 'Low Grade' or prev_screening.colposcopy_exam_finding = 'High Grade')
#                                             AND (prev_screening.ccs_treat_received_date <= '2024-01-01' or
#                                                  prev_screening.date_patient_referred_out <= '2024-01-01')
                                            AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                                '2024-01-01')) > 365 or
                                                 (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                                '2024-01-01')) > 365)
                                        THEN 'Eligible Post Treatment/Referral Follow-Up'
                                    -- Cytology ASCUS, hpv positive, colposcopy low/high and no referral or treatment
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.ccs_hpv_result = 'Positive' and
                                        prev_screening.cytology_result =
                                        'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                            and (prev_screening.colposcopy_exam_finding = 'Low Grade' or prev_screening.colposcopy_exam_finding = 'High Grade')
                                            #                                             AND (prev_screening.ccs_treat_received_date <= '2024-01-01' or
#                                                  prev_screening.date_patient_referred_out <= '2024-01-01')
                                            AND (prev_screening.ccs_treat_received_date is null
                                            or prev_screening.ccs_treat_received_date > '2024-01-01')
                                            and
                                        (prev_screening.date_patient_referred_out is null
                                            or prev_screening.date_patient_referred_out > '2024-01-01')
                                        THEN 'Eligible Treatment Not Received or Not Referred'

                                    -- Cytology ASCUS, hpv positive, colposcopy not done
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.ccs_hpv_result = 'Positive' and
                                        prev_screening.cytology_result =
                                        'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                            and prev_screening.colposcopy_exam_finding is null
                                        THEN 'Eligible Needs Colposcopy Test'

                                    -- Cytology ASCUS, hpv Negative
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.ccs_hpv_result = 'Negative result' and
                                        prev_screening.cytology_result =
                                        'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                            and  TIMESTAMPDIFF(DAY, prev_screening.hpv_dna_result_received_date, '2024-01-01') > 1095
                                        THEN 'Eligible (Needs HPV Triage)'

                                    -- Cytology ASCUS, hpv not done
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.ccs_hpv_result is null and
                                        prev_screening.cytology_result =
                                        'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                        THEN 'Eligible (Needs HPV Triage)'


                                    -- Cytology > ASCUS, colposcopy normal
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.cytology_result =
                                        '> Ascus'
                                            and prev_screening.colposcopy_exam_finding = 'Normal'
                                            and  TIMESTAMPDIFF(DAY, prev_screening.colposcopy_exam_date, '2024-01-01') > 1095
                                        THEN 'Eligible Needs Re-Screening'
                                    -- Cytology > ASCUS, colposcopy low/high and 365 days since treatment or referral
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.cytology_result =
                                        '> Ascus'
                                            and prev_screening.colposcopy_exam_finding = 'Normal'
                                            and (prev_screening.colposcopy_exam_finding = 'Low Grade' or prev_screening.colposcopy_exam_finding = 'High Grade')
                                            AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                                '2024-01-01')) > 365 or
                                                 (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                                '2024-01-01')) > 365)
                                        THEN 'Eligible Post Treatment/Referral Follow-Up'
                                    -- Cytology > ASCUS, colposcopy low/high and no treatment or referral
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.cytology_result =
                                        '> Ascus'
                                            and prev_screening.colposcopy_exam_finding = 'Normal'
                                            and (prev_screening.colposcopy_exam_finding = 'Low Grade' or prev_screening.colposcopy_exam_finding = 'High Grade')
                                            AND (prev_screening.ccs_treat_received_date is null
                                            or prev_screening.ccs_treat_received_date > '2024-01-01')
                                            and
                                        (prev_screening.date_patient_referred_out is null
                                            or prev_screening.date_patient_referred_out > '2024-01-01')
                                        THEN 'Eligible Treatment Not Received or Not Referred'

                                    -- Cytology > ASCUS, colposcopy not done
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.cytology_result =
                                        '> Ascus'
                                            and prev_screening.colposcopy_exam_finding is null
                                    THEN 'Eligible Needs Colposcopy Test'

                                    END                       AS EligibilityStatus,

                                CASE
                                    -- No previous screening and eligible for cxca screening
                                    WHEN prev_screening.client_id IS NULL AND
                                         latest_follow_up.eligible_for_cxca_screening = 'Yes'
                                        THEN '2024-01-01'
                                    -- No previous screening and eligibility not documented
                                    WHEN prev_screening.client_id IS NULL AND
                                         latest_follow_up.eligible_for_cxca_screening IS NULL
                                        THEN '2024-01-01'

                                    -- VIA Negative and prev via date > 730 days
                                    WHEN (TIMESTAMPDIFF(DAY, prev_screening.via_date, '2024-01-01')) > 730 AND
                                         prev_screening.ccs_via_result = 'VIA negative'
                                        THEN DATE_ADD(prev_screening.via_date, INTERVAL 730 DAY )

                                    -- VIA Positive or suspicious 365 days since treatment or referral
                                    WHEN (prev_screening.ccs_via_result =
                                          'VIA positive: eligible for cryo/thermo-coagula' or
                                          prev_screening.ccs_via_result =
                                          'VIA positive: non-eligible for cryo/thermo-coagula' or
                                          prev_screening.ccs_via_result = 'suspected cervical cancer')
                                        #                                         AND (prev_screening.ccs_treat_received_date <= '2024-01-01' or
#                                              prev_screening.date_patient_referred_out <= '2024-01-01')
                                        AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                            '2024-01-01')) > 365 or
                                             (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                            '2024-01-01')) > 365)
                                        THEN DATE_ADD(COALESCE(prev_screening.ccs_treat_received_date,prev_screening.date_patient_referred_out), INTERVAL 730 DAY )

                                    -- VIA Positive or suspicious and no treatment and not referred
                                    WHEN (prev_screening.ccs_via_result =
                                          'VIA positive: eligible for cryo/thermo-coagula' or
                                          prev_screening.ccs_via_result =
                                          'VIA positive: non-eligible for cryo/thermo-coagula' or
                                          prev_screening.ccs_via_result = 'suspected cervical cancer')
                                        AND (prev_screening.ccs_treat_received_date is null
                                            or prev_screening.ccs_treat_received_date > '2024-01-01')
                                        and
                                         (prev_screening.date_patient_referred_out is null
                                             or prev_screening.date_patient_referred_out > '2024-01-01')
                                        THEN '2024-01-01'

                                    -- VIA Unknown screening result
                                    WHEN prev_screening.ccs_via_result = 'Unknown'
                                        THEN '2024-01-01'

                                    -- HPV negative and previous hpv date > 1095
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.ccs_hpv_result = 'Negative result' and
                                        TIMESTAMPDIFF(DAY, prev_screening.hpv_dna_result_received_date,
                                                      '2024-01-01') > 1095
                                        THEN DATE_ADD(prev_screening.hpv_dna_result_received_date, INTERVAL 1095 DAY )
                                    -- HPV Positive and no via done
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.ccs_hpv_result = 'Positive'
                                        THEN '2024-01-01'
                                    -- HPV date unknown
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.ccs_hpv_result = 'Unknown'
                                        THEN '2024-01-01'
                                    -- Cytology negative result
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        -- prev_screening.ccs_hpv_result is null and
                                        prev_screening.cytology_result = 'Negative result' and
                                        TIMESTAMPDIFF(DAY, prev_screening.cytology_result_date, '2024-01-01') > 1095
                                        THEN DATE_ADD(prev_screening.cytology_result_date, INTERVAL 1095 DAY )
                                    -- Cytology ASCUS, hpv positive, colposcopy normal
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.ccs_hpv_result = 'Positive' and
                                        prev_screening.cytology_result =
                                        'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                            and prev_screening.colposcopy_exam_finding = 'Normal'
                                            and  TIMESTAMPDIFF(DAY, prev_screening.colposcopy_exam_date, '2024-01-01') > 1095
                                        THEN DATE_ADD(prev_screening.colposcopy_exam_date, INTERVAL 1095 DAY )
                                    -- Cytology ASCUS, hpv positive, colposcopy low/high and 365 days since treatment or referral
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.ccs_hpv_result = 'Positive' and
                                        prev_screening.cytology_result =
                                        'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                            and (prev_screening.colposcopy_exam_finding = 'Low Grade' or prev_screening.colposcopy_exam_finding = 'High Grade')
                                            #                                             AND (prev_screening.ccs_treat_received_date <= '2024-01-01' or
#                                                  prev_screening.date_patient_referred_out <= '2024-01-01')
                                            AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                                '2024-01-01')) > 365 or
                                                 (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                                '2024-01-01')) > 365)
                                        THEN DATE_ADD(COALESCE(prev_screening.ccs_treat_received_date,prev_screening.date_patient_referred_out), INTERVAL 365 DAY )
                                    -- Cytology ASCUS, hpv positive, colposcopy low/high and no referral or treatment
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.ccs_hpv_result = 'Positive' and
                                        prev_screening.cytology_result =
                                        'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                            and (prev_screening.colposcopy_exam_finding = 'Low Grade' or prev_screening.colposcopy_exam_finding = 'High Grade')
                                            #                                             AND (prev_screening.ccs_treat_received_date <= '2024-01-01' or
#                                                  prev_screening.date_patient_referred_out <= '2024-01-01')
                                            AND (prev_screening.ccs_treat_received_date is null
                                            or prev_screening.ccs_treat_received_date > '2024-01-01')
                                            and
                                        (prev_screening.date_patient_referred_out is null
                                            or prev_screening.date_patient_referred_out > '2024-01-01')
                                        THEN '2024-01-01'

                                    -- Cytology ASCUS, hpv positive, colposcopy not done
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.ccs_hpv_result = 'Positive' and
                                        prev_screening.cytology_result =
                                        'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                            and prev_screening.colposcopy_exam_finding is null
                                        THEN '2024-01-01'

                                    -- Cytology ASCUS, hpv Negative
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.ccs_hpv_result = 'Negative result' and
                                        prev_screening.cytology_result =
                                        'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                            and  TIMESTAMPDIFF(DAY, prev_screening.hpv_dna_result_received_date, '2024-01-01') > 1095
                                        THEN DATE_ADD(prev_screening.hpv_dna_result_received_date, INTERVAL 1095 DAY )

                                    -- Cytology ASCUS, hpv not done
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.ccs_hpv_result is null and
                                        prev_screening.cytology_result =
                                        'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                        THEN '2024-01-01'


                                    -- Cytology > ASCUS, colposcopy normal
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.cytology_result =
                                        '> Ascus'
                                            and prev_screening.colposcopy_exam_finding = 'Normal'
                                            and  TIMESTAMPDIFF(DAY, prev_screening.colposcopy_exam_date, '2024-01-01') > 1095
                                        THEN DATE_ADD(prev_screening.colposcopy_exam_date, INTERVAL 1095 DAY )
                                    -- Cytology > ASCUS, colposcopy low/high and 365 days since treatment or referral
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.cytology_result =
                                        '> Ascus'
                                            and prev_screening.colposcopy_exam_finding = 'Normal'
                                            and (prev_screening.colposcopy_exam_finding = 'Low Grade' or prev_screening.colposcopy_exam_finding = 'High Grade')
                                            AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                                '2024-01-01')) > 365 or
                                                 (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                                '2024-01-01')) > 365)
                                        THEN DATE_ADD(COALESCE(prev_screening.ccs_treat_received_date,prev_screening.date_patient_referred_out), INTERVAL 365 DAY )
                                    -- Cytology > ASCUS, colposcopy low/high and no treatment or referral
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.cytology_result =
                                        '> Ascus'
                                            and prev_screening.colposcopy_exam_finding = 'Normal'
                                            and (prev_screening.colposcopy_exam_finding = 'Low Grade' or prev_screening.colposcopy_exam_finding = 'High Grade')
                                            AND (prev_screening.ccs_treat_received_date is null
                                            or prev_screening.ccs_treat_received_date > '2024-01-01')
                                            and
                                        (prev_screening.date_patient_referred_out is null
                                            or prev_screening.date_patient_referred_out > '2024-01-01')
                                        THEN '2024-01-01'

                                    -- Cytology > ASCUS, colposcopy not done
                                    WHEN
                                        -- prev_screening.ccs_via_result is null and
                                        prev_screening.cytology_result =
                                        '> Ascus'
                                            and prev_screening.colposcopy_exam_finding is null
                                        THEN '2024-01-01'

                                    END                       AS EligibilityDate


                         from latest_follow_up
                                  left join prev_screening on prev_screening.client_id = latest_follow_up.client_id)
select EligibilityDate, LEFT(EligibilityStatus,8) as EligibilityStatus, SUBSTR(EligibilityStatus,10) as EligibilityReason from cx_base_clients