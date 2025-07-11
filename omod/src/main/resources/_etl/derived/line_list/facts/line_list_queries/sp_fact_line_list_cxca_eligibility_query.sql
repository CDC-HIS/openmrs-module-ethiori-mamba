DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_cxca_eligibility_query;

CREATE PROCEDURE sp_fact_line_list_cxca_eligibility_query(IN REPORT_END_DATE DATE)
BEGIN
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
         tmp_latest_follow_up AS (select encounter_id,
                                         FollowUP.client_id,
                                         TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE)                                                 as Age,
                                         art_start_date                                                                                      as ArtStartDate,
                                         next_visit_date,
                                         regimen,
                                         dose_days,
                                         sex,
                                         mrn,
                                         uan,
                                         patient_name,
                                         patient_uuid,
                                         date_of_birth,
                                         CCaCounsellingGiven,
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
                                         Accepted,
                                         weight_text_                                                                                        as Weight,
                                         mobile_no,
                                         phone_no,
                                         ROW_NUMBER() OVER (PARTITION BY FollowUP.client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  from FollowUP
                                           join mamba_dim_client client on FollowUP.client_id = client.client_id
                                  where follow_up_date <= REPORT_END_DATE),
         latest_follow_up as (select *
                              from tmp_latest_follow_up
                              where row_num = 1
                                and follow_up_status NOT IN ('Dead', 'Transferred out')
                                and sex = 'Female'
                                and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 65),

         tmp_prev_screening as (select *,
                                       ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS scrn_row_num
                                from tmp_latest_follow_up
                                where screening_status = 'Cervical cancer screening performed'),
         prev_screening as (select * from tmp_prev_screening where scrn_row_num = 1),
         cx_base_clients as (select latest_follow_up.*,
                                    prev_screening.follow_up_date as previous_screening_follow_up_date,
                                    CASE
                                        -- TC-02,TC-03,TC-04,TC-05 No previous screening and not eligible for cxca screening
                                        WHEN prev_screening.client_id IS NULL
                                            AND latest_follow_up.eligible_for_cxca_screening = 'No'
                                            THEN
                                            CONCAT('Not Eligible ',
                                                   COALESCE(latest_follow_up.reason_for_not_being_eligible,
                                                            latest_follow_up.other_reason_for_not_being_eligible_for_cxca))

                                        -- TC-06 User-Labeled as Eligible
                                        WHEN prev_screening.client_id IS NULL
                                            AND
                                             latest_follow_up.screening_status != 'Cervical cancer screening performed'
                                            AND latest_follow_up.eligible_for_cxca_screening = 'Yes'
                                            THEN 'Eligible Labeled by User'

                                        -- TC-07 Never screened/assessed
                                        WHEN prev_screening.client_id IS NULL
                                            AND latest_follow_up.eligible_for_cxca_screening IS NULL
                                            THEN 'Eligible Never Screened/Assessed'

                                        -- TC-08 Unknown VIA result
                                        WHEN prev_screening.ccs_via_result = 'Unknown'
                                            THEN 'Eligible Unknown VIA Screening Result'

                                        -- TC-09 VIA negative >2 years ago
                                        WHEN (TIMESTAMPDIFF(DAY, prev_screening.via_date, REPORT_END_DATE)) > 730
                                            AND prev_screening.ccs_via_result = 'VIA negative'
                                            THEN 'Eligible Needs Re-Screening'

                                        -- TC-10 VIA Positive: Eligible for Cryo/Thermocoagulation & Treatment Given/Referred
                                        WHEN (prev_screening.ccs_via_result =
                                              'VIA positive: eligible for cryo/thermo-coagula'
#                                           or prev_screening.ccs_via_result =
#                                           'VIA positive: non-eligible for cryo/thermo-coagula' or
#                                           prev_screening.ccs_via_result = 'suspected cervical cancer'
                                                 )
                                            AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                                REPORT_END_DATE)) > 365 or
                                                 (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                                REPORT_END_DATE)) > 365)
                                            AND prev_screening.biopsy_result is null
                                            THEN 'Eligible Post Treatment/Referral Follow-Up'

                                        -- TC-11 VIA Positive: Eligible for Cryo/Thermocoagulation No Treatment/Referral
                                        WHEN (prev_screening.ccs_via_result =
                                              'VIA positive: eligible for cryo/thermo-coagula'
#                                           or prev_screening.ccs_via_result =
#                                           'VIA positive: non-eligible for cryo/thermo-coagula' or
#                                           prev_screening.ccs_via_result = 'suspected cervical cancer'
                                                 )
                                            AND (prev_screening.ccs_treat_received_date is null
                                                or prev_screening.ccs_treat_received_date > REPORT_END_DATE)
                                            and
                                             (prev_screening.date_patient_referred_out is null
                                                 or prev_screening.date_patient_referred_out > REPORT_END_DATE)
                                            AND prev_screening.biopsy_result is null
                                            THEN 'Eligible Treatment Not Received or Not Referred'

                                        -- TC-12 VIA Positive & Not Eligible for Cryo/Thermocoagulation OR Suspicious for Cervical Cancer Treatment & NO BIOPSY
                                        WHEN (prev_screening.ccs_via_result =
                                              'VIA positive: non-eligible for cryo/thermo-coagula'
                                            or
                                              prev_screening.ccs_via_result = 'suspected cervical cancer'
                                                 )
                                            AND prev_screening.biopsy_result is null
                                            THEN 'Eligible Biopsy Test Not Done'

                                        -- TC-13 HPV negative >3 years ago
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.ccs_hpv_result = 'Negative result'
                                                and TIMESTAMPDIFF(DAY, prev_screening.hpv_dna_result_received_date,
                                                                  REPORT_END_DATE) > 1095
                                            THEN 'Eligible Needs Re-Screening'

                                        -- TC-14 HPV positive, needs VIA triage
                                        WHEN
                                            prev_screening.ccs_via_result is null
                                                and prev_screening.ccs_hpv_result = 'Positive'
                                            THEN 'Eligible Needs VIA Triage'

                                        -- TC-15 Cytology Negative >3 years ago
                                        WHEN
                                            prev_screening.cytology_result = 'Negative result'
                                                and
                                            TIMESTAMPDIFF(DAY, prev_screening.cytology_result_date, REPORT_END_DATE) >
                                            1095
                                            THEN 'Eligible Needs Re-Screening'

                                        -- TC-16 Cytology Positive  NO HPV Test Followed
                                        WHEN
                                            (prev_screening.cytology_result =
                                             'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                or prev_screening.cytology_result = '> Ascus')
                                                AND prev_screening.hpv_dna_result_received_date is null
                                            THEN 'Eligible Needs HPV Triage'

                                        -- TC-17 Cytology Positive, HPV Test Done Next & Result Nagative (Reassess after Two Years)(Algorithm 7)
                                        WHEN
                                            (prev_screening.cytology_result =
                                             'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                or prev_screening.cytology_result = '> Ascus')
                                                AND prev_screening.ccs_hpv_result = 'Negative result'
                                                AND prev_screening.hpv_dna_result_received_date >
                                                    prev_screening.cytology_result_date
                                                and TIMESTAMPDIFF(DAY, prev_screening.hpv_dna_result_received_date,
                                                                  REPORT_END_DATE) > 730
                                            THEN 'Eligible Needs Re-Screening'

                                        -- TC-18 Cytology Positive, HPV Test Done Next & Result Positive (Colposcopy Triage Needed)(Algorithm 7)
                                        WHEN
                                            (prev_screening.cytology_result =
                                             'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                or prev_screening.cytology_result = '> Ascus')
                                                AND prev_screening.ccs_hpv_result = 'Positive'
                                                AND prev_screening.hpv_dna_result_received_date >
                                                    prev_screening.cytology_result_date
                                                and prev_screening.colposcopy_exam_finding is null
                                            THEN 'Eligible Needs Colposcopy Test'

                                        -- TC-19 Biopsy +ve (> CIN3) AND Treatment Give/Referred (See Algorithm 1, 2, 3 & 6)
                                        WHEN (prev_screening.biopsy_result = 'Carcinoma in situ'
                                            or prev_screening.biopsy_result = 'Invasive cervical cancer'
                                            or prev_screening.biopsy_result = 'Other')
                                            AND (prev_screening.ccs_treat_received_date <= REPORT_END_DATE
                                                or prev_screening.date_patient_referred_out <= REPORT_END_DATE)
                                            THEN 'Not Eligible Confirmed Cirvical Cancer'

                                        -- TC-20 Biopsy Result CIN-1, CIN-2 Rescreening Needed After One YEAR (See Algorithm 1, 2, 3 & 6)
                                        WHEN (prev_screening.biopsy_result = 'CIN (1-3)'
                                            or prev_screening.biopsy_result = 'CIN-2'
                                                 )
                                            and TIMESTAMPDIFF(DAY, prev_screening.biopsy_result_received_date,
                                                              REPORT_END_DATE) > 365
                                            THEN 'Eligible Needs Re-Screening'

                                        -- TC-20A,TC-20B,TC-20C VIA result Positive & Biopsy Done (CIN1),CIN2,Negative
                                        WHEN
                                            (
                                                prev_screening.ccs_via_result =
                                                'VIA positive: non-eligible for cryo/thermo-coagula' or
                                                prev_screening.ccs_via_result = 'suspected cervical cancer'
                                                )
                                                AND (prev_screening.biopsy_result = 'CIN (1-3)' or
                                                     prev_screening.biopsy_result = 'CIN-2' or
                                                     prev_screening.biopsy_result = 'Negative result')
                                                and TIMESTAMPDIFF(DAY, prev_screening.biopsy_result_received_date,
                                                                  REPORT_END_DATE) > 365
                                                AND
                                            prev_screening.biopsy_result_received_date >= prev_screening.via_date
                                            THEN 'Eligible Needs Re-Screening'

                                        -- TC-21 Colposcopy +ve but No Treatment/Referral
                                        WHEN (prev_screening.colposcopy_exam_finding = 'Low Grade' or
                                              prev_screening.colposcopy_exam_finding = 'High Grade')
                                            AND (prev_screening.ccs_treat_received_date is null
                                                or prev_screening.ccs_treat_received_date > REPORT_END_DATE)
                                            and
                                             (prev_screening.date_patient_referred_out is null
                                                 or prev_screening.date_patient_referred_out > REPORT_END_DATE)
                                            THEN 'Eligible Treatment Not Received or Not Referred'

                                        -- TC-22 Colposcopy +ve and treatment given
                                        WHEN (prev_screening.colposcopy_exam_finding = 'Low Grade' or
                                              prev_screening.colposcopy_exam_finding = 'High Grade')
                                            AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                                REPORT_END_DATE)) > 365 or
                                                 (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                                REPORT_END_DATE)) > 365)
                                            THEN 'Eligible Post Treatment/Referral Follow-Up'
                                        -- OLD
                                        WHEN
                                            prev_screening.ccs_hpv_result = 'Unknown'
                                            THEN 'Eligible Unknown HPV Screening Result'

                                        -- Cytology ASCUS, hpv positive, colposcopy normal
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.ccs_hpv_result = 'Positive' and
                                            prev_screening.cytology_result =
                                            'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                and prev_screening.colposcopy_exam_finding = 'Normal'
                                                and
                                            TIMESTAMPDIFF(DAY, prev_screening.colposcopy_exam_date, REPORT_END_DATE) >
                                            1095
                                            THEN 'Eligible Needs Re-Screening'
                                        -- Cytology ASCUS, hpv positive, colposcopy low/high and 365 days since treatment or referral
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.ccs_hpv_result = 'Positive' and
                                            prev_screening.cytology_result =
                                            'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                and (prev_screening.colposcopy_exam_finding = 'Low Grade' or
                                                     prev_screening.colposcopy_exam_finding = 'High Grade')
                                                #                                             AND (prev_screening.ccs_treat_received_date <= REPORT_END_DATE or
#                                                  prev_screening.date_patient_referred_out <= REPORT_END_DATE)
                                                AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                                    REPORT_END_DATE)) > 365 or
                                                     (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                                    REPORT_END_DATE)) > 365)
                                            THEN 'Eligible Post Treatment/Referral Follow-Up'
                                        -- Cytology ASCUS, hpv positive, colposcopy low/high and no referral or treatment
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.ccs_hpv_result = 'Positive' and
                                            prev_screening.cytology_result =
                                            'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                and (prev_screening.colposcopy_exam_finding = 'Low Grade' or
                                                     prev_screening.colposcopy_exam_finding = 'High Grade')
                                                #                                             AND (prev_screening.ccs_treat_received_date <= REPORT_END_DATE or
#                                                  prev_screening.date_patient_referred_out <= REPORT_END_DATE)
                                                AND (prev_screening.ccs_treat_received_date is null
                                                or prev_screening.ccs_treat_received_date > REPORT_END_DATE)
                                                and
                                            (prev_screening.date_patient_referred_out is null
                                                or prev_screening.date_patient_referred_out > REPORT_END_DATE)
                                            THEN 'Eligible Treatment Not Received or Not Referred'

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
                                                and
                                            TIMESTAMPDIFF(DAY, prev_screening.colposcopy_exam_date, REPORT_END_DATE) >
                                            1095
                                            THEN 'Eligible Needs Re-Screening'
                                        -- Cytology > ASCUS, colposcopy low/high and 365 days since treatment or referral
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.cytology_result =
                                            '> Ascus'
                                                and prev_screening.colposcopy_exam_finding = 'Normal'
                                                and (prev_screening.colposcopy_exam_finding = 'Low Grade' or
                                                     prev_screening.colposcopy_exam_finding = 'High Grade')
                                                AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                                    REPORT_END_DATE)) > 365 or
                                                     (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                                    REPORT_END_DATE)) > 365)
                                            THEN 'Eligible Post Treatment/Referral Follow-Up'
                                        -- Cytology > ASCUS, colposcopy low/high and no treatment or referral
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.cytology_result =
                                            '> Ascus'
                                                and prev_screening.colposcopy_exam_finding = 'Normal'
                                                and (prev_screening.colposcopy_exam_finding = 'Low Grade' or
                                                     prev_screening.colposcopy_exam_finding = 'High Grade')
                                                AND (prev_screening.ccs_treat_received_date is null
                                                or prev_screening.ccs_treat_received_date > REPORT_END_DATE)
                                                and
                                            (prev_screening.date_patient_referred_out is null
                                                or prev_screening.date_patient_referred_out > REPORT_END_DATE)
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
                                        WHEN prev_screening.client_id IS NULL
                                            AND latest_follow_up.eligible_for_cxca_screening = 'No'
                                            THEN
                                            'Not Eligible '

                                        -- TC-06 User-Labeled as Eligible
                                        WHEN prev_screening.client_id IS NULL
                                            AND latest_follow_up.screening_status !=
                                                'Cervical cancer screening performed'
                                            AND latest_follow_up.eligible_for_cxca_screening = 'Yes'
                                            THEN REPORT_END_DATE

                                        -- TC-07 Never screened/assessed
                                        WHEN prev_screening.client_id IS NULL
                                            AND latest_follow_up.eligible_for_cxca_screening IS NULL
                                            THEN REPORT_END_DATE

                                        -- TC-08 Unknown VIA result
                                        WHEN prev_screening.ccs_via_result = 'Unknown'
                                            THEN REPORT_END_DATE

                                        -- TC-09 VIA negative >2 years ago
                                        WHEN (TIMESTAMPDIFF(DAY, prev_screening.via_date, REPORT_END_DATE)) > 730
                                            AND prev_screening.ccs_via_result = 'VIA negative'
                                            THEN DATE_ADD(prev_screening.via_date, INTERVAL 730 DAY)

                                        -- TC-10 VIA Positive: Eligible for Cryo/Thermocoagulation & Treatment Given/Referred
                                        WHEN (prev_screening.ccs_via_result =
                                              'VIA positive: eligible for cryo/thermo-coagula')
                                            AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                                REPORT_END_DATE)) > 365 or
                                                 (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                                REPORT_END_DATE)) > 365)
                                            AND prev_screening.biopsy_result is null
                                            THEN COALESCE(
                                                DATE_ADD(prev_screening.ccs_treat_received_date, INTERVAL 365 DAY),
                                                DATE_ADD(prev_screening.date_patient_referred_out, INTERVAL 365 DAY))

                                        -- TC-11 VIA Positive: Eligible for Cryo/Thermocoagulation No Treatment/Referral
                                        WHEN (prev_screening.ccs_via_result =
                                              'VIA positive: eligible for cryo/thermo-coagula')
                                            AND (prev_screening.ccs_treat_received_date is null
                                                or prev_screening.ccs_treat_received_date > REPORT_END_DATE)
                                            and
                                             (prev_screening.date_patient_referred_out is null
                                                 or prev_screening.date_patient_referred_out > REPORT_END_DATE)
                                            AND prev_screening.biopsy_result is null
                                            THEN REPORT_END_DATE

                                        -- TC-12 VIA Positive & Not Eligible for Cryo/Thermocoagulation OR Suspicious for Cervical Cancer Treatment & NO BIOPSY
                                        WHEN (prev_screening.ccs_via_result =
                                              'VIA positive: non-eligible for cryo/thermo-coagula'
                                            or
                                              prev_screening.ccs_via_result = 'suspected cervical cancer'
                                                 )
                                            AND prev_screening.biopsy_result is null
                                            THEN REPORT_END_DATE

                                        -- TC-13 HPV negative >3 years ago
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.ccs_hpv_result = 'Negative result'
                                                and TIMESTAMPDIFF(DAY, prev_screening.hpv_dna_result_received_date,
                                                                  REPORT_END_DATE) > 1095
                                            THEN DATE_ADD(prev_screening.hpv_dna_result_received_date, INTERVAL 1095
                                                          DAY)

                                        -- TC-14 HPV positive, needs VIA triage
                                        WHEN
                                            prev_screening.ccs_via_result is null
                                                and prev_screening.ccs_hpv_result = 'Positive'
                                            THEN REPORT_END_DATE

                                        -- TC-15 Cytology Negative >3 years ago
                                        WHEN
                                            prev_screening.cytology_result = 'Negative result'
                                                and
                                            TIMESTAMPDIFF(DAY, prev_screening.cytology_result_date,
                                                          REPORT_END_DATE) >
                                            1095
                                            THEN DATE_ADD(prev_screening.cytology_result_date, INTERVAL 1095 DAY)

                                        -- TC-16 Cytology Positive  NO HPV Test Followed
                                        WHEN
                                            (prev_screening.cytology_result =
                                             'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                or prev_screening.cytology_result = '> Ascus')
                                                AND prev_screening.hpv_dna_result_received_date is null
                                            THEN REPORT_END_DATE

                                        -- TC-17 Cytology Positive, HPV Test Done Next & Result Nagative (Reassess after Two Years)(Algorithm 7)
                                        WHEN
                                            (prev_screening.cytology_result =
                                             'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                or prev_screening.cytology_result = '> Ascus')
                                                AND prev_screening.ccs_hpv_result = 'Negative result'
                                                AND prev_screening.hpv_dna_result_received_date >
                                                    prev_screening.cytology_result_date
                                                and TIMESTAMPDIFF(DAY, prev_screening.hpv_dna_result_received_date,
                                                                  REPORT_END_DATE) > 730
                                            THEN DATE_ADD(prev_screening.hpv_dna_result_received_date, INTERVAL 730
                                                          DAY)

                                        -- TC-18 Cytology Positive, HPV Test Done Next & Result Positive (Colposcopy Triage Needed)(Algorithm 7)
                                        WHEN
                                            (prev_screening.cytology_result =
                                             'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                or prev_screening.cytology_result = '> Ascus')
                                                AND prev_screening.ccs_hpv_result = 'Positive'
                                                AND prev_screening.hpv_dna_result_received_date >
                                                    prev_screening.cytology_result_date
                                                and prev_screening.colposcopy_exam_finding is null
                                            THEN REPORT_END_DATE

                                        -- TC-19 Biopsy +ve (> CIN3) AND Treatment Give/Referred (See Algorithm 1, 2, 3 & 6)
                                        WHEN (prev_screening.biopsy_result = 'Carcinoma in situ'
                                            or prev_screening.biopsy_result = 'Invasive cervical cancer'
                                            or prev_screening.biopsy_result = 'Other')
                                            AND (prev_screening.ccs_treat_received_date <= REPORT_END_DATE
                                                or prev_screening.date_patient_referred_out <= REPORT_END_DATE)
                                            THEN 'Not Applicable'

                                        -- TC-20 Biopsy Result CIN-1, CIN-2 Rescreening Needed After One YEAR (See Algorithm 1, 2, 3 & 6)
                                        WHEN (prev_screening.biopsy_result = 'CIN (1-3)'
                                            or prev_screening.biopsy_result = 'CIN-2'
                                                 )
                                            and TIMESTAMPDIFF(DAY, prev_screening.biopsy_result_received_date,
                                                              REPORT_END_DATE) > 365
                                            THEN DATE_ADD(prev_screening.biopsy_result, INTERVAL 365 DAY)

                                        -- TC-20A,TC-20B,TC-20C VIA result Positive & Biopsy Done (CIN1),CIN2,Negative
                                        WHEN
                                            (
                                                prev_screening.ccs_via_result =
                                                'VIA positive: non-eligible for cryo/thermo-coagula' or
                                                prev_screening.ccs_via_result = 'suspected cervical cancer'
                                                )
                                                AND (prev_screening.biopsy_result = 'CIN (1-3)' or
                                                     prev_screening.biopsy_result = 'CIN-2' or
                                                     prev_screening.biopsy_result = 'Negative result')
                                                and TIMESTAMPDIFF(DAY, prev_screening.biopsy_result_received_date,
                                                                  REPORT_END_DATE) > 365
                                                AND
                                            prev_screening.biopsy_result_received_date >= prev_screening.via_date
                                            THEN DATE_ADD(prev_screening.biopsy_result, INTERVAL 365 DAY)

                                        -- TC-21 Colposcopy +ve but No Treatment/Referral
                                        WHEN (prev_screening.colposcopy_exam_finding = 'Low Grade' or
                                              prev_screening.colposcopy_exam_finding = 'High Grade')
                                            AND (prev_screening.ccs_treat_received_date is null
                                                or prev_screening.ccs_treat_received_date > REPORT_END_DATE)
                                            and
                                             (prev_screening.date_patient_referred_out is null
                                                 or prev_screening.date_patient_referred_out > REPORT_END_DATE)
                                            THEN 'Not Applicable'

                                        -- TC-22 Colposcopy +ve and treatment given
                                        WHEN (prev_screening.colposcopy_exam_finding = 'Low Grade' or
                                              prev_screening.colposcopy_exam_finding = 'High Grade')
                                            AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                                REPORT_END_DATE)) > 365 or
                                                 (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                                REPORT_END_DATE)) > 365)
                                            THEN REPORT_END_DATE
                                        -- OLD
                                        -- No previous screening and eligible for cxca screening
                                        WHEN prev_screening.client_id IS NULL AND
                                             latest_follow_up.eligible_for_cxca_screening = 'Yes'
                                            THEN REPORT_END_DATE
                                        -- No previous screening and eligibility not documented
                                        WHEN prev_screening.client_id IS NULL AND
                                             latest_follow_up.eligible_for_cxca_screening IS NULL
                                            THEN REPORT_END_DATE

                                        -- VIA Negative and prev via date > 730 days
                                        WHEN
                                            (TIMESTAMPDIFF(DAY, prev_screening.via_date, REPORT_END_DATE)) > 730 AND
                                            prev_screening.ccs_via_result = 'VIA negative'
                                            THEN DATE_ADD(prev_screening.via_date, INTERVAL 730 DAY)

                                        -- VIA Positive or suspicious 365 days since treatment or referral
                                        WHEN (prev_screening.ccs_via_result =
                                              'VIA positive: eligible for cryo/thermo-coagula' or
                                              prev_screening.ccs_via_result =
                                              'VIA positive: non-eligible for cryo/thermo-coagula' or
                                              prev_screening.ccs_via_result = 'suspected cervical cancer')
                                            #                                         AND (prev_screening.ccs_treat_received_date <= REPORT_END_DATE or
#                                              prev_screening.date_patient_referred_out <= REPORT_END_DATE)
                                            AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                                REPORT_END_DATE)) > 365 or
                                                 (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                                REPORT_END_DATE)) > 365)
                                            THEN DATE_ADD(COALESCE(prev_screening.ccs_treat_received_date,
                                                                   prev_screening.date_patient_referred_out),
                                                          INTERVAL
                                                          730 DAY)

                                        -- VIA Positive or suspicious and no treatment and not referred
                                        WHEN (prev_screening.ccs_via_result =
                                              'VIA positive: eligible for cryo/thermo-coagula' or
                                              prev_screening.ccs_via_result =
                                              'VIA positive: non-eligible for cryo/thermo-coagula' or
                                              prev_screening.ccs_via_result = 'suspected cervical cancer')
                                            AND (prev_screening.ccs_treat_received_date is null
                                                or prev_screening.ccs_treat_received_date > REPORT_END_DATE)
                                            and
                                             (prev_screening.date_patient_referred_out is null
                                                 or prev_screening.date_patient_referred_out > REPORT_END_DATE)
                                            THEN REPORT_END_DATE

                                        -- VIA Unknown screening result
                                        WHEN prev_screening.ccs_via_result = 'Unknown'
                                            THEN REPORT_END_DATE

                                        -- HPV negative and previous hpv date > 1095
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.ccs_hpv_result = 'Negative result' and
                                            TIMESTAMPDIFF(DAY, prev_screening.hpv_dna_result_received_date,
                                                          REPORT_END_DATE) > 1095
                                            THEN DATE_ADD(prev_screening.hpv_dna_result_received_date, INTERVAL 1095
                                                          DAY)
                                        -- HPV Positive and no via done
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.ccs_hpv_result = 'Positive'
                                            THEN REPORT_END_DATE
                                        -- HPV date unknown
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.ccs_hpv_result = 'Unknown'
                                            THEN REPORT_END_DATE
                                        -- Cytology negative result
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            -- prev_screening.ccs_hpv_result is null and
                                            prev_screening.cytology_result = 'Negative result' and
                                            TIMESTAMPDIFF(DAY, prev_screening.cytology_result_date,
                                                          REPORT_END_DATE) >
                                            1095
                                            THEN DATE_ADD(prev_screening.cytology_result_date, INTERVAL 1095 DAY)
                                        -- Cytology ASCUS, hpv positive, colposcopy normal
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.ccs_hpv_result = 'Positive' and
                                            prev_screening.cytology_result =
                                            'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                and prev_screening.colposcopy_exam_finding = 'Normal'
                                                and
                                            TIMESTAMPDIFF(DAY, prev_screening.colposcopy_exam_date,
                                                          REPORT_END_DATE) >
                                            1095
                                            THEN DATE_ADD(prev_screening.colposcopy_exam_date, INTERVAL 1095 DAY)
                                        -- Cytology ASCUS, hpv positive, colposcopy low/high and 365 days since treatment or referral
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.ccs_hpv_result = 'Positive' and
                                            prev_screening.cytology_result =
                                            'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                and (prev_screening.colposcopy_exam_finding = 'Low Grade' or
                                                     prev_screening.colposcopy_exam_finding = 'High Grade')
                                                #                                             AND (prev_screening.ccs_treat_received_date <= REPORT_END_DATE or
#                                                  prev_screening.date_patient_referred_out <= REPORT_END_DATE)
                                                AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                                    REPORT_END_DATE)) > 365 or
                                                     (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                                    REPORT_END_DATE)) > 365)
                                            THEN DATE_ADD(COALESCE(prev_screening.ccs_treat_received_date,
                                                                   prev_screening.date_patient_referred_out),
                                                          INTERVAL
                                                          365 DAY)
                                        -- Cytology ASCUS, hpv positive, colposcopy low/high and no referral or treatment
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.ccs_hpv_result = 'Positive' and
                                            prev_screening.cytology_result =
                                            'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                and (prev_screening.colposcopy_exam_finding = 'Low Grade' or
                                                     prev_screening.colposcopy_exam_finding = 'High Grade')
                                                #                                             AND (prev_screening.ccs_treat_received_date <= REPORT_END_DATE or
#                                                  prev_screening.date_patient_referred_out <= REPORT_END_DATE)
                                                AND (prev_screening.ccs_treat_received_date is null
                                                or prev_screening.ccs_treat_received_date > REPORT_END_DATE)
                                                and
                                            (prev_screening.date_patient_referred_out is null
                                                or prev_screening.date_patient_referred_out > REPORT_END_DATE)
                                            THEN REPORT_END_DATE

                                        -- Cytology ASCUS, hpv positive, colposcopy not done
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.ccs_hpv_result = 'Positive' and
                                            prev_screening.cytology_result =
                                            'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                and prev_screening.colposcopy_exam_finding is null
                                            THEN REPORT_END_DATE

                                        -- Cytology ASCUS, hpv Negative
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.ccs_hpv_result = 'Negative result' and
                                            prev_screening.cytology_result =
                                            'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                and TIMESTAMPDIFF(DAY, prev_screening.hpv_dna_result_received_date,
                                                                  REPORT_END_DATE) > 1095
                                            THEN DATE_ADD(prev_screening.hpv_dna_result_received_date, INTERVAL 1095
                                                          DAY)

                                        -- Cytology ASCUS, hpv not done
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.ccs_hpv_result is null and
                                            prev_screening.cytology_result =
                                            'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                            THEN REPORT_END_DATE


                                        -- Cytology > ASCUS, colposcopy normal
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.cytology_result =
                                            '> Ascus'
                                                and prev_screening.colposcopy_exam_finding = 'Normal'
                                                and
                                            TIMESTAMPDIFF(DAY, prev_screening.colposcopy_exam_date,
                                                          REPORT_END_DATE) >
                                            1095
                                            THEN DATE_ADD(prev_screening.colposcopy_exam_date, INTERVAL 1095 DAY)
                                        -- Cytology > ASCUS, colposcopy low/high and 365 days since treatment or referral
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.cytology_result =
                                            '> Ascus'
                                                and prev_screening.colposcopy_exam_finding = 'Normal'
                                                and (prev_screening.colposcopy_exam_finding = 'Low Grade' or
                                                     prev_screening.colposcopy_exam_finding = 'High Grade')
                                                AND ((TIMESTAMPDIFF(DAY, prev_screening.ccs_treat_received_date,
                                                                    REPORT_END_DATE)) > 365 or
                                                     (TIMESTAMPDIFF(DAY, prev_screening.date_patient_referred_out,
                                                                    REPORT_END_DATE)) > 365)
                                            THEN DATE_ADD(COALESCE(prev_screening.ccs_treat_received_date,
                                                                   prev_screening.date_patient_referred_out),
                                                          INTERVAL
                                                          365 DAY)
                                        -- Cytology > ASCUS, colposcopy low/high and no treatment or referral
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.cytology_result =
                                            '> Ascus'
                                                and prev_screening.colposcopy_exam_finding = 'Normal'
                                                and (prev_screening.colposcopy_exam_finding = 'Low Grade' or
                                                     prev_screening.colposcopy_exam_finding = 'High Grade')
                                                AND (prev_screening.ccs_treat_received_date is null
                                                or prev_screening.ccs_treat_received_date > REPORT_END_DATE)
                                                and
                                            (prev_screening.date_patient_referred_out is null
                                                or prev_screening.date_patient_referred_out > REPORT_END_DATE)
                                            THEN REPORT_END_DATE

                                        -- Cytology > ASCUS, colposcopy not done
                                        WHEN
                                            -- prev_screening.ccs_via_result is null and
                                            prev_screening.cytology_result =
                                            '> Ascus'
                                                and prev_screening.colposcopy_exam_finding is null
                                            THEN REPORT_END_DATE
                                        END                       AS EligibilityDate
                             from latest_follow_up
                                      left join prev_screening on prev_screening.client_id = latest_follow_up.client_id),
        cx_eligibility as (  select patient_name                                                                 `Patient Name`,
                                    patient_uuid                                                                 `UUID`,
                                    CAST(mrn AS CHAR(20))                                                     as mrn,
                                    uan,
                                    mobile_no                                                                 as `Mobile No`,
                                    phone_no                                                                  as `Home Telephone No`,
                                    Weight,
                                    Age,
                                    CAST(EligibilityDate AS Date)                                                           as `Eligibility Date`,
                                    CAST(EligibilityDate AS Date)                                                           as `Eligibility Date EC.`,
                                    LEFT(EligibilityStatus, IF(LEFT(EligibilityStatus, 3) = 'Eli', 8, 12))    as `Eligibility Status`,
                                    SUBSTR(EligibilityStatus, IF(LEFT(EligibilityStatus, 3) = 'Eli', 10, 14)) as `Eligibility Reason`,
                                    follow_up_date                                                            as `Follow Up Date`,
                                    follow_up_date                                                            as `Follow Up Date EC.`,
                                    ArtStartDate                                                              as `Art Start Date`,
                                    ArtStartDate                                                              as `Art Start Date EC.`,
                                    CASE follow_up_status
                                        WHEN 'Alive' THEN 'Alive on ART'
                                        WHEN 'Restart medication' THEN 'Restart'
                                        WHEN 'Transferred out' THEN 'TO'
                                        WHEN 'Stop all' THEN 'Stop'
                                        WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
                                        WHEN 'Ran away' THEN 'Drop'
                                        END                                                                   AS `Follow Up Status`,
                                    next_visit_date                                                           as `Next Appointment Date`,
                                    next_visit_date                                                           as `Next Appointment Date EC.`,
                                    regimen                                                                   as `ARV Regimen`,
                                    dose_days                                                                 as `ART Dose Days`,
                                    CCaCounsellingGiven                                                       as Counselled,
                                    Accepted,
                                    screening_type                                                            as `Screening Type`,
                                    screening_method                                                             `Screening Method`,
                                    hpv_subtype                                                               as `HPV SubType`,
                                    date_hpv_test_was_done                                                    as `HPV Sample Collected Date`,
                                    date_hpv_test_was_done                                                    as `HPV Sample Collected Date EC.`,
                                    hpv_dna_result_received_date                                              as `HPV DNA Result Received Date`,
                                    hpv_dna_result_received_date                                              as `HPV DNA Result Received Date EC.`,
                                    ccs_hpv_result                                                            as `HPV Result`,
                                    via_date                                                                  as `VIA Screening Date`,
                                    via_date                                                                  as `VIA Screening Date EC.`,
                                    ccs_via_result                                                            as `VIA Screening Result`,
                                    cytology_sample_collection_date                                           as `Cytology Sample Collected Date`,
                                    cytology_sample_collection_date                                           as `Cytology Sample Collected Date EC.`,
                                    cytology_result_date                                                      as `Cytology Result Received Date`,
                                    cytology_result_date                                                      as `Cytology Result Received Date EC.`,
                                    cytology_result                                                           as `Cytology Result`,
                                    colposcopy_exam_date                                                      as `Colposcopy Exam Date`,
                                    colposcopy_exam_date                                                      as `Colposcopy Exam Date EC.`,
                                    colposcopy_exam_finding                                                   as `Colposcopy Exam Result`,
                                    biopsy_sample_collected_date                                              as `Biopsy Sample Collected Date`,
                                    biopsy_sample_collected_date                                              as `Biopsy Sample Collected Date EC.`,
                                    biopsy_result_received_date                                               as `Biopsy Result Received Date`,
                                    biopsy_result_received_date                                               as `Biopsy Result Received Date EC.`,
                                    biopsy_result                                                             as `Biopsy Result`,
                                    CCS_Precancerous_Treat                                                    as `Treatment Received For Precancerous Lesion`,
                                    ccs_treat_received_date                                                   as `Date Treatment Given`,
                                    ccs_treat_received_date                                                   as `Date Treatment Given EC.`,
                                    referral_or_linkage_status                                                as `Referral Status`,
                                    reason_for_referral_cacx                                                  as `Reason For Referral`,
                                    date_patient_referred_out                                                 as `Date Referred to Other HF`,
                                    date_patient_referred_out                                                 as `Date Referred to Other HF EC.`,
                                    date_client_arrived_in_the_referred                                       as `Date Client Arrived in Reffered HF`,
                                    date_client_arrived_in_the_referred                                       as `Date Client Arrived in Reffered HF EC.`,
                                    date_client_served_in_the_referred_                                       as `Date Client Served in Reffered HF`,
                                    date_client_served_in_the_referred_                                       as `Date Client Served in Reffered HF EC.`,
                                    ccs_next_date                                                             as `Next Appointment Date for CCS`,
                                    ccs_next_date                                                             as `Next Appointment Date for CCS EC.`
                             from cx_base_clients)
    select * from cx_eligibility;

END //

DELIMITER ;