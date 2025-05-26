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
     prev_screening as (select CASE
                                   WHEN (TIMESTAMPDIFF(DAY, FollowUP.HPV_DNA_RESULT_RECEIVED_DATE, REPORT_END_DATE) >
                                         1095 AND
                                         FollowUP.ccs_hpv_result = 'Negative result')
                                       THEN 'Need Re-Screening'
                                   WHEN (TIMESTAMPDIFF(DAY, FollowUP.date_via_result, REPORT_END_DATE) > 730 AND
                                         FollowUP.ccs_via_result = 'VIA negative')
                                       THEN 'Need Re-Screening'
                                   WHEN (TIMESTAMPDIFF(DAY, FollowUP.date_cytology_result_received, REPORT_END_DATE) >
                                         1095 AND
                                         FollowUP.cytology_result = 'Negative result')
                                       THEN 'Need Re-Screening'
                                   WHEN (FollowUP.ccs_treat_received_date is null AND
                                         (FollowUP.colposcopy_exam_finding = 'High Grade' OR
                                          FollowUP.colposcopy_exam_finding = 'Low Grade' OR
                                          FollowUP.cytology_result = '> Ascus' OR
                                          FollowUP.ccs_via_result = 'VIA positive: eligible for cryo/thermo-coagula' OR
                                          FollowUP.ccs_via_result = 'VIA Positive: Non-Eligible for'))
                                       THEN 'Need Re-Screening'
                                   WHEN TIMESTAMPDIFF(DAY, FollowUP.ccs_treat_received_date, REPORT_END_DATE) > 181 AND
                                        FollowUP.ccs_treat_received_date is not null
                                       THEN 'Need Post Tx FU Screening'
                                   WHEN (TIMESTAMPDIFF(DAY, FollowUP.hpv_dna_result_received_date, REPORT_END_DATE) >
                                         356 AND
                                         FollowUP.ccs_hpv_result = 'Positive' AND
                                         FollowUP.ccs_via_result = 'VIA negative') THEN 'Need Re-Screening'
                                   WHEN (FollowUP.ccs_next_date <= REPORT_END_DATE AND
                                         FollowUP.ccs_next_date is not null)
                                       THEN 'Need Re-Screening' END         AS PrevCxCaStatus,
                               CASE
                                   WHEN FollowUP.COLPOSCOPY_EXAM_DATE IS NOT NULL and
                                        FollowUP.COLPOSCOPY_EXAM_DATE is not null
                                       THEN FollowUP.COLPOSCOPY_EXAM_DATE
                                   WHEN FollowUP.date_cytology_result_received IS NOT NULL and
                                        FollowUP.date_cytology_result_received is not null
                                       THEN FollowUP.date_cytology_result_received
                                   WHEN FollowUP.DATE_VIA_RESULT IS NOT NULL and FollowUP.DATE_VIA_RESULT is not null
                                       THEN FollowUP.DATE_VIA_RESULT
                                   WHEN (FollowUP.HPV_DNA_RESULT_RECEIVED_DATE IS NOT NULL and
                                         FollowUP.HPV_DNA_RESULT_RECEIVED_DATE is not null) AND
                                        FollowUP.CCS_HPV_Result = 'Negative result'
                                       THEN FollowUP.HPV_DNA_RESULT_RECEIVED_DATE
                                   WHEN FollowUP.follow_up_date is not null and
                                        FollowUP.screening_status = 'Cervical cancer screening performed'
                                       THEN FollowUP.follow_up_date END     AS Prev_CSS_Screen_Done_Date_Calculated,
                               FollowUP.screening_type                      as Prev_Screen_Type,
                               FollowUP.screening_method                    as Prev_Screen_Method,
                               FollowUP.HPV_SUBTYPE                         as Prev_HPV_SubType,
                               FollowUP.date_hpv_test_was_done              As Prev_HPV_DAN_SampleCollected_Date,
                               FollowUP.HPV_DNA_RESULT_RECEIVED_DATE        As Prev_HPV_DAN_ResultReceived_Date,

                               FollowUP.CCS_HPV_Result                      As Prev_HPV_Result,
                               FollowUP.DATE_VIA_RESULT                     As Prev_VIA_Screening_Date,


                               FollowUP.CCS_VIA_Result                      As Prev_VIA_Screening_Result,
                               FollowUP.CYTOLOGY_SAMPLE_COLLECTION_DATE     As Prev_Cytology_SampleCollected_Date,
                               FollowUP.date_cytology_result_received       As Prev_Cytology_ResultReceived_Date,
                               FollowUP.CYTOLOGY_RESULT                     As Prev_Cytology_Result,
                               FollowUP.COLPOSCOPY_EXAM_DATE                As Prev_Colposcopy_Exam_Date,
                               FollowUP.COLPOSCOPY_EXAM_FINDING             As Prev_Colposcopy_Exam_Result,
                               FollowUP.BIOPSY_SAMPLE_COLLECTED_DATE        As Prev_Biopsy_SampleCollected_Date,
                               FollowUP.BIOPSY_RESULT_RECEIVED_DATE         As Prev_Biopsy_ResultReceived_Date,
                               FollowUP.BIOPSY_RESULT                       As Prev_Biopsy_Result,
                               FollowUP.CCS_Precancerous_Treat              As Prev_TX_Received_for_PrecancerousLesion,

                               FollowUP.confirmed_cervical_cancer_cases_bas As Prev_TX_for_ConfirmedCxCaBasedOn_Biopsy,
                               FollowUP.CCS_Treat_Received_Date             As Prev_Date_TX_Given,
                               FollowUP.REFERRAL_OR_LINKAGE_STATUS          as Prev_ReferralStatus,

                               FollowUP.reason_for_referral_cacx            As Prev_Reason_for_Referral,
                               FollowUP.date_patient_referred_out           As Prev_Date_Referred_to_OtherHF,
                               FollowUP.date_client_arrived_in_the_referred As
                                                                               Prev_Date_Client_Arrived_in_RefferedHF,
                               FollowUP.date_client_served_in_the_referred_ AS Prev_Date_Client_Served_in_RefferedHF,
                               ccs_next_date,
                               screening_status,
                               CASE
                                   WHEN FollowUP.screening_type = 'Initial visit' AND
                                        (FollowUP.CCS_VIA_Result is null) AND
                                        (FollowUP.CYTOLOGY_RESULT is null) AND
                                        (FollowUP.COLPOSCOPY_EXAM_FINDING is null) And
                                        (FollowUP.COLPOSCOPY_EXAM_FINDING is null)
                                       THEN 'HPV_Positive-Requires VIA Triage'
                                   WHEN FollowUP.screening_type = 'Initial visit' AND
                                        (FollowUP.CCS_VIA_Result = 'VIA positive: eligible for cryo/thermo-coagula' OR
                                         FollowUP.CCS_VIA_Result =
                                         'VIA positive: non-eligible for cryo/thermo-coagula' OR
                                         (((FollowUP.CCS_VIA_Result is null OR FollowUP.CCS_VIA_Result = 'Unknown') AND
                                           (FollowUP.CYTOLOGY_RESULT = '> Ascus') AND
                                           (FollowUP.COLPOSCOPY_EXAM_FINDING = 'Low Grade' OR
                                            FollowUP.COLPOSCOPY_EXAM_FINDING = 'High Grade'))))
                                       THEN 'HPV_Positive'
                                   WHEN FollowUP.screening_type = 'Re-screening for cervical cancer after 1 year' AND
                                        (FollowUP.CCS_VIA_Result = 'VIA positive: eligible for cryo/thermo-coagula' OR
                                         FollowUP.CCS_VIA_Result = 'VIA positive: non-eligible for cryo/thermo-coagula')
                                       THEN 'VIA_Positive'
                                   WHEN FollowUP.screening_type = 'Post-treatment follow-up at 1 year' AND
                                        (FollowUP.Cytology_Result = '> Ascus' AND
                                         (FollowUP.COLPOSCOPY_EXAM_FINDING is null OR
                                          FollowUP.COLPOSCOPY_EXAM_FINDING = 'Low Grade' OR
                                          FollowUP.COLPOSCOPY_EXAM_FINDING = 'High Grade'))
                                       THEN 'CYT_Positive'
                                   WHEN FollowUP.screening_type = 'Initial visit' AND
                                        (FollowUP.CCS_HPV_Result = 'Positive'
                                            AND ((FollowUP.CCS_VIA_Result = 'VIA negative') OR
                                                 ((FollowUP.CCS_VIA_Result is null OR FollowUP.CCS_VIA_Result = 'Unknown') AND
                                                  (FollowUP.COLPOSCOPY_EXAM_FINDING = 'Normal' OR
                                                   (FollowUP.CYTOLOGY_RESULT = 'Negative result' OR
                                                    FollowUP.CYTOLOGY_RESULT =
                                                    'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear')))))
                                       THEN 'HPV_Negative'
                                   WHEN FollowUP.screening_type = 'Initial visit' AND
                                        (FollowUP.CCS_HPV_Result = 'Negative result')
                                       THEN 'HPV_Negative'
                                   WHEN FollowUP.screening_type = 'Re-screening for cervical cancer after 1 year' AND
                                        (FollowUP.CCS_VIA_Result = 'VIA negative')
                                       THEN 'VIA_Negative'
                                   WHEN FollowUP.screening_type = 'Post-treatment follow-up at 1 year' AND
                                        (FollowUP.Cytology_Result = 'Negative result' OR FollowUP.Cytology_Result =
                                                                                         'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear') OR
                                        (FollowUP.Cytology_Result = '> Ascus' AND
                                         FollowUP.COLPOSCOPY_EXAM_FINDING = 'Normal')
                                       THEN 'CYT_Negative'
                                   WHEN FollowUP.CCS_VIA_Result = 'VIA positive: non-eligible for cryo/thermo-coagula'
                                       THEN 'VIA_Suspected' END             AS Prev_CCS_Screen_Result,
                               CASE
                                   when (FollowUP.CCS_Precancerous_Treat = 'Cryosurgery of lesion of cervix') and
                                        ((FollowUP.CCS_Treat_Received_Date < REPORT_START_DATE And
                                          FollowUP.CCS_Treat_Received_Date is not null) OR
                                         FollowUP.follow_up_date < REPORT_START_DATE) THEN 'Cryotherapy'
                                   when (FollowUP.CCS_Precancerous_Treat =
                                         'Loop electrosurgical excision procedure of cervix') and
                                        ((FollowUP.CCS_Treat_Received_Date < REPORT_START_DATE
                                            And FollowUP.CCS_Treat_Received_Date is not null) OR
                                         FollowUP.follow_up_date < REPORT_START_DATE)
                                       THEN 'LEEP'
                                   when (FollowUP.CCS_Precancerous_Treat = 'Thermocauterization of cervix') and
                                        ((FollowUP.CCS_Treat_Received_Date < REPORT_START_DATE And
                                          FollowUP.CCS_Treat_Received_Date is not null) OR
                                         FollowUP.follow_up_date < REPORT_START_DATE)
                                       THEN 'Thermocoagulation' end         as Prev_CxCa_TreatmentGiven,
                               CASE
                                   when FollowUP.CCS_Treat_Received_Date is not null
                                       then CCS_Treat_Received_Date
                                   else FollowUP.follow_up_date end         as Prev_CxCaTreatmentGivenDate
                        from tmp_prev_screening
                        where scrn_row_num = 1),


