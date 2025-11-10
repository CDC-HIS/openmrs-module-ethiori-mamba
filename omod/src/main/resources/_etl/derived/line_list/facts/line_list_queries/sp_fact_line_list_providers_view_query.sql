DELIMITER //
DROP PROCEDURE IF EXISTS sp_fact_line_list_providers_view_query;
CREATE PROCEDURE sp_fact_line_list_providers_view_query(IN TYPE_OF_CLIENT VARCHAR(50), IN START_DATE DATE,
                                                        IN END_DATE DATE,
                                                        IN START_NV_DATE DATE, IN END_NV_DATE DATE,
                                                        IN PATIENT_GUID VARCHAR(50))
BEGIN

    WITH FollowUp as (select follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_status,
                             follow_up_date_followup_                        as follow_up_date,
                             art_antiretroviral_start_date                   as art_start_date,
                             date_started_on_tuberculosis_prophy             as inhprophylaxis_started_date,
                             date_completed_tuberculosis_prophyl             as inhprophylaxis_completed_date,
                             date_discontinued_tuberculosis_prop             as inhprophylaxis_discontinued_date,
                             date_active_tbrx_completed                      as date_active_tb_treatment_completed,
                             weight_text_                                    as weight,
                             eligible_for_tpt,
                             reason_not_eligible_for_tuberculosi,
                             date_of_reported_hiv_viral_load                 as viral_load_sent_date,
                             regimen_change,
                             date_viral_load_results_received                as viral_load_perform_date,
                             viral_load_test_status,
                             hiv_viral_load                                  as viral_load_count,
                             regimen,
                             cd4_count,
                             nutritional_status_of_adult                     as ns_adult,
                             nutritional_status_of_older_child_a             as ns_child,
                             nutritional_screening_result,
                             date_of_event                                   as hiv_confirmed_date,
                             pregnancy_status,
                             next_visit_date,
                             antiretroviral_art_dispensed_dose_i                arv_dose,
                             treatment_end_date                                 art_dose_end_date,
                             currently_breastfeeding_child                   as breastfeeding_status,
                             adherence,
                             date_of_last_menstrual_period_lmp_              as lmp_date,
                             pre_test_counselling_for_cervical_c             as councelling_given,
                             biopsy_result,
                             assessment_date,
                             next_follow_up_screening_date                   as ccs_next_date,
                             assessment_status,

                             COALESCE(
                                     at_3436_weeks_of_gestation,
                                     viral_load_after_eac_confirmatory_viral_load_where_initial_v,
                                     viral_load_after_eac_repeat_viral_load_where_initial_viral_l,
                                     every_six_months_until_mtct_ends,
                                     six_months_after_the_first_viral_load_test_at_postnatal_peri,
                                     three_months_after_delivery,
                                     at_the_first_antenatal_care_visit,
                                     annual_viral_load_test,
                                     second_viral_load_test_at_12_months_post_art,
                                     first_viral_load_test_at_6_months_or_longer_post_art,
                                     first_viral_load_test_at_3_months_or_longer_post_art
                             )                                               AS routine_viral_load_test_indication,
                             dsd_category,
                             hpv_dna_result_received_date,
                             date_cytology_result_received                   as cytology_result_date,
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
                             treatment_of_precancerous_lesions_of_the_cervix as CCS_Precancerous_Treat,
                             confirmed_cervical_cancer_cases_bas,
                             referral_or_linkage_status,
                             reason_for_referral_cacx,
                             date_client_served_in_the_referred_,
                             date_client_arrived_in_the_referred,
                             date_patient_referred_out,
                             prep_offered,
                             weight_text_,
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
                                         ON follow_up.encounter_id = follow_up_9.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_10 follow_up_10
                                         ON follow_up.encounter_id = follow_up_10.encounter_id),

         tmp_address as (select patient_uuid,
                                client_id,
                                patient_name                                 AS PatientName,
                                timestampdiff(YEAR, date_of_birth, END_DATE) AS Age,
                                sex                                          as sex,
                                patient_name,
                                CASE
                                    WHEN
                                        (given_name <> '' and middle_name <> '' and family_name <> '' and
                                            # ad.districtName woreda <> '' - - and ad.communityName kebele <> '' and ad.StreetName house no <> ''
#                                         and
                                         (mobile_no <> '' or phone_no <> '')
                                            ) THEN 'Green'
                                    ELSE 'Yellow'
                                    END                                      AS Adrress,
                                CAST(mrn AS CHAR(20))                        as mrn,
                                uan
                         from mamba_dim_client
                         where mrn is not null),

         tmp_all_art_not_started as (select client_id,
                                            encounter_id,
                                            art_start_date,
                                            follow_up_status,
                                            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY art_start_date DESC, FollowUp.encounter_id DESC) AS row_num
                                     from FollowUp),
         all_art_not_started as (select * from tmp_all_art_not_started where row_num = 1),

         tmp_all_art_follow_ups as (select client_id,
                                           encounter_id,
                                           follow_up_date,
                                           follow_up_status,
                                           ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, FollowUp.encounter_id DESC) AS row_num
                                    from FollowUp),
         all_art_follow_ups as (select * from tmp_all_art_follow_ups where row_num = 1),


         all_art_not_started_status as (select *, 'ART Not Started' as art_status
                                        from all_art_not_started
                                        where art_start_date is null),
         tmp_tpt_start as (select client_id,
                                  inhprophylaxis_started_date,
                                  ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY inhprophylaxis_started_date DESC, FollowUp.encounter_id DESC) AS row_num

                           from FollowUp
                           where inhprophylaxis_started_date is not null),
         tpt_start as (select * from tmp_tpt_start where row_num = 1),

         tmp_tpt_completed as (select client_id,
                                      inhprophylaxis_completed_date,
                                      ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY inhprophylaxis_completed_date DESC, FollowUp.encounter_id DESC) AS row_num

                               from FollowUp
                               where inhprophylaxis_completed_date is not null),
         tpt_completed as (select * from tmp_tpt_completed where row_num = 1),

         tmp_tpt_discontinued as (select client_id,
                                         inhprophylaxis_discontinued_date,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY inhprophylaxis_discontinued_date DESC, FollowUp.encounter_id DESC) AS row_num

                                  from FollowUp
                                  where inhprophylaxis_discontinued_date is not null),
         tpt_discontinued as (select * from tmp_tpt_discontinued where row_num = 1),

         tmp_tb_treatment_completed as (SELECT client_id,
                                               date_active_tb_treatment_completed                                                                             AS ActiveTBTreatmentCompletedDate,
                                               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY date_active_tb_treatment_completed DESC, encounter_id DESC) AS row_num
                                        FROM FollowUp
                                        WHERE date_active_tb_treatment_completed IS NOT NULL
                                          and TIMESTAMPDIFF(DAY, date_active_tb_treatment_completed, END_DATE) < 1095),
         tb_treatment_completed as (select * from tmp_tb_treatment_completed where row_num = 1),

         tb_treatment_completed_2 as (SELECT client_id,
                                             date_active_tb_treatment_completed                                                                             AS ActiveTBTreatmentCompletedDate,
                                             ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY date_active_tb_treatment_completed DESC, encounter_id DESC) AS row_num
                                      FROM FollowUp
                                      where date_active_tb_treatment_completed is not null
                                        and TIMESTAMPDIFF(DAY, date_active_tb_treatment_completed, END_DATE) < 1095
                                        and inhprophylaxis_completed_date is null
                                        and inhprophylaxis_discontinued_date is null
                                        and inhprophylaxis_started_date),


         tmp_tpt_eligible_yes as (select client_id,
                                         encounter_id,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  where eligible_for_tpt = 'Yes'
                                    and inhprophylaxis_completed_date is null
                                    and inhprophylaxis_discontinued_date is null
                                    and inhprophylaxis_started_date is null),
         tpt_eligible_yes as (select * from tmp_tpt_eligible_yes where row_num = 1),

         tmp_tpt_eligible_no as (select client_id,
                                        encounter_id,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                 FROM FollowUp
                                 where eligible_for_tpt = 'No'
                                   and inhprophylaxis_completed_date is null
                                   and inhprophylaxis_discontinued_date is null
                                   and inhprophylaxis_started_date is null),
         tpt_eligible_no as (select * from tmp_tpt_eligible_no where row_num = 1),


         tmp_tpt_not_eligible as (select client_id,
                                         encounter_id,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  where reason_not_eligible_for_tuberculosi = 'Contraindication'
                                    and inhprophylaxis_completed_date is null
                                    and inhprophylaxis_discontinued_date is null
                                    and inhprophylaxis_started_date is null),
         tpt_not_eligible as (select * from tmp_tpt_not_eligible where row_num = 1),

         contraindicated as (select client_id, 'Contraindicated' as tpt_status from tpt_not_eligible),

         tpt_bronze_5 as (select ActiveTBTreatmentCompletedDate,
                                 client_id,
                                 Case
                                     When timestampdiff(DAY, tb_treatment_completed.ActiveTBTreatmentCompletedDate,
                                                        END_DATE) < 1095
                                         THEN 'Treatment for TB'
                                     END as tpt_status
                          from tb_treatment_completed
                          where tb_treatment_completed.client_id not in
                                (select contraindicated.client_id from contraindicated)),
         tpt_gold as (select tpt_start.client_id,
                             Case
                                 When tpt_start.inhprophylaxis_started_date <=
                                      tpt_completed.inhprophylaxis_completed_date
                                     THEN 'Gold'
                                 When tpt_start.inhprophylaxis_started_date >
                                      tpt_completed.inhprophylaxis_completed_date
                                     THEN 'Gold'
                                 END as tpt_status
                      from tpt_start
                               inner join tpt_completed
                                          on tpt_start.client_id = tpt_completed.client_id
                      where tpt_start.client_id not in
                            (select tb_treatment_completed.client_id from tb_treatment_completed)
                        AND tpt_start.client_id not in (select contraindicated.client_id from contraindicated))
            ,
         tpt_silver_bronze_1 as (select client_id,
                                        Case
                                            When TIMESTAMPDIFF(DAY, tpt_start.inhprophylaxis_started_date, END_DATE) <=
                                                 270
                                                THEN 'Silver'
                                            When TIMESTAMPDIFF(
                                                         DAY, tpt_start.inhprophylaxis_started_date, END_DATE) > 270
                                                THEN 'Bronze1'
                                            END as tpt_status
                                 from tpt_start
                                 where client_id not in
                                       (select tpt_completed.client_id from tpt_completed)
                                   and tpt_start.client_id not in
                                       (select tpt_discontinued.client_id from tpt_discontinued)
                                   and tpt_start.client_id not in
                                       (select tb_treatment_completed.client_id from tb_treatment_completed)
                                   AND
                                     tpt_start.client_id not in (select contraindicated.client_id from contraindicated))
            ,
         tpt_bronze_3 as (select tpt_start.client_id,
                                 Case
                                     When tpt_start.inhprophylaxis_started_date <=
                                          tpt_discontinued.inhprophylaxis_discontinued_date
                                         THEN 'Bronze3'
                                     When tpt_start.inhprophylaxis_started_date >
                                          tpt_discontinued.inhprophylaxis_discontinued_date
                                         THEN 'Bronze3'
                                     END as tpt_status
                          from tpt_start
                                   inner join tpt_discontinued
                                              on tpt_start.client_id = tpt_discontinued.client_id
                          where tpt_start.client_id not in (select tpt_gold.client_id from tpt_gold)
                            and
                              tpt_start.client_id not in (select tpt_silver_bronze_1.client_id from tpt_silver_bronze_1)

                            and tpt_start.client_id not in
                                (select tb_treatment_completed.client_id from tb_treatment_completed)
                            AND tpt_start.client_id not in (select contraindicated.client_id from contraindicated)),

         tpt_bronze_5_changed as (select tb_treatment_completed_2.client_id,
                                         Case
                                             When
                                                 timestampdiff(DAY,
                                                               tb_treatment_completed_2.ActiveTBTreatmentCompletedDate,
                                                               END_DATE) >= 1095
                                                 THEN 'Bronze5'
                                             END as tpt_status
                                  from tb_treatment_completed_2
                                  where tb_treatment_completed_2.client_id not in
                                        (select tpt_gold.client_id from tpt_gold)
                                    and tb_treatment_completed_2.client_id not in
                                        (select tpt_silver_bronze_1.client_id from tpt_silver_bronze_1)
                                    and tb_treatment_completed_2.client_id not in
                                        (select tpt_bronze_3.client_id from tpt_bronze_3)
                                    and tb_treatment_completed_2.client_id not in
                                        (select tpt_bronze_5.client_id from tpt_bronze_5)),
         tpt_bronze_4 as (select tpt_eligible_yes.client_id,
                                 'Bronze4' as tpt_status
                          from tpt_eligible_yes
                          where tpt_eligible_yes.client_id not in (select tpt_gold.client_id from tpt_gold)
                            and tpt_eligible_yes.client_id not in
                                (select tpt_silver_bronze_1.client_id from tpt_silver_bronze_1)
                            and tpt_eligible_yes.client_id not in (select tpt_bronze_3.client_id from tpt_bronze_3)
                            and tpt_eligible_yes.client_id not in
                                (select tb_treatment_completed.client_id from tb_treatment_completed)
                            AND
                              tpt_eligible_yes.client_id not in (select contraindicated.client_id from contraindicated)
                            AND tpt_eligible_yes.client_id not in
                                (select tpt_bronze_5_changed.client_id from tpt_bronze_5_changed)),

         tpt_bronze_6 as (select tpt_completed.client_id,
                                 'Bronze6' as tpt_status
                          from tpt_completed
                          where tpt_completed.client_id not in (select tpt_gold.client_id from tpt_gold)
                            and tpt_completed.client_id not in
                                (select tpt_silver_bronze_1.client_id from tpt_silver_bronze_1)
                            and tpt_completed.client_id not in (select tpt_bronze_3.client_id from tpt_bronze_3)
                            and tpt_completed.client_id not in
                                (select tb_treatment_completed.client_id from tb_treatment_completed)
                            AND tpt_completed.client_id not in (select contraindicated.client_id from contraindicated)
                            AND tpt_completed.client_id not in
                                (select tpt_bronze_5_changed.client_id from tpt_bronze_5_changed)
                            AND tpt_completed.client_id not in (select tpt_bronze_4.client_id from tpt_bronze_4)
                            AND tpt_completed.client_id not in (select tpt_start.client_id from tpt_start)
                          UNION
                          select tpt_discontinued.client_id,
                                 'Bronze6' as tpt_status
                          from tpt_discontinued
                          where tpt_discontinued.client_id not in (select tpt_gold.client_id from tpt_gold)
                            and tpt_discontinued.client_id not in
                                (select tpt_silver_bronze_1.client_id from tpt_silver_bronze_1)
                            and tpt_discontinued.client_id not in (select tpt_bronze_3.client_id from tpt_bronze_3)
                            and tpt_discontinued.client_id not in
                                (select tb_treatment_completed.client_id from tb_treatment_completed)
                            AND
                              tpt_discontinued.client_id not in (select contraindicated.client_id from contraindicated)
                            AND tpt_discontinued.client_id not in
                                (select tpt_bronze_5_changed.client_id from tpt_bronze_5_changed)
                            AND tpt_discontinued.client_id not in (select tpt_bronze_4.client_id from tpt_bronze_4)
                            AND tpt_discontinued.client_id not in (select tpt_start.client_id from tpt_start)
                            AND tpt_discontinued.client_id not in (select tpt_completed.client_id from tpt_completed)),

         tpt_bronze_2 as (select distinct tpt_eligible_no.client_id,
                                          'Bronze2' as tpt_status
                          from tpt_eligible_no
                          where tpt_eligible_no.client_id not in (select tpt_start.client_id from tpt_start)
                            and tpt_eligible_no.client_id not in (select tpt_completed.client_id from tpt_completed)
                            and tpt_eligible_no.client_id not in
                                (select tb_treatment_completed.client_id from tb_treatment_completed)
                            AND tpt_eligible_no.client_id not in (select contraindicated.client_id from contraindicated)
                            and tpt_eligible_no.client_id not in (select tpt_bronze_3.client_id from tpt_bronze_3)
                            and tpt_eligible_no.client_id not in (select tpt_bronze_4.client_id from tpt_bronze_4)
                            and tpt_eligible_no.client_id not in (select tpt_bronze_5.client_id from tpt_bronze_5)
                            and tpt_eligible_no.client_id not in
                                (select tpt_bronze_5_changed.client_id from tpt_bronze_5_changed)
                            and tpt_eligible_no.client_id not in (select tpt_bronze_6.client_id from tpt_bronze_6)
                            and tpt_eligible_no.client_id not in
                                (select tpt_discontinued.client_id from tpt_discontinued)),

         tmp_vl_sent_date as (SELECT encounter_id,
                                     client_id,
                                     viral_load_sent_date                                                                             AS VL_Sent_Date,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_sent_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE viral_load_sent_date <= COALESCE(END_DATE, CURDATE())),
         vl_sent_date as (select * from tmp_vl_sent_date where row_num = 1),
         tmp_switch_sub_date as (SELECT encounter_id,
                                        client_id,
                                        follow_up_date                                                                             AS FollowUpDate,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                 FROM FollowUp
                                 WHERE follow_up_date <= COALESCE(END_DATE, CURDATE())
                                   and regimen_change is not null),
         switch_sub_date as (select * from tmp_switch_sub_date where row_num = 1),

         tmp_vl_performed_date_1 as (SELECT encounter_id,
                                            client_id,
                                            viral_load_perform_date                                                                             AS viral_load_perform_date,
                                            routine_viral_load_test_indication,
                                            viral_load_test_status,
                                            viral_load_count,
                                            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
                                     FROM FollowUp
                                     WHERE art_start_date IS NOT NULL
                                       AND (
                                         (viral_load_perform_date IS NOT NULL AND
                                          viral_load_perform_date <= COALESCE(END_DATE, CURDATE())
                                             )
                                             OR
                                         viral_load_perform_date IS NULL
                                         )),
         tmp_vl_performed_date_2 as (select * from tmp_vl_performed_date_1 where row_num = 1),

         tmp_vl_performed_date_3 as (SELECT tmp_vl_performed_date_2.encounter_id,
                                            tmp_vl_performed_date_2.client_id,
                                            vl_sent_date.VL_Sent_Date,
                                            viral_load_test_status,
                                            case
                                                when tmp_vl_performed_date_2.viral_load_perform_date <
                                                     vl_sent_date.VL_Sent_Date
                                                    then null
                                                else tmp_vl_performed_date_2.viral_load_perform_date end as viral_load_perform_date,
                                            case
                                                when tmp_vl_performed_date_2.viral_load_perform_date <
                                                     vl_sent_date.VL_Sent_Date
                                                    then null
                                                else tmp_vl_performed_date_2.viral_load_test_status end  as viral_load_status,
                                            CASE
                                                WHEN tmp_vl_performed_date_2.viral_load_count > 0 AND
                                                     tmp_vl_performed_date_2.viral_load_perform_date >=
                                                     vl_sent_date.VL_Sent_Date
                                                    THEN CAST(tmp_vl_performed_date_2.viral_load_count AS DECIMAL(12, 2))
                                                ELSE NULL END                                            AS viral_load_count,
                                            CASE
                                                WHEN
                                                    viral_load_test_status IS NULL AND
                                                    tmp_vl_performed_date_2.viral_load_perform_date >=
                                                    vl_sent_date.VL_Sent_Date
                                                    THEN
                                                    NULL
                                                WHEN tmp_vl_performed_date_2.viral_load_perform_date >=
                                                     vl_sent_date.VL_Sent_Date AND
                                                     (viral_load_test_status LIKE 'Det%'
                                                         OR viral_load_test_status LIKE 'Uns%'
                                                         OR viral_load_test_status LIKE 'High VL%'
                                                         OR viral_load_test_status LIKE 'Low Level Viremia%')
                                                    THEN
                                                    'U'
                                                WHEN tmp_vl_performed_date_2.viral_load_perform_date >=
                                                     vl_sent_date.VL_Sent_Date AND
                                                     (viral_load_test_status LIKE 'Su%'
                                                         OR viral_load_test_status LIKE 'Undet%')
                                                    THEN
                                                    'S'
                                                WHEN
                                                    tmp_vl_performed_date_2.viral_load_perform_date >=
                                                    vl_sent_date.VL_Sent_Date AND
                                                    (ISNULL(viral_load_count) > CAST(50 AS float)
                                                        )
                                                    THEN
                                                    'U'
                                                WHEN
                                                    tmp_vl_performed_date_2.viral_load_perform_date >=
                                                    vl_sent_date.VL_Sent_Date AND
                                                    (ISNULL(viral_load_count) <= CAST(50 AS float)
                                                        )
                                                    THEN
                                                    'S'
                                                ELSE
                                                    NULL
                                                END                                                      AS viral_load_status_inferred,
                                            CASE
                                                WHEN vl_sent_date.VL_Sent_Date IS NOT NULL
                                                    THEN vl_sent_date.VL_Sent_Date
                                                WHEN tmp_vl_performed_date_2.viral_load_perform_date IS NOT NULL
                                                    THEN tmp_vl_performed_date_2.viral_load_perform_date
                                                ELSE NULL END                                            AS viral_load_ref_date,
                                            tmp_vl_performed_date_2.routine_viral_load_test_indication
                                     FROM tmp_vl_performed_date_2
                                              LEFT JOIN vl_sent_date
                                                        ON tmp_vl_performed_date_2.client_id = vl_sent_date.client_id),

         tmp_1 as (select client_id,
                          encounter_id,
                          follow_up_date,
                          ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                   from FollowUp
                   where follow_up_status is not null
                     and follow_up_date <= END_DATE),
         tmp_2 as (select *
                   from tmp_1
                   where row_num = 1),

         tmp_3 as (select f_case.client_id,
                          f_case.encounter_id,
                          f_case.weight,
                          f_case.hiv_confirmed_date,
                          f_case.art_start_date,
                          f_case.follow_up_date,
                          f_case.pregnancy_status,
                          regimen                                            as ARVDispendsedDoseCode,
                          f_case.arv_dose,
                          f_case.next_visit_date,
                          f_case.follow_up_status,
                          f_case.nutritional_screening_result,
                          f_case.ns_adult,
                          f_case.art_dose_end_date,
                          f_case.adherence,
                          f_case.breastfeeding_status,
                          f_case.cd4_count,
                          f_case.dsd_category,
                          vlperfdate.viral_load_perform_date,
                          vlperfdate.viral_load_test_status,
                          vlperfdate.viral_load_count,
                          vlsentdate.VL_Sent_Date,
                          vlperfdate.viral_load_ref_date,
                          sub_switch_date.FollowupDate,
                          CASE

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NULL
                                      AND f_case.follow_up_status = 'Restart medication')
                                  THEN DATE_ADD(f_case.follow_up_date, INTERVAL 91 DAY)

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NULL
                                      AND sub_switch_date.FollowupDate IS NOt NULL
                                      )
                                  THEN DATE_ADD(sub_switch_date.FollowupDate, INTERVAL 181 DAY)

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NULL
                                      AND f_case.pregnancy_status = 'Yes'
                                      AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                        COALESCE(END_DATE, CURDATE())) > 90)
                                  THEN DATE_ADD(f_case.art_start_date, INTERVAL 91 DAY)

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NULL
                                      AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                        COALESCE(END_DATE, CURDATE())) <= 180)
                                  THEN NULL


                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NULL
                                      AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                        COALESCE(END_DATE, CURDATE())) > 180)
                                  THEN DATE_ADD(f_case.art_start_date, INTERVAL 181 DAY)

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NOT NULL
                                      AND vlperfdate.VL_Sent_Date < f_case.follow_up_date)
                                      AND (f_case.follow_up_status = 'Restart medication')
                                  THEN DATE_ADD(f_case.follow_up_date, INTERVAL 91 DAY)

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NOT NULL
                                      AND vlperfdate.VL_Sent_Date < sub_switch_date.FollowupDate
                                      AND sub_switch_date.FollowupDate IS NOT NULL
                                      )
                                  THEN DATE_ADD(sub_switch_date.FollowupDate, INTERVAL 181 DAY)

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NOT NULL
                                      AND vlperfdate.viral_load_status_inferred = 'U')
                                  THEN DATE_ADD(vlperfdate.VL_Sent_Date, INTERVAL 91 DAY)

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NOT NULL
                                      AND
                                   (f_case.pregnancy_status = 'Yes' OR f_case.breastfeeding_status = 'Yes')
                                      AND vlperfdate.routine_viral_load_test_indication in
                                          ('First viral load test at 6 months or longer post ART',
                                           'Viral load after EAC: repeat viral load where initial viral load greater than 50 and less than 1000 copies per ml',
                                           'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml'))
                                  THEN DATE_ADD(vlperfdate.VL_Sent_Date, INTERVAL 91 DAY)

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NOT NULL
                                      AND
                                   (f_case.pregnancy_status = 'Yes' OR f_case.breastfeeding_status = 'Yes')
                                      AND vlperfdate.routine_viral_load_test_indication IS NOT NULL
                                      AND vlperfdate.routine_viral_load_test_indication not in
                                          ('First viral load test at 6 months or longer post ART',
                                           'Viral load after EAC: repeat viral load where initial viral load greater than 50 and less than 1000 copies per ml',
                                           'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml'))
                                  THEN DATE_ADD(vlperfdate.VL_Sent_Date, INTERVAL 181 DAY)


                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NOT NULL)
                                  THEN DATE_ADD(vlperfdate.VL_Sent_Date, INTERVAL 365 DAY)

                              ELSE DATE_ADD(END_DATE, INTERVAL 100 YEAR) End AS eligiblityDate,

                          CASE

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NULL
                                      AND f_case.follow_up_status = 'Restart medication')
                                  THEN 'client restarted ART'

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NULL
                                      AND sub_switch_date.FollowupDate IS NOt NULL
                                      )
                                  THEN 'Regimen Change'


                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NULL
                                      AND f_case.pregnancy_status = 'Yes'
                                      AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                        COALESCE(END_DATE, CURDATE())) > 90)
                                  THEN 'First VL for Pregnant'

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NULL
                                      AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                        COALESCE(END_DATE, CURDATE())) <= 180)
                                  THEN 'N/A'

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NULL
                                      AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                        COALESCE(END_DATE, CURDATE())) > 180)
                                  THEN 'First VL'


                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NOT NULL
                                      AND vlperfdate.VL_Sent_Date < f_case.follow_up_date)
                                      AND (f_case.follow_up_status = 'Restart medication')
                                  THEN 'client restarted ART'

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NOT NULL
                                      AND vlperfdate.VL_Sent_Date < sub_switch_date.FollowupDate
                                      AND sub_switch_date.FollowupDate IS NOT NULL
                                      )
                                  THEN 'Regimen Change'

                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NOT NULL
                                      AND vlperfdate.viral_load_status_inferred = 'U')
                                  THEN 'Repeat/Confirmatory Viral Load test'

                              WHEN
                                  (vlperfdate.viral_load_status_inferred IS NOT NULL
                                      AND
                                   (f_case.pregnancy_status = 'Yes' OR f_case.breastfeeding_status = 'Yes'))
                                  THEN 'Pregnant/Breastfeeding and needs retesting'


                              WHEN
                                  (vlperfdate.VL_Sent_Date IS NOT NULL)
                                  THEN 'Annual Viral Load Test'

                              ELSE 'Unassigned' End                          AS vl_status_final,
                          CASE

                              WHEN
                                  (vlperfdate.viral_load_perform_date IS NULL
                                      AND f_case.follow_up_status = 'Restart medication')
                                  THEN 'client restarted ART'

                              WHEN
                                  (vlperfdate.viral_load_perform_date IS NULL
                                      AND sub_switch_date.FollowupDate IS NOt NULL
                                      )
                                  THEN 'Regimen Change'


                              WHEN
                                  (vlperfdate.viral_load_perform_date IS NULL
                                      AND f_case.pregnancy_status = 'Yes'
                                      AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                        COALESCE(END_DATE, CURDATE())) > 90)
                                  THEN 'First VL for Pregnant'

                              WHEN
                                  (vlperfdate.viral_load_perform_date IS NULL
                                      AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                        COALESCE(END_DATE, CURDATE())) <= 180)
                                  THEN 'N/A'

                              WHEN
                                  (vlperfdate.viral_load_perform_date IS NULL
                                      AND TIMESTAMPDIFF(DAY, f_case.art_start_date,
                                                        COALESCE(END_DATE, CURDATE())) > 180)
                                  THEN 'First VL'


                              WHEN
                                  (vlperfdate.viral_load_perform_date IS NOT NULL
                                      AND vlperfdate.viral_load_perform_date < f_case.follow_up_date)
                                      AND (f_case.follow_up_status = 'Restart medication')
                                  THEN 'client restarted ART'

                              WHEN
                                  (vlperfdate.viral_load_perform_date IS NOT NULL
                                      AND vlperfdate.viral_load_perform_date < sub_switch_date.FollowupDate
                                      AND sub_switch_date.FollowupDate IS NOT NULL
                                      )
                                  THEN 'Regimen Change'

                              WHEN
                                  (vlperfdate.viral_load_perform_date IS NOT NULL
                                      AND vlperfdate.viral_load_status_inferred = 'U')
                                  THEN 'Repeat/Confirmatory Viral Load test'

                              WHEN
                                  (vlperfdate.viral_load_status_inferred IS NOT NULL
                                      AND
                                   (f_case.pregnancy_status = 'Yes' OR f_case.breastfeeding_status = 'Yes'))
                                  THEN 'Pregnant/Breastfeeding and needs retesting'


                              WHEN
                                  (vlperfdate.viral_load_perform_date IS NOT NULL)
                                  THEN 'Annual Viral Load Test'

                              ELSE 'Unassigned' End                          AS VL_STATUS_COVERAGE


                   FROM FollowUp AS f_case
                            INNER JOIN tmp_2
                                       ON f_case.encounter_id = tmp_2.encounter_id
                            LEFT JOIN tmp_vl_performed_date_3 as vlperfdate
                                      ON vlperfdate.client_id = f_case.client_id
                            Left join vl_sent_date as vlsentdate
                                      ON vlsentdate.client_id = f_case.client_id
                            Left join switch_sub_date as sub_switch_date
                                      ON sub_switch_date.client_id = f_case.client_id
                            Left join all_art_not_started on f_case.client_id = all_art_not_started.client_id),


         vl_eligibility as (select tmp_3.client_id,
                                   vl_status_final as vl_status
                            from tmp_3
                                     Left join all_art_not_started
                                               on all_art_not_started.client_id = tmp_3.client_id

                            where tmp_3.client_id not in
                                  (select all_art_not_started_status.client_id from all_art_not_started_status)
                            union
                            select client_id, art_start_date
                            from all_art_not_started_status),


         tmp_dsd_cat as (SELECT client_id,
                                dsd_category,
                                follow_up_date                                                                              AS dsd_fdate,
                                assessment_status,
                                ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY assessment_date DESC, encounter_id DESC) AS row_num
                         FROM FollowUp
                         WHERE follow_up_date <= END_DATE
                           and assessment_date is not null),
         dsd_cat as (select *
                     from tmp_dsd_cat
                     where row_num = 1
                       and client_id not in
                           (select all_art_not_started_status.client_id from all_art_not_started_status)),
         tpt as (select client_id, tpt_status
                 from tpt_gold
                 union
                 select client_id
                      , tpt_status
                 from tpt_silver_bronze_1
                 union
                 select client_id
                      , tpt_status
                 from tpt_bronze_2
                 union
                 select client_id
                      , tpt_status
                 from tpt_bronze_3
                 union
                 select client_id
                      , tpt_status
                 from tpt_bronze_4
                 union
                 select client_id
                      , tpt_status
                 from tpt_bronze_5
                 union
                 select client_id
                      , tpt_status
                 from contraindicated
                 union
                 select client_id
                      , tpt_status
                 from tpt_bronze_5_changed
                 union
                 select client_id
                      , tpt_status
                 from tpt_bronze_6),


         latest_follow_up AS (SELECT fu.client_id,
                                     fu.follow_up_date,
                                     fu.follow_up_status                                                                                 AS final_follow_up_status,
                                     fu.eligible_for_cxca_screening,
                                     fu.reason_for_not_being_eligible,
                                     fu.other_reason_for_not_being_eligible_for_cxca,
                                     fu.screening_status,
                                     fu.hiv_confirmed_date,
                                     fu.art_start_date,
                                     ROW_NUMBER() OVER (PARTITION BY fu.client_id ORDER BY fu.follow_up_date DESC, fu.encounter_id DESC) AS rn
                              FROM FollowUp fu
                              WHERE fu.follow_up_date <= END_DATE),


         prev_screening AS (SELECT sc.client_id,
                                   sc.follow_up_date,
                                   sc.ccs_via_result,
                                   sc.via_date,
                                   sc.ccs_treat_received_date,
                                   sc.date_patient_referred_out,
                                   sc.biopsy_result,
                                   sc.ccs_hpv_result,
                                   sc.hpv_dna_result_received_date,
                                   sc.cytology_result,
                                   sc.cytology_result_date,
                                   sc.colposcopy_exam_finding,
                                   sc.biopsy_result_received_date,
                                   ROW_NUMBER() OVER (PARTITION BY sc.client_id ORDER BY sc.follow_up_date DESC, sc.encounter_id DESC) AS rn
                            FROM FollowUp sc
                            WHERE sc.follow_up_date <= END_DATE
                              AND sc.screening_status = 'Cervical cancer screening performed'),

         cxca_eligibility_base_clients AS (SELECT lf.*,
                                                  ps.follow_up_date AS previous_screening_follow_up_date,
                                                  ps.ccs_via_result,
                                                  ps.via_date,
                                                  ps.ccs_treat_received_date,
                                                  ps.date_patient_referred_out,
                                                  ps.biopsy_result,
                                                  ps.ccs_hpv_result,
                                                  ps.hpv_dna_result_received_date,
                                                  ps.cytology_result,
                                                  ps.cytology_result_date,
                                                  ps.colposcopy_exam_finding,
                                                  ps.biopsy_result_received_date,

                                                  c.sex,
                                                  c.Age,

                                                  -- This CASE statement generates the detailed Eligibility Status
                                                  CASE
                                                      -- TC-02, TC-03, ...: Not eligible by user/form (e.g., Hysterectomy, etc.)
                                                      WHEN ps.client_id IS NULL
                                                          AND lf.eligible_for_cxca_screening = 'No'
                                                          THEN CONCAT('Not Eligible ',
                                                                      COALESCE(lf.reason_for_not_being_eligible,
                                                                               lf.other_reason_for_not_being_eligible_for_cxca))

                                                      -- TC-19: Biopsy confirmed cancer (Red rule source)
                                                      WHEN (ps.biopsy_result = 'Carcinoma in situ'
                                                          OR ps.biopsy_result = 'Invasive cervical cancer'
                                                          OR ps.biopsy_result = 'Other')
                                                          AND (ps.ccs_treat_received_date <= END_DATE
                                                              OR ps.date_patient_referred_out <= END_DATE)
                                                          THEN 'Not Eligible Confirmed Cirvical Cancer'

                                                      -- TC-06: User-Labeled as Eligible
                                                      WHEN ps.client_id IS NULL
                                                          AND
                                                           lf.screening_status != 'Cervical cancer screening performed'
                                                          AND lf.eligible_for_cxca_screening = 'Yes'
                                                          THEN 'Eligible Labeled by User'

                                                      -- TC-07: Never screened/assessed
                                                      WHEN ps.client_id IS NULL
                                                          AND lf.eligible_for_cxca_screening IS NULL
                                                          THEN 'Eligible Never Screened/Assessed'

                                                      -- TC-08: Unknown VIA result
                                                      WHEN ps.ccs_via_result = 'Unknown'
                                                          THEN 'Eligible Unknown VIA Screening Result'

                                                      -- TC-09: VIA negative >2 years ago
                                                      WHEN (TIMESTAMPDIFF(DAY, ps.via_date, END_DATE)) > 730
                                                          AND ps.ccs_via_result = 'VIA negative'
                                                          THEN 'Eligible Needs Re-Screening'

                                                      -- TC-10, TC-11 (VIA Positive logic)
                                                      WHEN ps.ccs_via_result = 'VIA positive: eligible for cryo/thermo-coagula'
                                                          THEN
                                                          CASE
                                                              -- TC-10: Treatment/Referral Given > 1 year (Needs follow-up)
                                                              WHEN
                                                                  ((TIMESTAMPDIFF(DAY, ps.ccs_treat_received_date, END_DATE)) >
                                                                   365
                                                                      OR
                                                                   (TIMESTAMPDIFF(DAY, ps.date_patient_referred_out, END_DATE)) >
                                                                   365)
                                                                      AND ps.biopsy_result IS NULL
                                                                  THEN 'Eligible Post Treatment/Referral Follow-Up'
                                                              -- TC-11: No Treatment/Referral
                                                              WHEN ps.ccs_treat_received_date IS NULL
                                                                  AND ps.date_patient_referred_out IS NULL
                                                                  THEN 'Eligible Treatment Not Received or Not Referred'
                                                              ELSE 'Not Eligible (Screening Up-to-Date)' -- Default if treatment received recently
                                                              END

                                                      -- TC-12: VIA Positive & Not Eligible for Cryo OR Suspected Cancer & NO BIOPSY
                                                      WHEN (ps.ccs_via_result =
                                                            'VIA positive: non-eligible for cryo/thermo-coagula'
                                                          OR ps.ccs_via_result = 'suspected cervical cancer')
                                                          AND ps.biopsy_result IS NULL
                                                          THEN 'Eligible Biopsy Test Not Done'

                                                      -- TC-13: HPV negative >3 years ago
                                                      WHEN ps.ccs_hpv_result = 'Negative result'
                                                          AND
                                                           TIMESTAMPDIFF(DAY, ps.hpv_dna_result_received_date, END_DATE) >
                                                           1095
                                                          THEN 'Eligible Needs Re-Screening'

                                                      -- TC-14: HPV positive, needs VIA triage
                                                      WHEN ps.ccs_via_result IS NULL
                                                          AND ps.ccs_hpv_result = 'Positive'
                                                          THEN 'Eligible Needs VIA Triage'

                                                      -- TC-15: Cytology Negative >3 years ago
                                                      WHEN ps.cytology_result = 'Negative result'
                                                          AND
                                                           TIMESTAMPDIFF(DAY, ps.cytology_result_date, END_DATE) > 1095
                                                          THEN 'Eligible Needs Re-Screening'

                                                      -- TC-16, TC-17, TC-18 (Cytology Positive logic)
                                                      WHEN (ps.cytology_result =
                                                            'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear'
                                                          OR ps.cytology_result = '> Ascus')
                                                          THEN
                                                          CASE
                                                              -- TC-16: NO HPV Test
                                                              WHEN ps.hpv_dna_result_received_date IS NULL
                                                                  THEN 'Eligible Needs HPV Triage'
                                                              -- TC-17: HPV Negative > 2 years (Needs Reassessment)
                                                              WHEN ps.ccs_hpv_result = 'Negative result'
                                                                  AND
                                                                   TIMESTAMPDIFF(DAY, ps.hpv_dna_result_received_date, END_DATE) >
                                                                   730
                                                                  THEN 'Eligible Needs Re-Screening'
                                                              -- TC-18: HPV Positive & Needs Colposcopy
                                                              WHEN ps.ccs_hpv_result = 'Positive'
                                                                  AND ps.colposcopy_exam_finding IS NULL
                                                                  THEN 'Eligible Needs Colposcopy Test'
                                                              ELSE 'Not Eligible (Screening Up-to-Date)'
                                                              END

                                                      -- TC-20: Biopsy CIN-1/CIN-2 > 1 year
                                                      WHEN (ps.biopsy_result = 'CIN (1-3)'
                                                          OR ps.biopsy_result = 'CIN-2')
                                                          AND
                                                           TIMESTAMPDIFF(DAY, ps.biopsy_result_received_date, END_DATE) >
                                                           365
                                                          THEN 'Eligible Needs Re-Screening'

                                                      -- DEFAULT (CATCH-ALL): Client is not due for screening / Screening Done (Green rule source)
                                                      ELSE 'Not Eligible (Screening Up-to-Date)'
                                                      END           AS EligibilityStatus
                                           FROM latest_follow_up lf
                                                    LEFT JOIN prev_screening ps
                                                              ON lf.client_id = ps.client_id AND ps.rn = 1 -- Join to most recent screening
                                                    JOIN tmp_address c ON lf.client_id = c.client_id
                                           WHERE lf.rn = 1),

         cervical as (SELECT base.*, -- Selects all client info AND the EligibilityStatus

                             -- Calculate Age based on END_DATE
                             age     AS current_age,

                             -- This CASE statement implements the color-coded rules
                             CASE
                                 -- RULE 1: Blue (Not Applicable)
                                 -- Female <25 OR Female >65 OR All Males, Follow-Up Status = TO, DEAD, STOP
                                 WHEN base.sex = 'Male'
                                     OR age < 25
                                     OR age > 65
                                     OR
                                      base.final_follow_up_status IN ('Dead', 'Transferred out', 'Stop all', 'Ran away')
                                     THEN 'Blue'

                                 -- RULE 2: Black (ART Not Started)
                                 -- HIV Confirmation Date but no ART START DATE
                                 WHEN base.hiv_confirmed_date IS NOT NULL AND base.art_start_date IS NULL
                                     THEN 'Black'

                                 -- RULE 3: Red (Confirmed CXCA)
                                 -- Clients with confirmed cancer are excluded from screening eligibility (using the detailed status)
                                 WHEN base.EligibilityStatus = 'Not Eligible Confirmed Cirvical Cancer'
                                     THEN 'Red'

                                 -- The remaining rules apply to the active, 25-65 female cohort

                                 -- RULE 4: Green (Previously Screened / Screening Done)
                                 -- Maps to the catch-all status where screening is up-to-date
                                 WHEN base.EligibilityStatus = 'Not Eligible (Screening Up-to-Date)'
                                     THEN 'Green'

                                 -- RULE 5: Yellow (Eligible for Screening/RX)
                                 -- Maps to *any* status that requires screening/follow-up action
                                 WHEN base.EligibilityStatus LIKE 'Eligible%'
                                     THEN 'Yellow'

                                 -- RULE 6: White (Not Eligible - Other Reasons)
                                 -- Maps to other 'Not Eligible' statuses (e.g., Hysterectomy, etc.)
                                 WHEN base.EligibilityStatus LIKE 'Not Eligible%'
                                     THEN 'White'

                                 -- Fallback
                                 ELSE 'Unknown Status'
                                 END AS Cervical_status

                      FROM cxca_eligibility_base_clients AS base),


         asm as (select client_id
                      , assessment_status
                      , dsd_category
                 from dsd_cat
                 union
                 select client_id
                      , ''
                      , 'ART not started'
                 from all_art_not_started_status),

         offer_list as (select client.client_id,
                               client.mrn,
                               client.uan,
                               client.sex,
                               client.patient_uuid,
                               client.patient_name,
                               offer.offered_date,
                               visit_date_ict_offer,
                               priority_criteria,
                               offered,
                               ict_serial_number,
                               offer.yes,
                               offer.no,
                               elicited,
                               high_risk_for_ipv,
                               accepted,
                               accepted_date,
                               linked_to_appropriate_care,
                               specify_other_adverse_event_type,
                               reason_ict_service_not_accepted,
                               adverse_event_type,
                               ROW_NUMBER() over (PARTITION BY client.client_id ORDER BY offered_date DESC ) as row_num
                        from mamba_flat_encounter_ict_general ict_general
                                 join mamba_flat_encounter_ict_offer offer on ict_general.client_id = offer.client_id
                                 join mamba_dim_client client on ict_general.client_id = client.client_id
                                 join tmp_address on tmp_address.client_id = client.client_id
                        where offered_date <= END_DATE),
         tmp_offer as (select * from offer_list where row_num = 1),
         offer as (select tmp_offer.client_id,
                          offered_date,
                          CASE
                              WHEN offered_date is null and final_follow_up_status NOT IN ('Dead', 'Transferred Out')
                                  THEN 'Never Screened for ICT'
                              WHEN offered_date IS NOT NULL AND
                                   DATE_ADD(offered_date, INTERVAL 730 DAY) <= END_DATE
                                  THEN 'Screened for ICT Previously (Not Eligible)'
                              WHEN offered_date IS NOT NULL AND
                                   DATE_ADD(offered_date, INTERVAL 730 DAY) > END_DATE
                                  THEN 'Currently Eligible for Rescreening'
                              WHEN final_follow_up_status IN ('Dead', 'Transferred Out')
                                  THEN 'Not Applicable' END AS `ict_eligibility`

                   from tmp_offer
                            join latest_follow_up lfu on tmp_offer.client_id = lfu.client_id)

    select tmp_address.patient_name                                          AS `Patient Name`,
           tmp_address.patient_uuid                                          AS `UUID`,
           tmp_address.mrn,
           tmp_address.uan,
           tmp_address.patientname,
           tmp_address.age,
           tmp_address.sex,
           tmp_address.Adrress,
           tpt.tpt_status,
           cervical.Cervical_status,

           case

               when vl_status_final = 'N/A' THEN 'Not Applicable'
               when eligiblityDate <= COALESCE(END_DATE, CURDATE())
                   THEN 'Eligible for Viral Load'
               when eligiblityDate > COALESCE(END_DATE, CURDATE())
                   THEN 'Viral Load Done' -- 'Viral Load Done'
               when tmp_3.art_start_date is NULL and follow_up_status is null THEN 'Not Started ART'

#                when vl_eligibility.vl_status is not null then vl_eligibility.vl_status
               Else 'undetermined_VL' end                                    as `Viral Load Eligibility Status`
            ,
           offer.ict_eligibility                                             as `ICT Eligibility Status`,
           offer.offered_date as `Latest ICT Offer Date GC.`,
           offer.offered_date as `Latest ICT Offer Date EC.`,
           case
               when asm.assessment_status is not null then asm.assessment_status
               Else '' end                                                   as `DSD Assesment Status`
            ,
           case
               when tmp_3.dsd_category is not null then tmp_3.dsd_category
               Else 'Not enrolled to any DSD' end                            as dsd_category
            ,

           CASE tmp_3.follow_up_status
               WHEN 'Alive' THEN 'Alive on ART'
               WHEN 'Restart medication' THEN 'Restart'
               WHEN 'Transferred out' THEN 'TO'
               WHEN 'Stop all' THEN 'Stop'
               WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
               WHEN 'Ran away' THEN 'Drop'
               END                                                           as follow_up_status,

           tmp_3.follow_up_date                                              as `Follow Up Date`,
           tmp_3.follow_up_date                                              as `Follow Up Date EC.`,
           tmp_3.art_start_date                                              as `Art Start Date`,
           tmp_3.art_start_date                                              as `Art Start Date EC.`,
           tmp_3.hiv_confirmed_date as `HIV Confirmed Date GC.`,
           tmp_3.hiv_confirmed_date as `HIV Confirmed Date EC.`,
           tmp_3.next_visit_date                                             as `Nex Visit Date`,
           tmp_3.next_visit_date                                             as `Nex Visit Date EC.`,
           timestampdiff(month, tmp_3.art_start_date, tmp_3.next_visit_date) as monthsonART,
           tmp_3.arv_dose                                                    as artdosecode,
           tmp_3.ARVDispendsedDoseCode                                       as regimen,
           tmp_3.weight,
           tmp_3.adherence                                                   AS Adherance,
           tmp_3.nutritional_screening_result                                As NutritionalStatus,
           tmp_3.ns_adult                                                    As Nstatus
    from tmp_address
             left join tpt
                       on tpt.client_id = tmp_address.client_id
             left join vl_eligibility on vl_eligibility.client_id = tmp_address.client_id
             left join asm on asm.client_id = tmp_address.client_id
             left join cervical on cervical.client_id = tmp_address.client_id
             left join tmp_3 on tmp_3.client_id = tmp_address.client_id
             left join offer on tmp_address.client_id = offer.client_id

    where (
        (START_NV_DATE IS NULL OR END_NV_DATE IS NULL)
            OR (tmp_3.next_visit_date BETWEEN START_NV_DATE AND END_NV_DATE)
        )
      AND (
        (TYPE_OF_CLIENT = 'TX_CURR'
            AND tmp_3.follow_up_status IN ('Alive', 'Restart medication')
            AND tmp_3.art_dose_end_date >= END_DATE
            )
            OR TYPE_OF_CLIENT = 'ALL'
        )
      AND (
        PATIENT_GUID IS NULL
            OR tmp_address.patient_uuid = PATIENT_GUID
        )
    ORDER BY tmp_address.PatientName;

END //
DELIMITER ;