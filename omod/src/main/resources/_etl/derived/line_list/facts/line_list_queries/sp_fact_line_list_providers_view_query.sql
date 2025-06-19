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
                             follow_up_date_followup_            as follow_up_date,
                             art_antiretroviral_start_date       as art_start_date,
                             date_started_on_tuberculosis_prophy as inhprophylaxis_started_date,
                             date_completed_tuberculosis_prophyl as inhprophylaxis_completed_date,
                             date_discontinued_tuberculosis_prop as inhprophylaxis_discontinued_date,
                             date_active_tbrx_completed          as date_active_tb_treatment_completed,
                             weight_text_                        as weight,
                             eligible_for_tpt,
                             reason_not_eligible_for_tuberculosi,
                             date_of_reported_hiv_viral_load     as viral_load_sent_date,
                             regimen_change,
                             date_viral_load_results_received    as viral_load_perform_date,
                             viral_load_test_status,
                             hiv_viral_load                      as viral_load_count,
                             regimen,
                             cd4_count,
                             nutritional_status_of_adult         as ns_adult,
                             nutritional_status_of_older_child_a as ns_child,
                             nutritional_screening_result,
                             date_of_event                       as hiv_confirmed_date,
                             pregnancy_status,
                             next_visit_date,
                             antiretroviral_art_dispensed_dose_i    arv_dose,
                             treatment_end_date                     art_dose_end_date,
                             currently_breastfeeding_child       as breastfeeding_status,
                             adherence,
                             date_of_last_menstrual_period_lmp_  as lmp_date,
                             pre_test_counselling_for_cervical_c as councelling_given,
                             biopsy_result,
                             assessment_date,
                             cervical_cancer_screening_status,
                             next_follow_up_screening_date       as ccs_next_date,
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
                             )                                   AS routine_viral_load_test_indication,
                             dsd_category
                      FROM mamba_flat_encounter_follow_up follow_up
                               left JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                         ON follow_up.encounter_id = follow_up_1.encounter_id
                               left JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                         ON follow_up.encounter_id = follow_up_2.encounter_id
                               left JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id
                               left JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                         ON follow_up.encounter_id = follow_up_4.encounter_id),

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
                              WHERE viral_load_sent_date <= END_DATE),
         vl_sent_date as (select * from tmp_vl_sent_date where row_num = 1),
         tmp_switch_sub_date as (SELECT encounter_id,
                                        client_id,
                                        follow_up_date                                                                             AS FollowUpDate,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                 FROM FollowUp
                                 WHERE follow_up_date <= END_DATE
                                   and regimen_change is not null),
         switch_sub_date as (select * from tmp_switch_sub_date where row_num = 1),

         vl_perf_date_1 as (SELECT encounter_id,
                                   client_id,
                                   viral_load_perform_date                                                                             AS viral_load_perform_date,
                                   ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
                            FROM FollowUp
                            where follow_up_status is not null
                              and art_start_date is not null
                              and viral_load_perform_date <= END_DATE),
         tmp_vl_perf_date_2 as (select * from vl_perf_date_1 where row_num = 1),
         vl_perf_date_3 as (select FollowUp.encounter_id,
                                   FollowUp.client_id,
                                   FollowUp.viral_load_perform_date,
                                   FollowUp.viral_load_test_status,

                                   viral_load_count,
                                   CASE
                                       WHEN vl_sent_date.VL_Sent_Date IS NOT NULL THEN vl_sent_date.VL_Sent_Date
                                       WHEN FollowUp.viral_load_perform_date IS NOT NULL
                                           THEN FollowUp.viral_load_perform_date
                                       ELSE FollowUp.viral_load_perform_date END AS viral_load_ref_date
                            from FollowUp
                                     INNER JOIN tmp_vl_perf_date_2
                                                ON FollowUp.encounter_id = tmp_vl_perf_date_2.encounter_id
                                     LEFT JOIN vl_sent_date
                                               ON FollowUp.client_id = vl_sent_date.client_id),

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
                          regimen               as ARVDispendsedDoseCode,
                          f_case.arv_dose,
                          f_case.next_visit_date,
                          f_case.follow_up_status,
                          f_case.nutritional_screening_result,
                          f_case.ns_adult,
                          f_case.art_dose_end_date,
                          f_case.adherence,
                          f_case.breastfeeding_status,
                          f_case.cd4_count,
                          vlperfdate.viral_load_perform_date,
                          vlperfdate.viral_load_test_status,
                          vlperfdate.viral_load_count,
                          vlsentdate.VL_Sent_Date,
                          vlperfdate.viral_load_ref_date,
                          sub_switch_date.FollowupDate,
                          CASE
                              WHEN
                                  (
                                      (f_case.follow_up_status = 'Restart medication')
                                      )
                                  THEN date_add(f_case.follow_up_date, interval 91 day)
                              WHEN
                                  (
                                      (f_case.routine_viral_load_test_indication = 'Three months after delivery')
                                      )
                                  THEN date_add(f_case.follow_up_date, interval 91 day)
                              WHEN
                                  (
                                      (f_case.routine_viral_load_test_indication =
                                       'Six months after the first viral load test at postnatal period')
                                      )
                                  THEN date_add(f_case.follow_up_date, interval 181 day)
                              WHEN sub_switch_date.FollowupDate IS not NULL
                                  THEN date_add(sub_switch_date.FollowupDate, interval 181 day)
                              WHEN ((timestampdiff(month, f_case.art_start_date, END_DATE) <= 12)
                                  AND (f_case.pregnancy_status is null OR f_case.pregnancy_status = 'No')
                                  AND sub_switch_date.FollowupDate IS NULL
                                  AND vlperfdate.viral_load_ref_date IS NULL
                                  AND (vlperfdate.viral_load_count IS NULL))
                                  THEN date_add(f_case.art_start_date, interval 181 day)
                              WHEN ((timestampdiff(month, f_case.art_start_date, END_DATE) <= 12)
                                  AND (f_case.pregnancy_status is null OR f_case.pregnancy_status = 'No')
                                  AND (f_case.breastfeeding_status is null OR f_case.breastfeeding_status = 'No')
                                  AND (sub_switch_date.FollowupDate IS NULL or (
                                      sub_switch_date.FollowupDate is not null and
                                      sub_switch_date.FollowupDate < vlperfdate.viral_load_ref_date))
                                  AND vlperfdate.viral_load_ref_date IS NOT NULL
                                  AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51))
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, interval 181 day)
                              WHEN ((timestampdiff(month, f_case.art_start_date, END_DATE) > 12)
                                  AND (f_case.pregnancy_status is null OR f_case.pregnancy_status = 'No')
                                  AND (f_case.breastfeeding_status is null OR f_case.breastfeeding_status = 'No')
                                  AND (sub_switch_date.FollowupDate IS NULL OR
                                       (sub_switch_date.FollowupDate is not null and
                                        sub_switch_date.FollowupDate <= vlperfdate.viral_load_ref_date))
                                  AND vlperfdate.viral_load_ref_date IS NOT NULL
                                  AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51))
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, interval 365 day)
                              WHEN ((timestampdiff(month, f_case.art_start_date, END_DATE) > 12)
                                  AND (f_case.pregnancy_status is null OR f_case.pregnancy_status = 'No')
                                  AND (f_case.breastfeeding_status is null OR f_case.breastfeeding_status = 'No')
                                  AND sub_switch_date.FollowupDate IS NULL
                                  AND
                                    vlperfdate.viral_load_ref_date IS NULL
                                  AND (vlperfdate.viral_load_count IS NULL))
                                  THEN END_DATE
                              WHEN ((timestampdiff(month, f_case.art_start_date, END_DATE) <= 3)
                                  AND (f_case.pregnancy_status = 'Yes')
                                  AND (f_case.breastfeeding_status is null or f_case.breastfeeding_status = 'No')
                                  AND sub_switch_date.FollowupDate IS NULL
                                  AND vlperfdate.viral_load_ref_date IS NULL
                                  AND (vlperfdate.viral_load_count IS NULL)
                                  )
                                  THEN DATE_ADD(f_case.art_start_date, INTERVAL 91 DAY)
                              WHEN ((timestampdiff(month, f_case.art_start_date, END_DATE) > 3)
                                  AND (timestampdiff(week, END_DATE, DATE_ADD(f_case.lmp_date, INTERVAL 280 DAY)) <
                                       34 and
                                       f_case.lmp_date is not null)
                                  AND vlperfdate.viral_load_ref_date IS not NULL
                                  AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51)
                                  AND (Abs(timestampdiff(month, vlperfdate.viral_load_ref_date, END_DATE)) >= 3)
                                  AND (f_case.pregnancy_status = 'Yes')
                                  )
                                  THEN END_DATE
                              WHEN ((TIMESTAMPDIFF(month, f_case.art_start_date, END_DATE) > 3)
                                  AND (f_case.lmp_date is not null)
                                  AND (TIMESTAMPDIFF(WEEK, END_DATE, DATE_ADD(f_case.lmp_date, interval 280 DAY)) < 34)
                                  AND vlperfdate.viral_load_ref_date IS NULL
                                  AND (f_case.pregnancy_status = 'Yes')
                                  )
                                  THEN END_DATE
                              WHEN (
                                  (f_case.pregnancy_status = 'Yes')
                                      AND (TIMESTAMPDIFF(MONTH, f_case.art_start_date, END_DATE) > 3)
                                      AND f_case.lmp_date is null
                                      AND
                                  (TIMESTAMPDIFF(WEEK, END_DATE, DATE_ADD(f_case.follow_up_date, INTERVAL 280 day)) <
                                   34)
                                      AND vlperfdate.viral_load_ref_date IS not NULL
                                      AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51)
                                      AND (timestampdiff(MONTH, vlperfdate.viral_load_ref_date, END_DATE) >= 3)
                                  )
                                  THEN END_DATE
                              WHEN (
                                  f_case.lmp_date is not null
                                      AND
                                  (TIMESTAMPDIFF(WEEK, END_DATE, DATE_ADD(f_case.lmp_date, INTERVAL 280 DAY)) < 34)
                                      AND vlperfdate.viral_load_ref_date IS not NULL
                                      AND (f_case.pregnancy_status = 'Yes')
                                  )
                                  THEN DATE_ADD(f_case.lmp_date, INTERVAL 239 DAY)
                              WHEN (
                                  (f_case.pregnancy_status = 'Yes')
                                      AND f_case.lmp_date is not null
                                      AND (DATE_ADD(f_case.lmp_date, interval 280 DAY) <= END_DATE)
                                      AND (vlperfdate.viral_load_ref_date IS NULL or
                                           vlperfdate.viral_load_ref_date < DATE_ADD(f_case.lmp_date, INTERVAL 280 DAY))
                                  )
                                  THEN DATE_ADD(f_case.lmp_date, INTERVAL 371 DAY)
                              WHEN (
                                  (f_case.pregnancy_status = 'Yes')
                                      AND (f_case.lmp_date is null)
                                      AND (vlperfdate.viral_load_ref_date IS NULL or
                                           vlperfdate.viral_load_ref_date < f_case.follow_up_date)
                                  )
                                  THEN DATE_ADD(f_case.follow_up_date, INTERVAL 371 DAY)
                              WHEN (
                                  (f_case.pregnancy_status = 'Yes')
                                      AND (f_case.lmp_date is not null)
                                      AND vlperfdate.viral_load_ref_date IS not NULL
                                      AND (vlperfdate.viral_load_ref_date > (f_case.lmp_date + 280))
                                  )
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 181 DAY)
                              WHEN (
                                  (f_case.breastfeeding_status = 'Yes')
                                      AND vlperfdate.viral_load_ref_date IS not NULL
                                      AND (vlperfdate.viral_load_ref_date < f_case.follow_up_date and
                                           TIMESTAMPDIFF(DAY, vlperfdate.viral_load_ref_date,
                                                         END_DATE) > 180)
                                  )
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 181 DAY)
                              WHEN (
                                  vlperfdate.viral_load_ref_date IS not NULL
                                      AND
                                  (vlperfdate.viral_load_count IS not NULL and vlperfdate.viral_load_count <= 1000 and
                                   vlperfdate.viral_load_count >= 51)
                                      AND (sub_switch_date.FollowupDate IS NULL or (
                                      sub_switch_date.FollowupDate is not null and
                                      sub_switch_date.FollowupDate < vlperfdate.viral_load_ref_date))
                                  )
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 91 DAY)
                              WHEN (
                                  vlperfdate.viral_load_ref_date IS not NULL
                                      AND
                                  (vlperfdate.viral_load_count IS not NULL and vlperfdate.viral_load_count > 1000)
                                      AND (sub_switch_date.FollowupDate IS NULL or (
                                      sub_switch_date.FollowupDate is not null and
                                      sub_switch_date.FollowupDate < vlperfdate.viral_load_ref_date))
                                  )
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 91 DAY)

                              WHEN
                                  (
                                      vlperfdate.viral_load_ref_date IS not NULL
                                          AND sub_switch_date.FollowupDate IS not NULL
                                          AND (vlperfdate.viral_load_ref_date <= sub_switch_date.FollowupDate))
                                  THEN DATE_ADD(sub_switch_date.FollowupDate, INTERVAL 181 DAY)
                              When (
                                  (f_case.breastfeeding_status = 'Yes')
                                      AND vlperfdate.viral_load_ref_date IS not NULL
                                      AND (timestampdiff(day, vlperfdate.viral_load_ref_date, END_DATE) >= 90)
                                  )
                                  THEN END_DATE
                              When (
                                  (f_case.pregnancy_status = 'Yes')
                                      AND vlperfdate.viral_load_ref_date IS not NULL
                                      AND (timestampdiff(DAY, vlperfdate.viral_load_ref_date, END_DATE) >= 90)
                                  )
                                  THEN END_DATE
                              ELSE END_DATE End AS eligiblityDate


                   FROM FollowUp AS f_case
                            INNER JOIN tmp_2
                                       ON f_case.encounter_id = tmp_2.encounter_id
                            LEFT JOIN vl_perf_date_3 as vlperfdate
                                      ON vlperfdate.client_id = f_case.client_id
                            Left join vl_sent_date as vlsentdate
                                      ON vlsentdate.client_id = f_case.client_id
                            Left join switch_sub_date as sub_switch_date
                                      ON sub_switch_date.client_id = f_case.client_id
                            Left join all_art_not_started on f_case.client_id = all_art_not_started.client_id),


         vl_eligibility as (select tmp_3.client_id,
                                   Case
                                       When tmp_3.art_start_date is not null
                                           and tmp_3.follow_up_status not in ('Alive', 'Restart medication')
                                           THEN 'Viral Load Not Applicable 2'
                                       When tmp_3.art_start_date is not null
                                           and tmp_3.follow_up_status in ('Alive', 'Restart medication') and
                                            tmp_3.eligiblityDate > END_DATE
                                           and timestampdiff(month, tmp_3.art_start_date, END_DATE) < 12 and
                                            tmp_3.viral_load_perform_date is null
                                           THEN 'Viral Load Not Applicable 1'
                                       When tmp_3.eligiblityDate <= END_DATE and tmp_3.art_start_date is not null
                                           and tmp_3.follow_up_status = 'Restart medication'
                                           THEN 'Eligible for Viral Load 3'
                                       When tmp_3.eligiblityDate <= END_DATE and tmp_3.art_start_date is not null
                                           and tmp_3.follow_up_status in ('Alive', 'Restart medication')
                                           THEN 'Eligible for Viral Load 1'
                                       When tmp_3.eligiblityDate <= END_DATE
                                           and tmp_3.follow_up_status in ('Alive', 'Restart medication')
                                           and tmp_3.viral_load_perform_date is null
                                           and timestampdiff(month, tmp_3.art_start_date, END_DATE) >= 12
                                           THEN 'Eligible for Viral Load 2'
                                       When tmp_3.eligiblityDate > END_DATE
                                           THEN 'Viral Load Done'
                                       When tmp_3.art_start_date is null and tmp_3.follow_up_status is null
                                           THEN 'Not Started ART'
                                       ELSE 'unk'
                                       END as vl_status
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
                                follow_up_date                                                                             AS dsd_fdate,
                                assessment_status,
                                ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
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
         cervical_1 as (select tmp_address.client_id, 'Blue' as Cervical_status
                        from tmp_address
                        where tmp_address.sex = 'M'
                           or tmp_address.age <= 14
                           or tmp_address.age > 49),

         cervical_2 as (select tmp_3.client_id, 'Blue' as Cervical_status
                        from tmp_3
                        where tmp_3.client_id not in (select cervical_1.client_id from cervical_1)
                          and (tmp_3.follow_up_status = 'Transferred out' or tmp_3.follow_up_status = 'Dead')),
         cervical_3 as (select all_art_not_started_status.client_id, 'Black' as Cervical_status
                        from all_art_not_started_status
                        where all_art_not_started_status.client_id not in (select cervical_1.client_id from cervical_1)
                          and
                            all_art_not_started_status.client_id not in (select cervical_2.client_id from cervical_2)),
         cervical_4 as (select FollowUp.client_id
                             , 'Red' as Cervical_status
                        from FollowUp
                        where (councelling_given is not null or
                               cervical_cancer_screening_status is not null) -- OR councelling/screeening
                          and FollowUp.BIOPSY_RESULT in ('Invasive cervical cancer',
                                                         'Carcinoma in situ',
                                                         'Other'
                            )
                          and FollowUp.follow_up_date <= END_DATE
                          and FollowUp.client_id not in (select cervical_1.client_id from cervical_1)
                          and FollowUp.client_id not in (select cervical_2.client_id
                                                         from cervical_2)
                          and FollowUp.client_id not in (select cervical_3.client_id
                                                         from cervical_3)),
         tmp_cervical_5 as (SELECT encounter_id,
                                   client_id                                                                                  as ca_id,
                                   follow_up_date                                                                             AS ca_fdate,
                                   ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                            FROM FollowUp
                            where (councelling_given is not null or cervical_cancer_screening_status is not null)
                              and follow_up_date <= END_DATE
                              and cervical_cancer_screening_status = 'Cervical cancer screening performed'),
         tmp_2_cervical_5 as (select * from tmp_cervical_5 where row_num = 1),
         cervical_5 as (select FollowUp.client_id
                             , 'Green' as Cervical_status
                        from FollowUp
                                 inner join tmp_2_cervical_5 on FollowUp.encounter_id = tmp_2_cervical_5.encounter_id
                        where FollowUp.ccs_next_date
                            > END_DATE
                          and FollowUp.client_id not in (select cervical_1.client_id from cervical_1)
                          and FollowUp.client_id not in (select cervical_2.client_id
                                                         from cervical_2)
                          and FollowUp.client_id not in (select cervical_3.client_id
                                                         from cervical_3)
                          and FollowUp.client_id not in (select cervical_4.client_id
                                                         from cervical_4)),
         cervical_55 as (select FollowUp.client_id
                              , 'Yellow' as Cervical_status
                              , CCS_Next_Date
                         from FollowUp
                                  inner join tmp_2_cervical_5 on FollowUp.encounter_id = tmp_2_cervical_5.encounter_id
                         where FollowUp.client_id not in (select cervical_1.client_id from cervical_1)
                           and FollowUp.client_id not in (select cervical_2.client_id
                                                          from cervical_2)
                           and FollowUp.client_id not in (select cervical_3.client_id
                                                          from cervical_3)
                           and FollowUp.client_id not in (select cervical_4.client_id
                                                          from cervical_4)
                           and FollowUp.client_id not in (select cervical_5.client_id
                                                          from cervical_5)),
         cervical_66 as (select client_id
                              , 'Yellow' as Cervical_status
                         from FollowUp
                         where (councelling_given is not null or cervical_cancer_screening_status is not null)
                           and client_id
                             not in (select client_id from cervical_55)
                           and FollowUp.client_id not in (select cervical_1.client_id
                                                          from cervical_1)
                           and FollowUp.client_id not in (select cervical_2.client_id
                                                          from cervical_2)
                           and FollowUp.client_id not in (select cervical_3.client_id
                                                          from cervical_3)
                           and FollowUp.client_id not in (select cervical_4.client_id
                                                          from cervical_4)
                           and FollowUp.client_id not in (select cervical_5.client_id
                                                          from cervical_5)),

         cervical_6 as (select client_id, Cervical_status
                        from cervical_55
                        where CCS_Next_Date < END_DATE
                        UNION
                        select client_id, Cervical_status
                        from cervical_66),
         cervical_7 as (select all_art_follow_ups.client_id, 'Yellow' as Cervical_status
                        from all_art_follow_ups
                        where all_art_follow_ups.client_id not in
                              (select client_id
                               from FollowUp
                               where (councelling_given is not null or cervical_cancer_screening_status is not null))
                          and all_art_follow_ups.client_id not in (select cervical_1.client_id from cervical_1)
                          and all_art_follow_ups.client_id not in (select cervical_2.client_id from cervical_2)
                          and all_art_follow_ups.client_id not in (select cervical_3.client_id from cervical_3)
                          and all_art_follow_ups.client_id not in (select cervical_4.client_id from cervical_4)
                          and all_art_follow_ups.client_id not in (select cervical_5.client_id from cervical_5)
                          and all_art_follow_ups.client_id not in (select cervical_6.client_id from cervical_6)),
         cervical as (select client_id, Cervical_status
                      from cervical_1
                      union
                      select client_id
                           , Cervical_status
                      from cervical_2
                      union
                      select client_id
                           , Cervical_status
                      from cervical_3
                      union
                      select client_id
                           , Cervical_status
                      from cervical_4
                      union
                      select client_id
                           , Cervical_status
                      from cervical_5
                      union
                      select client_id
                           , Cervical_status
                      from cervical_6
                      union
                      select client_id
                           , Cervical_status
                      from cervical_7),

         asm as (select client_id
                      , assessment_status
                      , dsd_category
                 from dsd_cat
                 union
                 select client_id
                      , ''
                      , 'ART not started'
                 from all_art_not_started_status)

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
               when vl_eligibility.vl_status is not null then vl_eligibility.vl_status
               Else 'undetermined_VL' end                                    as vl_status
            ,
           case
               when asm.assessment_status is not null then asm.assessment_status
               Else '' end                                                   as asm_status
            ,
           case
               when asm.dsd_category is not null then asm.dsd_category
               Else 'Not enrolled to any DSD' end                            as dsd_category
            ,

           CASE tmp_3.follow_up_status
               WHEN 'Alive' THEN 'Alive on ART'
               WHEN 'Restart medication' THEN 'Restart'
               WHEN 'Transferred out' THEN 'TO'
               WHEN 'Stop all' THEN 'Stop'
               WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
               WHEN 'Ran away' THEN 'Drop'
               END as follow_up_status,

           tmp_3.follow_up_date                                              as `Follow Up Date`,
           tmp_3.follow_up_date                                              as `Follow Up Date EC.`,
           tmp_3.art_start_date                                              as `Art Start Date`,
           tmp_3.art_start_date                                              as `Art Start Date EC.`,
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

    where (
        (START_NV_DATE IS NULL OR END_NV_DATE IS NULL)
            OR (tmp_3.next_visit_date BETWEEN START_NV_DATE AND END_NV_DATE)
        )
      AND (
        (TYPE_OF_CLIENT = 'curr'
            AND tmp_3.follow_up_status IN ('Alive', 'Restart medication')
            AND tmp_3.art_dose_end_date >= END_DATE
            )
            OR TYPE_OF_CLIENT = 'all'
        )
      AND (
        PATIENT_GUID IS NULL
            OR tmp_address.patient_uuid = PATIENT_GUID
        )
    ORDER BY tmp_address.PatientName;

END //
DELIMITER ;