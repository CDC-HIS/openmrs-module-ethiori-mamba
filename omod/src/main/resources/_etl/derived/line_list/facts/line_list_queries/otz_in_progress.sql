WITH FollowUp as (
    select follow_up.client_id,
           follow_up.encounter_id,
           date_viral_load_results_received AS viral_load_perform_date,
           viral_load_received_,
           follow_up_status,
           follow_up_date_followup_         AS follow_up_date,
           art_antiretroviral_start_date       art_start_date,
           viral_load_test_status,
           hiv_viral_load                   AS viral_load_count,
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
           )                                AS routine_viral_load_test_indication,
           COALESCE(repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                    suspected_antiretroviral_failure
           )                                AS targeted_viral_load_test_indication,
           viral_load_test_indication,
           pregnancy_status,
           currently_breastfeeding_child    AS breastfeeding_status,
           antiretroviral_art_dispensed_dose_i arv_dispensed_dose,
           regimen,
           next_visit_date,
           treatment_end_date,
           date_of_event                       date_hiv_confirmed,
           weight_text_                     as weight,
           date_of_reported_hiv_viral_load  as viral_load_sent_date,
           regimen_change,
           operation_triple_zero_enrollment_da as otz_date
    FROM mamba_flat_encounter_follow_up follow_up
             left JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                       ON follow_up.encounter_id = follow_up_1.encounter_id
             left JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                       ON follow_up.encounter_id = follow_up_2.encounter_id
             left JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                       ON follow_up.encounter_id = follow_up_3.encounter_id
             left JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                       ON follow_up.encounter_id = follow_up_4.encounter_id
),
    tmp_latest_follow_up as (
        SELECT encounter_id,
               client_id,
               follow_up_date,
               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
        FROM FollowUp
        WHERE follow_up_status IS NOT NULL
          AND art_start_date IS NOT NULL
          AND follow_up_date <= REPORT_END_DATE
    ),
     latest_follow_up as (
         select * from tmp_latest_follow_up where row_num=1 -- temp3
     ),
     tmp_vl_performed_date as (
         SELECT encounter_id,
                client_id,
                follow_up_date,
                viral_load_perform_date,
                viral_load_sent_date,
                ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
         FROM FollowUp
         WHERE follow_up_status IS NOT NULL
           AND art_start_date IS NOT NULL
           AND follow_up_date <= REPORT_END_DATE
     ),
     vl_performed_date as (
         select * from tmp_vl_performed_date where row_num=1  -- temp6
     ),
     tmp_otz_vl_performed_date as (
         SELECT encounter_id,
                client_id,
                follow_up_date,
                viral_load_perform_date,
                viral_load_sent_date,
                otz_date,
                ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
         FROM FollowUp
         WHERE follow_up_status IS NOT NULL
           AND art_start_date IS NOT NULL
           AND follow_up_date <= REPORT_END_DATE
           AND otz_date BETWEEN DATE_ADD(viral_load_perform_date, INTERVAL -3 MONTH) AND DATE_ADD(viral_load_perform_date, INTERVAL 2 MONTH)
     ),
     otz_vl_performed_date as (
         select * from tmp_otz_vl_performed_date where row_num=1 -- temp9
     ),
     tmp_oldest_follow_up as (
         SELECT encounter_id,
                client_id,
                follow_up_date,
                ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date , encounter_id) AS row_num
         FROM FollowUp
         WHERE follow_up_status IS NOT NULL
           AND art_start_date IS NOT NULL
           AND follow_up_date <= REPORT_END_DATE
     ),
     oldest_follow_up as (
         select * from tmp_oldest_follow_up where row_num=1 -- temp13
     ),


    # temp3, temp6