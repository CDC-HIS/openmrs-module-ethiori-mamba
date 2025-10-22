DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_vl_sent_query;

CREATE PROCEDURE sp_fact_line_list_vl_sent_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH FollowUp AS (SELECT follow_up.client_id,
                             follow_up.encounter_id,
                             date_viral_load_results_received AS viral_load_perform_date,
                             date_of_reported_hiv_viral_load  as viral_load_sent_date,
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
                             adherence,
                             cd4_
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
                                         ON follow_up.encounter_id = follow_up_10.encounter_id
                      ),


         vl_sent_date_tmp AS (SELECT FollowUp.encounter_id,
                                     FollowUp.client_id,
                                     FollowUp.viral_load_perform_date,
                                     FollowUp.viral_load_test_status,
                                     'vl_sent'                                                                                        as vl_type,
                                     cd4_,
                                     follow_up_date,
                                     arv_dispensed_dose,
                                     follow_up_status,
                                     Adherence,
                                     viral_load_count,
                                     regimen,
                                     FollowUp.viral_load_sent_date,
                                     routine_viral_load_test_indication,
                                     targeted_viral_load_test_indication,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_sent_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND art_start_date IS NOT NULL
                                AND viral_load_sent_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                              GROUP BY client_id, encounter_id),
         latest_follow_up_tmp AS (SELECT client_id,
                                         follow_up_date                                                                             AS FollowupDate,
                                         encounter_id,
                                         follow_up_status,
                                         arv_dispensed_dose,
                                         adherence,
                                         next_visit_date,
                                         treatment_end_date,
                                         regimen,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
                                    AND follow_up_date <= REPORT_END_DATE),
         latest_follow_up AS (select * from latest_follow_up_tmp where row_num = 1),
         vl_sent_date as (select * from vl_sent_date_tmp where row_num = 1),


         vl_test_sent AS (SELECT patient_name                                        as `Patient Name`,
                                 patient_uuid as `UUID`,
                                 MRN,
                                 UAN,
                                 TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) as Age,
                                 Sex,
                                 Weight,
                                 vlsent.cd4_                                         as CD4,
                                 art_start_date                                      as `ART Start Date`,
                                 art_start_date                                      as `ART Start Date EC.`,
                                 vlsent.follow_up_date                               as `Follow-up Date`,
                                 vlsent.follow_up_date                               as `Follow-up Date EC.`,
                                 vlsent.follow_up_status                             as `Follow-up Status`,
                                 vlsent.regimen                                      as Regimen,
                                 vlsent.arv_dispensed_dose                              `ARV Dose Days`,
                                 vlsent.Adherence,
                                 pregnancy_status                                    as PregnancyStatus,
                                 breastfeeding_status                                as BreastfeedingStatus,
                                 CASE
                                     WHEN pregnancy_status = 'Yes' THEN 'Yes'
                                     WHEN breastfeeding_status = 'Yes' THEN 'Yes'
                                     ELSE 'No' END                                   AS `On PMTCT?`,
                                 vlsent.viral_load_sent_date                         as `Viral Load Sent Date`,
                                 vlsent.viral_load_sent_date                         as `Viral Load Sent Date EC.`,
                                 vlsent.viral_load_perform_date                      as `VL Received Date`,
                                 vlsent.viral_load_perform_date                      as `VL Received Date EC.`,
                                 TIMESTAMPDIFF(DAY, vlsent.viral_load_sent_date,
                                               vlsent.viral_load_perform_date)                         `TAT (in days)`,
                                 vlsent.routine_viral_load_test_indication           as `Routine Test type`,
                                 vlsent.targeted_viral_load_test_indication          as `Targeted Test Type`,
                                 vlsent.viral_load_test_status                       as viral_load_status,
                                 vlsent.viral_load_count,
                                 latest_follow_up.FollowupDate                       as `Latest Follow-up Date`,
                                 latest_follow_up.FollowupDate                       as `Latest Follow-up Date EC.`,
                                 latest_follow_up.follow_up_status                   as `Latest Follow-up Status`,
                                 latest_follow_up.regimen                            as `Latest Regimen`,
                                 latest_follow_up.arv_dispensed_dose                 as `Latest ARV Dose Days`,
                                 latest_follow_up.adherence                          as `Latest Adherence`,
                                 latest_follow_up.next_visit_date                    as `Next Visit Date`,
                                 latest_follow_up.next_visit_date                    as `Next Visit Date EC.`,
                                 latest_follow_up.treatment_end_date                 as `Treatment End Date`,
                                 latest_follow_up.treatment_end_date                 as `Treatment End Date EC.`,
                                 mobile_no                                           as `Mobile No.`

                          FROM FollowUp
                                   INNER JOIN vl_sent_date as vlsent
                                              ON vlsent.encounter_id = FollowUp.encounter_id
                                   LEFT JOIN mamba_dim_client client ON vlsent.client_id = client.client_id
                                   LEFT JOIN latest_follow_up
                                             ON latest_follow_up.encounter_id = FollowUp.encounter_id)
    select *
    from vl_test_sent;

END //

DELIMITER ;