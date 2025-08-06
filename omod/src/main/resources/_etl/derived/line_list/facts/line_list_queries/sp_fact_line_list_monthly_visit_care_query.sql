DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_monthly_visit_care_query;

CREATE PROCEDURE sp_fact_line_list_monthly_visit_care_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE,
                                                            IN STATUS TEXT)
BEGIN
    WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_2.weight_text_                 as weight,
                             follow_up_1.bmi_text_                    as bmi,
                             follow_up_7.nutritional_screening_result as nutritional_screening_result,
                             follow_up_4.pregnancy_status             as pregnancy,
                             CASE follow_up_status
                                 WHEN 'Alive' THEN 'Alive on ART'
                                 WHEN 'Restart medication' THEN 'Restart'
                                 WHEN 'Transferred out' THEN 'TO'
                                 WHEN 'Stop all' THEN 'Stop'
                                 WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
                                 WHEN 'Ran away' THEN 'Drop'
                                 END                                  as follow_up_status,
                             follow_up_date_followup_                 as follow_up_date,
                             art_antiretroviral_start_date            as art_start_date,
                             height,
                             tb_prophylaxis_type,
                             current_who_hiv_stage,
                             antiretroviral_art_dispensed_dose_i      as art_dose_days,
                             regimen,
                             date_of_reported_hiv_viral_load          as date_vl_requested,
                             date_viral_load_results_received         as date_vl_received,
                             anitiretroviral_adherence_level          as adherence,
                             next_visit_date                          as next_visit_date,
                             treatment_end_date                       as tx_curr_date,
                             viral_load_test_status                   as vl_test_status,
                             cd4_count ,
                             nutritional_status_of_adult              as nutritional_status,
                             tpt_followup_6h_                         as tpt_status,
                             screening_test_result_tuberculosis       as tb_screening_result,
                             dsd_category ,
                             assessment_date                          as dsd_assessment_date,
                             transferred_in_check_this_for_all_t      as transfer_in,
                             reg.registration_date                    as enrollement_date

                      FROM mamba_flat_encounter_intake_a mfeia
                               LEFT JOIN mamba_flat_encounter_follow_up follow_up
                                         on follow_up.client_id = mfeia.client_id
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
                               LEFT JOIN mamba_flat_encounter_registration reg
                                         ON reg.client_id = follow_up.client_id
                      WHERE follow_up_date_followup_ >= COALESCE(REPORT_START_DATE, CURDATE())
                        AND follow_up_date_followup_ <= COALESCE(REPORT_END_DATE, CURDATE())
                        and FIND_IN_SET(follow_up_status, STATUS)),

         tmp_latest_follow_up AS (SELECT FollowUp.*,
                                         ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY follow_up_date DESC,
                                             encounter_id DESC) AS row_num
                                  FROM FollowUp
                                           join mamba_dim_client client on FollowUp.client_id = client.client_id),

         latest_follow_up AS (select *
                              from tmp_latest_follow_up
                              where row_num = 1)

    SELECT ROW_NUMBER() OVER (ORDER BY patient_name)           as `#`,
           client.patient_uuid                                 as `Patient GUID`,
           client.patient_name                                 as `Patient Name`,
           CAST(client.mrn AS CHAR(20))                        as MRN,
           client.uan                                          as UAN,
           TIMESTAMPDIFF(YEAR, client.date_of_birth,
                         COALESCE(REPORT_END_DATE, CURDATE())) as `Age`,
           client.sex                                          as Sex,
           f_case.art_start_date                               as `ART Start Date in EC.`,
           f_case.art_start_date                               as `ART Start Date in G.C.`,
           f_case.follow_up_date                               as `Follow-up Date in EC.`,
           f_case.follow_up_date                               as `Follow-up Date in G.C.`,
           f_case.follow_up_status                             as `Follow-up Status`,
           f_case.regimen                                      as `Regimen`,
           f_case.art_dose_days                                as `ARV Dose`,
           f_case.adherence                                    as `Adherence`,
           f_case.weight                                       as `Weight (kg)`,
           f_case.bmi                                          as `BMI`,
           nutritional_status                                  as `Nutritional Status`,
           pregnancy                                           as `Pregnancy Status`,
           f_case.cd4_count                                           as `CD4`,
           f_case.date_vl_requested                            as `VL Requested Date in EC.`,
           f_case.date_vl_requested                            as `VL Requested Date in G.C.`,
           f_case.date_vl_received                             as `VL Received Date in EC.`,
           f_case.date_vl_received                             as `VL Received Date in G.C.`,
           f_case.vl_test_status                               as `VL Status`,
           f_case.tpt_status                                   as `TPT Status`,
           f_case.tb_screening_result                          as `TB Screening Result`,
           f_case.dsd_category                                        as `DSD Category`,
           f_case.dsd_assessment_date                          as `Current DSD Assessment Date in EC.`,
           f_case.dsd_assessment_date                          as `Current DSD Assessment Date in G.C.`,
           f_case.next_visit_date                              as `Next Visit Date in EC.`,
           f_case.next_visit_date                              as `Next Visit Date in G.C.`,
           f_case.tx_curr_date                                 as `Last TX_CURR Date in EC.`,
           f_case.tx_curr_date                                 as `Last TX_CURR Date in GC.`,
           client.mobile_no                                    as `Mobile NO.`


    FROM latest_follow_up AS f_case
             INNER JOIN mamba_dim_client client on f_case.client_id = client.client_id
    ORDER BY client.patient_name;


END //

DELIMITER ;
