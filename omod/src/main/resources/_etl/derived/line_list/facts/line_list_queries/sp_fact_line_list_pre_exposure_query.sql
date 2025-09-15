DELIMITER //
DROP PROCEDURE IF EXISTS sp_fact_line_list_pre_exposure_query;
CREATE PROCEDURE sp_fact_line_list_pre_exposure_query(IN REPORT_START_DATE DATE,
                                                       IN REPORT_END_DATE DATE)
BEGIN
    WITH FollowUp AS (select pf.encounter_id,
                             pf.client_id,
                             pf_1.followup_date_followup_1             as follow_up_date,
                             pf.pregnancy_status,
                             final_hiv_test_result,
                             method_of_family_planning,
                             screened_for_tuberculosis,
                             chronic_kidney_disease_stage              AS EGFR,
                             preexposure_prophylaxis_prep_regimen      as regimen,
                             antiretroviral_art_dispensed_dose_in_days as dose_day,
                             reason_for_stopping_adherence             as reason_for_stopping,
                             hiv_side_effects                          as side_effects,
                             currently_breastfeeding_child,
                             prep_dose_end_date,
                             next_visit_date,
                             number_of_missed_tablets_in_the_past_30_days,
                             CASE follow_up_status
                                 WHEN 'Alive' THEN 'Alive on ART'
                                 WHEN 'Restart medication' THEN 'Restart'
                                 WHEN 'Transferred out' THEN 'TO'
                                 WHEN 'Stop all' THEN 'Stop'
                                 WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
                                 WHEN 'Ran away' THEN 'Drop'
                                 END                                   as follow_up_status

                      FROM mamba_flat_encounter_pre_exposure_follo as pf
                               INNER JOIN mamba_flat_encounter_pre_exposure_follo_1 as pf_1
                                          ON pf.encounter_id = pf_1.encounter_id

                      WHERE pf_1.prep_dose_end_date >= COALESCE(REPORT_END_DATE, CURDATE())
                        AND pf_1.followup_date_followup_1 <= COALESCE(REPORT_END_DATE, CURDATE()))
            ,


         tmp_latest_prep_follow_up AS (SELECT f.*,
                                              ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY follow_up_date DESC,
                                                  encounter_id DESC) AS row_num
                                       FROM FollowUp as f
                                                join mamba_dim_client client on f.client_id = client.client_id),


         latest_prep_followup AS (select *
                                  from tmp_latest_prep_follow_up
                                  where row_num = 1),

         temp_screening_prep AS (select pf.*,
                                        sc.prep_started,
                                        sc.screening_date,
                                        sc.type_of_client,
                                        sc_1.antiretroviral_art_dispensed_dose_in_days as screened_dose,
                                        sc_1.prep_antiretrovirals_prescribed           as regimen_screened,
                                        sc.prep_dose_end_date                          as screened_dose_end_date,
                                        sc_1.screen_result_for_sti,
                                        sc.sex_worker,
                                        sc_1.preexposure_prophylaxis_provision_site,
                                        sc.referred_from,
                                        treatment_start_date,
                                        service_delivery_point_number,
                                        sc_1.contraindications_to_prep_medicines,
                                        sc_1.do_you_have_an_hiv_positive_partner,
                                        ROW_NUMBER() OVER (PARTITION BY sc.client_id ORDER BY prep_dose_end_date DESC,
                                            sc.encounter_id DESC)                      AS r_n
                                 from mamba_flat_encounter_pre_exposure_scree as sc
                                          INNER JOIN mamba_flat_encounter_pre_exposure_scree_1 as sc_1
                                                     ON sc.encounter_id = sc_1.encounter_id
                                          LEFT JOIN latest_prep_followup AS pf on pf.client_id = sc.client_id
                                 WHERE sc.prep_dose_end_date >=
                                       COALESCE(REPORT_END_DATE, CURDATE())),

         all_in_one_prep as (select * from temp_screening_prep where r_n = 1)

    SELECT ROW_NUMBER() OVER (ORDER BY patient_name)           as `#`,
           client.patient_uuid                                 as `Patient GUID`,
           client.patient_name                                 as `Patient Name`,
           CAST(client.mrn AS CHAR(20))                        as MRN,
           client.uan                                          AS UAN,

           TIMESTAMPDIFF(YEAR, client.date_of_birth,
                         COALESCE(REPORT_END_DATE, CURDATE())) as `Age`,
           client.sex                                          as Sex,
           f_case.service_delivery_point_number                as `UIC`,
           f_case.screening_date                               as `PrEP Screening Date in EC.`,
           f_case.screening_date                               as `PrEP Screening Date in GC.`,
           f_case.prep_started                                 as `PrEP Started?`,
           f_case.treatment_start_date                         as `PrEP Start Date in EC`,
           f_case.treatment_start_date                         as `PrEP Start Date in G.C`,
           f_case.type_of_client                               as `Type of Client`,
           f_case.follow_up_date                               as `PrEP Follow Up Date in EC.`,
           f_case.follow_up_date                               as `PrEP Follow Up Date in GC.`,
           f_case.follow_up_status                             as `PrEP Follow Up Status`,
           case
               when f_case.regimen is not null then f_case.regimen
               else f_case.regimen_screened end                as `PrEP Regimen`,
           f_case.number_of_missed_tablets_in_the_past_30_days as `Missed Tablets`,
           f_case.next_visit_date                              as `Next Visit Date in EC.`,
           f_case.next_visit_date                              as `Next Visit Date in GC.`,
           case
               when f_case.prep_dose_end_date is not null then f_case.screened_dose_end_date
               else f_case.screened_dose end                   as `Dose End Date in EC.`,
           case
               when f_case.prep_dose_end_date is not null then f_case.screened_dose_end_date
               else f_case.screened_dose end                   as `Dose End Date in GC.`,
           f_case.final_hiv_test_result                        as `HIV Test Final Result`,
           f_case.pregnancy_status                             as `Pregnant?`,
           f_case.currently_breastfeeding_child                as `Breastfeeding?`,
           f_case.method_of_family_planning                    as `Family Planning Method`,
           f_case.screened_for_tuberculosis                    as `TB Screening Result`,
           f_case.screen_result_for_sti                        as `STI Screening Result`,
           f_case.EGFR                                         as `eGFR Estimate`,
           f_case.side_effects                                 as `Side Effects`,
           f_case.reason_for_stopping                          as `Reason to stop PrEP`,
           f_case.contraindications_to_prep_medicines          as `Contraindications to PrEP Medicines`,
           f_case.sex_worker                                   as `Self-Identifying FSW`,
           f_case.do_you_have_an_hiv_positive_partner          as `Have HIV+ Partner?`,
           f_case.preexposure_prophylaxis_provision_site       as `PrEP Provision Site`,
           f_case.referred_from                                as `Referred From`

    FROM all_in_one_prep AS f_case
             INNER JOIN mamba_dim_client client on f_case.client_id = client.client_id

    ORDER BY client.patient_name;

END //

DELIMITER ;
