DELIMITER //
DROP PROCEDURE IF EXISTS sp_fact_line_list_pre_exposure_query;
CREATE PROCEDURE sp_fact_line_list_pre_exposure_query( IN REPORT_END_DATE DATE)
BEGIN
    WITH PreExposure AS (select screening.client_id,
                                screening.encounter_id,
                                screening.encounter_datetime,
                                screening.sex_worker,
                                screening.prep_started,
                                screening.type_of_client,
                                screening_1.treatment_start_date,
                                screening_1.do_you_have_an_hiv_positive_partner,
                                screening_1.antiretroviral_art_dispensed_dose_in_days as screened_dose,
                                follow_up.pregnancy_status                            as f_pregnancy_status,
                                follow_up_1.prep_dose_end_date,
                                screening.screening_date,
                                final_hiv_test_result,
                                prep_antiretrovirals_prescribed           as regimen_screened,
                                screening.prep_dose_end_date                          as screened_dose_end_date,
                                currently_breastfeeding_child,
                                preexposure_prophylaxis_prep_regimen as regimen,
                                referred_from,
                                service_delivery_point_number,
                                followup_date_followup_1             as follow_up_date,
                                screened_for_tuberculosis,
                                screening_1.screen_result_for_sti,
                                follow_up_1.chronic_kidney_disease_stage              AS EGFR,
                                contraindications_to_prep_medicines,
                                preexposure_prophylaxis_provision_site,
                                reason_for_stopping_adherence             as reason_for_stopping,
                                follow_up_1.method_of_family_planning,
                                hiv_side_effects                          as side_effects,
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
                         FROM mamba_flat_encounter_pre_exposure_scree screening
                                  left join mamba_flat_encounter_pre_exposure_scree_1 screening_1
                                            on screening.encounter_id = screening_1.encounter_id
                                  LEFT JOIN mamba_flat_encounter_pre_exposure_follo follow_up
                                            on screening.client_id = follow_up.client_id
                                  LEFT JOIN mamba_flat_encounter_pre_exposure_follo_1 follow_up_1
                                            on follow_up.encounter_id = follow_up_1.encounter_id),
         tmp_latest_follow_up as (select prep_dose_end_date,
                                         prep_started,
                                         date_of_birth,
                                         sex,
                                         patient_uuid,
                                         mrn,
                                         uan,
                                         do_you_have_an_hiv_positive_partner,
                                         sex_worker,
                                         treatment_start_date,
                                         patient_name,
                                         screening_date,
                                         follow_up_status,
                                         final_hiv_test_result,
                                         type_of_client,
                                         currently_breastfeeding_child,
                                         client.client_id,
                                         screen_result_for_sti,
                                         next_visit_date,
                                         contraindications_to_prep_medicines,
                                         referred_from,
                                         side_effects,
                                         screened_dose_end_date,
                                         regimen,
                                         preexposure_prophylaxis_provision_site,
                                         f_pregnancy_status,
                                         method_of_family_planning,
                                         reason_for_stopping,
                                         service_delivery_point_number,
                                         number_of_missed_tablets_in_the_past_30_days,
                                         screened_for_tuberculosis,
                                         EGFR,
                                         regimen_screened,
                                         screened_dose,
                                         follow_up_date,
                                         ROW_NUMBER() OVER (
                                             PARTITION BY PreExposure.client_id
                                             ORDER BY
                                                 CASE
                                                     WHEN PreExposure.follow_up_date IS NULL THEN 1
                                                     ELSE 0
                                                     END,
                                                 PreExposure.follow_up_date DESC,
                                                 PreExposure.encounter_id DESC
                                             ) AS row_num
                                  from PreExposure
                                           join mamba_dim_client client on PreExposure.client_id = client.client_id
                                  where (follow_up_date <= REPORT_END_DATE
                                      or follow_up_date is null)),
         prep as (select * from tmp_latest_follow_up where row_num = 1)


    SELECT ROW_NUMBER() OVER (ORDER BY patient_name)           as `#`,
           patient_uuid                                 as `Patient GUID`,
           patient_name                                 as `Patient Name`,
           CAST(mrn AS CHAR(20))                        as MRN,
           uan                                          AS UAN,

           TIMESTAMPDIFF(YEAR, date_of_birth,
                         COALESCE(REPORT_END_DATE, CURDATE())) as `Age`,
           sex                                          as Sex,
           prep.service_delivery_point_number                as `UIC`,
           prep.screening_date                               as `PrEP Screening Date in EC.`,
           prep.screening_date                               as `PrEP Screening Date in GC.`,
           prep.prep_started                                 as `PrEP Started?`,
           prep.treatment_start_date                         as `PrEP Start Date in EC`,
           prep.treatment_start_date                         as `PrEP Start Date in G.C`,
           prep.type_of_client                               as `Type of Client`,
           prep.follow_up_date                               as `PrEP Follow Up Date in EC.`,
           prep.follow_up_date                               as `PrEP Follow Up Date in GC.`,
           prep.follow_up_status                             as `PrEP Follow Up Status`,
           case
               when prep.regimen is not null then prep.regimen
               else prep.regimen_screened end                as `PrEP Regimen`,
           prep.number_of_missed_tablets_in_the_past_30_days as `Missed Tablets`,
           prep.next_visit_date                              as `Next Visit Date in EC.`,
           prep.next_visit_date                              as `Next Visit Date in GC.`,
           case
               when prep.prep_dose_end_date is not null then prep.screened_dose_end_date
               else prep.screened_dose end                   as `Dose End Date in EC.`,
           case
               when prep.prep_dose_end_date is not null then prep.screened_dose_end_date
               else prep.screened_dose end                   as `Dose End Date in GC.`,
           prep.final_hiv_test_result                        as `HIV Test Final Result`,
           prep.f_pregnancy_status                             as `Pregnant?`,
           prep.currently_breastfeeding_child                as `Breastfeeding?`,
           prep.method_of_family_planning                    as `Family Planning Method`,
           prep.screened_for_tuberculosis                    as `TB Screening Result`,
           prep.screen_result_for_sti                        as `STI Screening Result`,
           prep.EGFR                                         as `eGFR Estimate`,
           prep.side_effects                                 as `Side Effects`,
           prep.reason_for_stopping                          as `Reason to stop PrEP`,
           prep.contraindications_to_prep_medicines          as `Contraindications to PrEP Medicines`,
           prep.sex_worker                                   as `Self-Identifying FSW`,
           prep.do_you_have_an_hiv_positive_partner          as `Have HIV+ Partner?`,
           prep.preexposure_prophylaxis_provision_site       as `PrEP Provision Site`,
           prep.referred_from                                as `Referred From`

    FROM prep;

END //

DELIMITER ;
