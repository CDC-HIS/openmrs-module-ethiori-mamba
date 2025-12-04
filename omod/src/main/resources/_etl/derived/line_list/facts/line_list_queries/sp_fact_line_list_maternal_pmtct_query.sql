DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_maternal_pmtct_query;

CREATE PROCEDURE sp_fact_line_list_maternal_pmtct_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH Enrollment_tmp as (select client_id,
                                   antenatal_care_provider,
                                   ld_client,
                                   post_natal_care,
                                   art_clinic,
                                   location_of_birth,
                                   date_of_enrollment_or_booking,
                                   ROW_NUMBER() over (PARTITION BY client_id ORDER BY date_of_enrollment_or_booking DESC, encounter_id DESC) as row_num
                            from mamba_flat_encounter_pmtct_enrollment
                            where date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         Enrollment as (select * from Enrollment_tmp where row_num = 1),
         tmp_discharge as (select discharge.*,
                                  ROW_NUMBER() over (PARTITION BY discharge.client_id ORDER BY discharge_date, encounter_id ) as row_num
                           from mamba_flat_encounter_pmtct_discharge discharge
                                    join Enrollment on discharge.client_id = Enrollment.client_id
                           WHERE discharge_date > Enrollment.date_of_enrollment_or_booking),
         discharge as (select * from tmp_discharge where row_num = 1),
         FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id                 AS PatientId,
                             follow_up_status,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date       AS art_start_date,
                             assessment_date,
                             treatment_end_date,
                             antiretroviral_art_dispensed_dose_i AS ARTDoseDays,
                             weight_text_                        AS Weight,
                             screening_test_result_tuberculosis  AS TB_SreeningResult,
                             date_of_last_menstrual_period_lmp_     LMP_Date,
                             anitiretroviral_adherence_level     AS AdherenceLevel,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child          breast_feeding_status,
                             pregnancy_status,
                             diagnosis_date                      AS ActiveTBDiagnoseddate,
                             nutritional_status_of_adult,
                             nutritional_supplements_provided,
                             stages_of_disclosure,
                             date_started_on_tuberculosis_prophy,
                             method_of_family_planning,
                             patient_diagnosed_with_active_tuber as ActiveTBDiagnosed,
                             dsd_category,
                             nutritional_screening_result,
                             hiv_viral_load                      AS viral_load_count,
                             cd4_count,
                             date_of_event                       as hiv_confirmed_date,
                             date_started_on_tuberculosis_prophy AS inhprophylaxis_started_date,
                             date_completed_tuberculosis_prophyl AS InhprophylaxisCompletedDate,
                             date_active_tbrx_completed,
                             date_of_reported_hiv_viral_load     as viral_load_sent_date,
                             date_viral_load_results_received    AS viral_load_perform_date,
                             date_referred_to_pmtct,
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
                             COALESCE(repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                                      suspected_antiretroviral_failure
                             )                                   AS targeted_viral_load_test_indication,
                             viral_load_test_status,
                             CASE visitect_cd4_result
                                 WHEN 'VISITECT <=200 copies/ml' THEN 'VISITECT >200 copies/ml'
                                 ELSE visitect_cd4_result END    AS visitect_cd4_result,
                             visitect_cd4_test_date
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
         tmp_latest_follow_up AS (SELECT FollowUp.*,
                                         ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC, FollowUp.encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE FollowUp.follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
                                    AND follow_up_date <=
                                        COALESCE((select discharge_date from discharge), REPORT_END_DATE, CURDATE())),
         latest_follow_up as (select * from tmp_latest_follow_up where row_num = 1),
         vl_sent_date_tmp AS (SELECT FollowUp.encounter_id,
                                     FollowUp.PatientId,
                                     FollowUp.viral_load_perform_date,
                                     FollowUp.viral_load_test_status,
                                     cd4_count,
                                     follow_up_date,
                                     ARTDoseDays,
                                     follow_up_status,
                                     AdherenceLevel,
                                     viral_load_count,
                                     regimen,
                                     FollowUp.viral_load_sent_date,
                                     routine_viral_load_test_indication,
                                     targeted_viral_load_test_indication,
                                     ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY viral_load_sent_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND art_start_date IS NOT NULL
                                AND viral_load_sent_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         vl_sent as (select * from vl_sent_date_tmp where row_num = 1),
         vl_performed_date_tmp AS (SELECT FollowUp.encounter_id,
                                          FollowUp.PatientId,
                                          FollowUp.viral_load_perform_date,
                                          FollowUp.viral_load_test_status,
                                          follow_up_date,
                                          viral_load_sent_date,
                                          follow_up_status,
                                          ARTDoseDays,
                                          AdherenceLevel,
                                          regimen,
                                          viral_load_count,
                                          cd4_count,
                                          routine_viral_load_test_indication,
                                          targeted_viral_load_test_indication,
                                          ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
                                   FROM FollowUp
                                   WHERE follow_up_status IS NOT NULL
                                     AND art_start_date IS NOT NULL
                                     AND viral_load_perform_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         vl_performed_date as (select * from vl_performed_date_tmp where row_num = 1)

    select client.patient_name                                 as                    `Patient Name`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) as                    `Age`,
           art_start_date                                      as                    `ART Start Date`,
           date_of_enrollment_or_booking                       as                    `PMTCT Booking Date`,
           COALESCE(art_clinic, antenatal_care_provider, ld_client, post_natal_care) `Status at Enrollment`,
           date_referred_to_pmtct                              as                    `Date Referred to PMTCT`,
           pregnancy_status                                    as                    `Pregnant?`,
           breast_feeding_status                               as                    `Breastfeeding?`,
           discharge_date                                      as                    `Date of Discharge`,
           reason_for_discharge_from_pmtct                     as                    `Reason for Discharge`,
           discharge.follow_up_status                          as                    `Maternal PMTCT Final Outcome`,
#            `Date of Final Outcome`,
           latest_follow_up.follow_up_date                                           `Latest Follow-up Date`,
           latest_follow_up.follow_up_status                                         `Latest Follow-up Status`,
           latest_follow_up.regimen                                                  `Regimen	Dose`,
           latest_follow_up.AdherenceLevel                                           Adherence,
           nutritional_status_of_adult                                               `Nutritional Status`,
           vl_sent.cd4_count                                                         `CD4 count`,
           vl_sent.viral_load_sent_date                        as                    `Latest VL Sent Date`,
           coalesce(vl_sent.routine_viral_load_test_indication,
                    vl_sent.targeted_viral_load_test_indication)                     `Latest VL Test Indication`,
           vl_performed_date.viral_load_perform_date                                 `Latest VL Received Date`,
           vl_performed_date.viral_load_test_status                                  `Latest VL Status`,
           next_visit_date                                                           `Next Visit Date`
    from Enrollment
             join latest_follow_up on Enrollment.client_id = latest_follow_up.PatientId
             left join discharge on discharge.client_id = Enrollment.client_id
             left join vl_sent on vl_sent.PatientId = Enrollment.client_id
             left join vl_performed_date on vl_performed_date.PatientId = Enrollment.client_id
             join mamba_dim_client client on Enrollment.client_id = client.client_id;
END //

DELIMITER ;