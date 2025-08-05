DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_chronic_care_query;

CREATE PROCEDURE sp_fact_line_list_chronic_care_query(IN REPORT_START_DATE DATE,
                                                      IN REPORT_END_DATE DATE,
                                                      IN STATUS TEXT)
BEGIN

    WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id,
                             CASE follow_up_status
                                 WHEN 'Alive' THEN 'Alive on ART'
                                 WHEN 'Restart medication' THEN 'Restart'
                                 WHEN 'Transferred out' THEN 'TO'
                                 WHEN 'Stop all' THEN 'Stop'
                                 WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
                                 WHEN 'Ran away' THEN 'Drop'
                                 END                                         as follow_up_status,
                             follow_up_date_followup_                        as follow_up_date,
                             art_antiretroviral_start_date                   as art_start_date,
                             height,
                             mfeia.date_hiv_confirmed                        as date_hiv_confirmed,
                             current_who_hiv_stage,
                             cd4_count,
                             follow_up_2.weight_text_                        as weight,
                             follow_up_6.method_of_family_planning           as family_planning_method,
                             follow_up_6.current_functional_status           as functional_status,
                             follow_up_4.pregnancy_status                    as pregnancy_status,
                             follow_up_7.currently_breastfeeding_child       as breastfeeding_status,
                             follow_up_9.tb_related_ois_opportunistic_illnes as tb_related_ois,
                             follow_up.cd4_                                  as cd4,
                             follow_up_1.cd4_count                           as cd4_count_1,
                             follow_up_5.stages_of_disclosure                as disclosure_stage,
                             follow_up_8.date_viral_load_results_received    as vl_received_date,
                             follow_up_5.viral_load_test_status              as vl_test_status,
                             follow_up_7.nutritional_screening_result        as nutritional_screening_result,
                             nutritional_status_of_adult                     as nutritional_status,
                             screening_test_result_tuberculosis              as tb_screening_result,

                             CASE scheduled_visit
                                 WHEN scheduled_visit <> NULL
                                     THEN 'Scheduled'
                                 ELSE 'Unscheduled'
                                 END                                         as scheduled_visit,
                             antiretroviral_art_dispensed_dose_i             AS art_dose_days,
                             regimen,
                             anitiretroviral_adherence_level                 as adherence,
                             next_visit_date,
                             treatment_end_date,
                             CASE
                                 WHEN transferred_in_check_this_for_all_t = 'Yes'
                                     THEN follow_up_date_followup_
                                 end                                         as ti_date,
                             transferred_in_check_this_for_all_t             as transfer_in,
                             reg.registration_date                           as enrollement_date
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
                        AND FIND_IN_SET(follow_up_status, STATUS)),


         tmp_latest_follow_up AS (SELECT FollowUp.*,
                                         ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY follow_up_date DESC,
                                             encounter_id DESC) AS row_num
                                  FROM FollowUp
                                           join mamba_dim_client client on FollowUp.client_id = client.client_id),




         latest_follow_up AS (select *
                              from tmp_latest_follow_up
                              where row_num = 1),

         previous_regiment as (select l.client_id,
                                      antiretroviral_side_effects,
                                      follow_up_7.reason_for_art_regimen_change                                           as reason_substitution,
                                      follow_up_5.regimen_switch_type                                                     as regimen_switch,
                                      CASE
                                          when follow_up_1.regimen is null then l.regimen
                                          else follow_up_1.regimen end                                                    as previous_regimen,
                                      ROW_NUMBER() OVER (PARTITION BY l.client_id ORDER BY follow_up_6.encounter_id DESC) as rn

                               from latest_follow_up as l
                                        left join mamba_flat_encounter_follow_up_1 as follow_up_1
                                                  on l.client_id = follow_up_1.client_id
                                        left join mamba_flat_encounter_follow_up_6 follow_up_6 on
                                   follow_up_1.encounter_id = follow_up_6.encounter_id
                                        left join mamba_flat_encounter_follow_up_5 follow_up_5 on
                                   follow_up_1.encounter_id = follow_up_5.encounter_id
                                        left join mamba_flat_encounter_follow_up_7 follow_up_7
                                                  on follow_up_7.encounter_id = follow_up_1.encounter_id
                               where follow_up_1.regimen <> l.regimen),

         deduped_previous_regiment AS (SELECT * FROM previous_regiment WHERE rn = 1)

    SELECT ROW_NUMBER() OVER (ORDER BY patient_name)                 as `#`,
           client.patient_uuid                                       as `Patient GUID`,
           client.patient_name                                       as `Patient Name`,
           CAST(client.mrn AS CHAR(20))                              as MRN,
           client.uan                                                AS UAN,
           client.date_of_birth                                      as `Date of Birth`,
           TIMESTAMPDIFF(YEAR, client.date_of_birth,
                         COALESCE(enrollement_date, CURDATE()))      as `Age At Enrollment`,
           TIMESTAMPDIFF(YEAR, client.date_of_birth,
                         COALESCE(REPORT_END_DATE, CURDATE()))       as `Current Age`,
           client.sex                                                as Sex,
           f_case.enrollement_date                                   as `Enrollment Date E.C`,
           f_case.enrollement_date                                   as `Enrollment Date G.C.`,
           f_case.date_hiv_confirmed                                 as `Date Confirmed HIV+ E.C`,
           f_case.date_hiv_confirmed                                 as `Date Confirmed HIV+ G.C.`,
           f_case.art_start_date                                     as `ART Start Date EC`,
           TIMESTAMPDIFF(DAY, f_case.date_hiv_confirmed,
                         COALESCE(f_case.art_start_date, CURDATE())) as `Time to ART Initiation (in days)`,
           f_case.transfer_in                                        as `TI ?`,
           f_case.ti_date                                            as `TI Date`,
           f_case.follow_up_date                                     as `Latest Follow-Up Date E.C`,
           f_case.follow_up_date                                     as `Latest Follow-Up Date G.C.`,
           f_case.scheduled_visit                                    as `Scheduled Type`,
           f_case.follow_up_status                                   AS `Latest Follow-Up Status`,
           f_case.regimen                                            as `Current Regimen`,
           f_case.art_dose_days                                      as `Regimen Dose Days`,
           f_case.adherence                                          as `Adherence`,
           d.previous_regimen                                        as `Previous Regimen`,
           d.reason_substitution                                     as `Reason for Regimen Switch/Substitution`,
           d.regimen_switch                                          as `Regimen Switch/Substitution`,
           d.antiretroviral_side_effects                             as `Side Effects`,
           f_case.weight                                             as `Weight (kg)`,
           f_case.current_who_hiv_stage                              as `WHO Stage`,
           f_case.functional_status                                  as `Functional Status`,
           f_case.pregnancy_status                                   as `Pregnancy Status`,
           f_case.breastfeeding_status                               as `Breastfeeding Status`,
           f_case.family_planning_method                             as `Family Planning`,
           f_case.tb_related_ois                                     as `OIs/HIV related cancers`,
           f_case.cd4                                                as `CD4 `,
           f_case.cd4_count_1                                        as `CD4 %`,
           f_case.disclosure_stage                                   as `Disclosure Stage(DSO-3) `,
           f_case.vl_received_date                                   as `Latest VL Received Date E.C`,
           f_case.vl_received_date                                   as `Latest VL Received Date G.C.`,
           f_case.vl_test_status                                     as `Latest VL Status`,
           f_case.nutritional_screening_result                       as `Nutritional Screening Result`,
           f_case.nutritional_status                                 as `Nutritional Status`,
           f_case.tb_screening_result                                as `TB Screening Result`,
           f_case.next_visit_date                                    as `Next Visit Date E.C`,
           f_case.next_visit_date                                    as `Next Visit Date G.C.`,
           f_case.treatment_end_date                                 as `Last TX_CURR Date E.C`,
           f_case.treatment_end_date                                 as `Last TX_CURR Date G.C.`,
           client.state_province                                     as `Region`,
           client.county_district                                    as `Zone`,
           client.city_village                                       as `Woreda`,
           client.kebele                                             as `Kebele`,
           client.house_number                                       as `House #`,
           client.mobile_no                                          as `Mobile Phone Number.`
    FROM latest_follow_up AS f_case
             INNER JOIN mamba_dim_client client on f_case.client_id = client.client_id
             LEFT JOIN deduped_previous_regiment as d on d.client_id = f_case.client_id
    ORDER BY client.patient_name asc;


END //

DELIMITER ;
