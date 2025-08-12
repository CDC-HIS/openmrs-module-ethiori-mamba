DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_positive_tracking_query;

CREATE PROCEDURE sp_fact_line_list_positive_tracking_query(IN REPORT_START_DATE DATE,
                                                           IN REPORT_END_DATE DATE)
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
                                 END                                        as follow_up_status,
                             follow_up_date_followup_                       as follow_up_date,
                             p_tracking.date_when_final_outcome_was_known   as date_final_outcome_known,
                             art_antiretroviral_start_date                  as art_start_date,
                             height,
                             p_tracking.date_of_hiv_diagnosis               as date_hiv_confirmed,
                             p_tracking.entry_point                         as entry_point,
                             p_tracking.linked_to_hiv_care                  as linked_to_treatment,
                             p_tracking.date_linked_to_hiv_care             as linked_to_treatment_date,
                             p_tracking.reason_art_not_started_the_same_day as reason_art_not_started,
                             p_tracking.plan_for_next_step                  as plan_for_next_step,
                             p_tracking.final_outcome_known,
                             p_tracking.final_outcome,
                             current_who_hiv_stage,
                             cd4_count,
                             follow_up_2.weight_text_                       as weight,
                             antiretroviral_art_dispensed_dose_i            AS art_dose_days,
                             regimen,
                             anitiretroviral_adherence_level                as adherence,
                             next_visit_date,
                             p_tracking.date_enrolled_in_care               as registration_date,
                             treatment_end_date
                      FROM mamba_flat_encounter_positive_tracking p_tracking
                               LEFT JOIN mamba_flat_encounter_follow_up follow_up
                                         on follow_up.client_id = p_tracking.client_id
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
                      WHERE p_tracking.date_of_hiv_diagnosis >= COALESCE
                                                                (REPORT_START_DATE, CURDATE())
                        AND p_tracking.date_of_hiv_diagnosis <= COALESCE
                                                                (REPORT_END_DATE, CURDATE())
                        AND follow_up_date_followup_ <= COALESCE
                                                        (REPORT_END_DATE, CURDATE()))
            ,


         tmp_latest_follow_up AS (SELECT f.*,
                                         ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY follow_up_date DESC,
                                             encounter_id DESC) AS row_num
                                  FROM FollowUp as f),


         latest_follow_up AS (select *
                              from tmp_latest_follow_up
                              where row_num = 1)


    SELECT ROW_NUMBER() OVER (ORDER BY patient_name)                                       as `#`,
           client.patient_uuid                                                             as `Patient GUID`,
           client.patient_name                                                             as `Patient Name`,
           CAST(client.mrn AS
               CHAR(20))                                                                   as MRN,
           client.uan                                                                      AS UAN,
           TIMESTAMPDIFF(YEAR, client.date_of_birth, COALESCE(REPORT_END_DATE, CURDATE())) as `Age`,
           client.sex                                                                      as Sex,
           f_case.registration_date                                                        as `Registration Date in EC.`,
           f_case.registration_date                                                        as `Registration Date in G.C.`,
           f_case.date_hiv_confirmed                                                       as `Date Tested HIV +ve in EC.`,
           f_case.date_hiv_confirmed                                                       as `Date Tested HIV +ve in G.C.`,
           f_case.entry_point                                                              as `Entry Point`,
           f_case.date_hiv_confirmed                                                       as `HIV Confirmed Date in EC.`,
           f_case.date_hiv_confirmed                                                       as `HIV Confirmed Date in G
.C.`,
           f_case.art_start_date                                                           as `ART Start Date EC.`,
           f_case.art_start_date                                                           as `ART Start Date G.C`,
           TIMESTAMPDIFF(YEAR, f_case.art_start_date,
                         COALESCE(date_hiv_confirmed, CURDATE()))                          as `Days Difference`,
           f_case.linked_to_treatment                                                      as `Linked to Care & Treatment?`,
           f_case.linked_to_treatment_date                                                 as `Date Linked to Care & Treatment in EC.`,
           f_case.linked_to_treatment_date                                                 as `Date Linked to Care & Treatment in G.C.`,
           f_case.reason_art_not_started                                                   as `Reason for not Starting ART the same day`,
           f_case.plan_for_next_step                                                       as `Plan for Next Step`,
           f_case.final_outcome_known                                                      as `Final Outcome Known?`,
           f_case.date_final_outcome_known                                                 as `Date Linked to Care &
Treatment in EC.`,
           f_case.date_final_outcome_known                                                 as `Date Linked to Care &
Treatment in G.C.`,
           f_case.final_outcome                                                            as `Final Outcome`,
           client.mobile_no                                                                as `Mobile Phone Number.`,
           client.mobile_no                                                                as `Mobile Phone Number`
    FROM latest_follow_up AS f_case
             INNER JOIN mamba_dim_client client
                        on f_case.client_id = client.client_id
    ORDER BY client.patient_name;

END //

DELIMITER ;
