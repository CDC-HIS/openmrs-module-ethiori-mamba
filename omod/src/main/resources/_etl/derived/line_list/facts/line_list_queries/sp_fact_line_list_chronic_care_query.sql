DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_chronic_care_query;

CREATE PROCEDURE sp_fact_line_list_chronic_care_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_status,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date       AS art_start_date,
                             height,
                             mfeia.date_hiv_confirmed            AS date_hiv_confirmed,
                             current_who_hiv_stage,
                             cd4_count,
                             antiretroviral_art_dispensed_dose_i AS art_dose_days,
                             regimen,
                             anitiretroviral_adherence_level     as adherence,
                             next_visit_date,
                             treatment_end_date,
                             transferred_in_check_this_for_all_t as transfer_in,
                             reg.registration_date               as enrollement_date

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
                        AND follow_up_date_followup_ <= COALESCE(REPORT_END_DATE, CURDATE())),

         tmp_latest_follow_up AS (SELECT FollowUp.*,
                                         ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY follow_up_date DESC,
                                             encounter_id DESC) AS row_num
                                  FROM FollowUp
                                           join mamba_dim_client client on FollowUp.client_id = client.client_id),

         latest_follow_up AS (select *
                              from tmp_latest_follow_up
                              where row_num = 1)

    SELECT client.patient_uuid                                           as UUID,
           client.patient_name                                           as `Patient Name`,
           CAST(client.mrn AS CHAR(20))                                  as MRN,
           client.uan                                                    AS UAN,
           TIMESTAMPDIFF(YEAR, client.date_of_birth,
                         COALESCE(enrollement_date, CURDATE()))          as `Age At Enrollment`,
           TIMESTAMPDIFF(YEAR, client.date_of_birth,
                         COALESCE(REPORT_END_DATE, CURDATE()))           as `Current Age`,
           client.sex                                                    as Sex,
           client.mobile_no                                               as `Mobile No.`,
           f_case.enrollement_date                                       as `Enrollment Date
EC.`,
           f_case.date_hiv_confirmed                                     as `HIV
Confirmed Date EC.`,
           f_case.art_start_date                                         as `ART Start
Date EC`,
           TIMESTAMPDIFF(DAY, f_case.art_start_date,
                         COALESCE(f_case.date_hiv_confirmed, CURDATE())) as `Time to ART
Initiation (in
days)`,
           f_case.follow_up_date                                         as `Latest Follow-up Date`,
           CASE f_case.follow_up_status
               WHEN 'Alive' THEN 'Alive on ART'
               WHEN 'Restart medication' THEN 'Restart'
               WHEN 'Transferred out' THEN 'TO'
               WHEN 'Stop all' THEN 'Stop'
               WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
               WHEN 'Ran away' THEN 'Drop'
               END                                                       AS `Latest Follow-up Status`,
           f_case.regimen                                                as `Latest Regimen`,
           f_case.art_dose_days                                          as `Regimen Dose Days`,
           f_case.adherence                                              as `Adherence`,
           f_case.next_visit_date                                        as `Next Visit Date EC.`,
           f_case.treatment_end_date                                     as `Last TX_CURR Date EC.`,
           transfer_in                                                   as `TI ?`,
           IF(f_case.transfer_in = 'Yes', f_case.follow_up_date, '--')   AS `TI Date`,
           client.state_province                                         as `Region`,
           client.county_district                                        as `Zone`,
           client.city_village                                           as `Woreda`,
           client.kebele,
           client.house_number                                           as `House #`


    FROM latest_follow_up AS f_case
                        INNER JOIN mamba_dim_client client on f_case.client_id = client.client_id
    ORDER BY client.patient_name asc;


END //

DELIMITER ;
