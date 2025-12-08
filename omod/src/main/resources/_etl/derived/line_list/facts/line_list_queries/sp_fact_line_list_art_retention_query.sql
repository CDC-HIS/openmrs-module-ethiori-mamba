DELIMITER //
DROP PROCEDURE IF EXISTS sp_fact_line_list_art_retention_query;
CREATE PROCEDURE sp_fact_line_list_art_retention_query(IN REPORT_START_DATE DATE,
                                                       IN REPORT_END_DATE DATE)
BEGIN
    WITH FollowUp AS (SELECT follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date       AS art_start_date,
                             treatment_end_date,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child          breast_feeding_status,
                             pregnancy_status,
                             transferred_in_check_this_for_all_t AS transferred_in,
                             height,
                             date_of_event            as date_hiv_confirmed,
                             follow_up_2.weight_text_            as weight,
                             next_visit_date                     as next_vist_date,
                             date_enrolled_in_care         as enrollement_date,
                             CASE follow_up_status
                                 WHEN 'Alive' THEN 'Alive on ART'
                                 WHEN 'Restart medication' THEN 'Restart'
                                 WHEN 'Transferred out' THEN 'TO'
                                 WHEN 'Stop all' THEN 'Stop'
                                 WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
                                 WHEN 'Ran away' THEN 'Drop'
                                 END                             as follow_up_status,
                             antiretroviral_art_dispensed_dose_i as dose_day,
                             anitiretroviral_adherence_level     as adherence

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
                                         ON follow_up.encounter_id = follow_up_9.encounter_id),

         tmp_latest_follow_up AS (SELECT client_id,
                                         follow_up_date                                                                             AS FollowupDate,
                                         encounter_id,
                                         follow_up_status,
                                         treatment_end_date,
                                         art_start_date,
                                         pregnancy_status,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
             --  AND follow_up_date <= REPORT_END_DATE

         ),
         latest_follow_up as (select * from tmp_latest_follow_up where row_num = 1),
         tmp_art_start_date_before_12_months as (SELECT encounter_id,
                                                        client_id,
                                                        follow_up_date                                                                             AS FollowupDate,
                                                        follow_up_status,
                                                        art_start_date,
                                                        treatment_end_date,
                                                        adherence,
                                                        dose_day,
                                                        next_vist_date,
                                                        date_hiv_confirmed,
                                                        regimen,
                                                        TIMESTAMPDIFF(MONTH, art_start_date, follow_up_date)                                       as months_on_art,
                                                        pregnancy_status,
                                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                                 FROM FollowUp
                                                 WHERE follow_up_status IS NOT NULL
                                                   AND art_start_date IS NOT NULL
                                                   AND art_start_date >= fn_ethiopian_to_gregorian_calendar(date_add(
                                                         fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'),
                                                         INTERVAL -12 MONTH))
                                                   AND art_start_date <= fn_ethiopian_to_gregorian_calendar(date_add(
                                                         fn_gregorian_to_ethiopian_calendar(REPORT_END_DATE, 'Y-M-D'),
                                                         INTERVAL -12 MONTH))
             --  AND follow_up_date <= REPORT_END_DATE
         ),
         art_start_date_before_12_months as (select curr.client_id,
                                                    curr.FollowupDate,
                                                    curr.follow_up_status,
                                                    curr.pregnancy_status,
                                                    client.date_of_birth,
                                                    client.sex,
                                                    date_hiv_confirmed,
                                                    next_vist_date,
                                                    adherence,
                                                    curr.treatment_end_date,
                                                    dose_day,
                                                    regimen,
                                                    curr.art_start_date
                                             from tmp_art_start_date_before_12_months as curr
                                                      join mamba_dim_client client on curr.client_id = client.client_id
                                                      join latest_follow_up latest_follow_up
                                                           on curr.client_id = latest_follow_up.client_id
                                             where curr.row_num = 1
                                               and latest_follow_up.follow_up_status != 'Transferred out')

    SELECT ROW_NUMBER() OVER (ORDER BY patient_name)           as `#`,
           client.patient_uuid                                 as `Patient GUID`,
           client.patient_name                                 as `Patient Name`,
           CAST(client.mrn AS CHAR(20))                        as MRN,
           client.uan                                          AS UAN,

           TIMESTAMPDIFF(YEAR, client.date_of_birth,
                         COALESCE(REPORT_END_DATE, CURDATE())) as `Age`,
           client.sex                                          as Sex,
           f_case.date_hiv_confirmed                           as `HIV Confirmed Date EC.`,
           f_case.date_hiv_confirmed                           as `HIV Confirmed Date GC.`,
           f_case.art_start_date                               as `ART Start Date EC.`,
           f_case.art_start_date                               as `ART Start Date G.C`,
           case
               when fn_get_ti_status
                    (f_case.client_id, REPORT_START_DATE,
                     REPORT_END_DATE) = 'TI' then 'Yes'
               else '' end                                     as `TI?`,
           f_case.FollowupDate                               as `Last Follow Up Date EC.`,
           f_case.FollowupDate                               as `Last Follow Up Date G.C.`,
           f_case.follow_up_status                             as `Latest Follow Up Status`,
           f_case.regimen                                      as `Latest Regimen`,
           f_case.dose_day                                     as `Latest ARV Dose Days`,
           f_case.adherence                                    as `Latest Adherence`,
           f_case.pregnancy_status                             as `Pregnant`,
           f_case.next_vist_date                               as `Next Visit Date EC.`,
           f_case.next_vist_date                               as `Next Visit Date G.C.`,
           f_case.treatment_end_date                           as `Last TX_Curr Date EC.`,
           f_case.treatment_end_date                           as `Last TX_Curr Date G.C.`

    FROM art_start_date_before_12_months AS f_case
             INNER JOIN mamba_dim_client client on f_case.client_id = client.client_id

    ORDER BY client.patient_name;

END //

DELIMITER ;
