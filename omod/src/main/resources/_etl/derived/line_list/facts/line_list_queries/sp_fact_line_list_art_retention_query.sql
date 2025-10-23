DELIMITER //
DROP PROCEDURE IF EXISTS sp_fact_line_list_art_retention_query;
CREATE PROCEDURE sp_fact_line_list_art_retention_query(IN REPORT_START_DATE DATE,
                                                       IN REPORT_END_DATE DATE)
BEGIN
WITH FollowUp AS (select follow_up.encounter_id,
                         follow_up.client_id,
                         follow_up_date_followup_            as follow_up_date,
                         art_antiretroviral_start_date       as art_start_date,
                         height,
                         pregnancy_status,
                         mfeia.date_hiv_confirmed            as date_hiv_confirmed,
                         follow_up_2.weight_text_            as weight,
                         next_visit_date                  as next_vist_date,
                         treatment_end_date,
                         mfeia.date_enrolled_in_care         as enrollement_date,
                         CASE follow_up_status
                             WHEN 'Alive' THEN 'Alive on ART'
                             WHEN 'Restart medication' THEN 'Restart'
                             WHEN 'Transferred out' THEN 'TO'
                             WHEN 'Stop all' THEN 'Stop'
                             WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
                             WHEN 'Ran away' THEN 'Drop'
                             END                             as follow_up_status,
                         regimen,
                         antiretroviral_art_dispensed_dose_i as dose_day,
                         anitiretroviral_adherence_level     as adherence


                  FROM mamba_flat_encounter_intake_a mfeia
                           INNER JOIN mamba_flat_encounter_follow_up follow_up
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
                  WHERE treatment_end_date >= COALESCE(REPORT_END_DATE, CURDATE())
                    AND follow_up_date_followup_ <= COALESCE(REPORT_END_DATE, CURDATE())
                    AND (transferred_out is NULL or transferred_out = 'No'))
     ,


     tmp_latest_follow_up AS (SELECT f.*,
                                     ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY follow_up_date DESC,
                                         encounter_id DESC) AS row_num
                              FROM FollowUp as f
                                       join mamba_dim_client client on f.client_id = client.client_id),


     latest_followup AS (select *
                         from tmp_latest_follow_up
                         where row_num = 1),

    tmp_art_retention as (
        select f.*, ROW_NUMBER() OVER (PARTITION BY f_7.client_id ORDER BY f_7.art_antiretroviral_start_date DESC,
            f_7.encounter_id DESC) AS r_n from
                     mamba_flat_encounter_follow_up_7
                  as f_7 inner join latest_followup as f on f.client_id = f_7.client_id
                 where f_7.art_antiretroviral_start_date >= fn_ethiopian_to_gregorian_calendar(date_add(fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'), INTERVAL -12 MONTH))
                    AND f_7.art_antiretroviral_start_date <= fn_ethiopian_to_gregorian_calendar(date_add(fn_gregorian_to_ethiopian_calendar(REPORT_END_DATE, 'Y-M-D'), INTERVAL -12 MONTH))
    ),

     art_retention AS (select *
    from tmp_art_retention
    where r_n = 1)

SELECT ROW_NUMBER() OVER (ORDER BY patient_name)                   as `#`,
       client.patient_uuid                                         as `Patient GUID`,
       client.patient_name                                         as `Patient Name`,
       CAST(client.mrn AS CHAR(20))                                as MRN,
       client.uan                                                  AS UAN,

       TIMESTAMPDIFF(YEAR, client.date_of_birth,
                     COALESCE(REPORT_END_DATE, CURDATE()))         as `Age`,
       client.sex                                                  as Sex,
       f_case.date_hiv_confirmed                                   as `HIV Confirmed Date EC.`,
       f_case.date_hiv_confirmed                                   as `HIV Confirmed Date GC.`,
       f_case.art_start_date                                       as `ART Start Date EC.`,
       f_case.art_start_date                                       as `ART Start Date G.C`,
      case when   fn_get_ti_status
       (f_case.client_id, REPORT_START_DATE,
        REPORT_END_DATE) ='TI' then 'Yes' else''end                                              as `TI?`,
       f_case.follow_up_date                                       as `Last Follow Up Date EC.`,
       f_case.follow_up_date                                       as `Last Follow Up Date G.C.`,
       f_case.follow_up_status                                     as `Latest Follow Up Status`,
       f_case.regimen                                              as `Latest Regimen`,
       f_case.dose_day                                             as `Latest ARV Dose Days`,
       f_case.adherence                                            as `Latest Adherence`,
       f_case.pregnancy_status                                    as `Pregnant`,
       f_case.next_vist_date                                       as `Next Visit Date EC.`,
       f_case.next_vist_date                                       as `Next Visit Date G.C.`,
       f_case.treatment_end_date                                   as `Treatment End Date in EC.`,
       f_case.treatment_end_date                                   as `Treatment End Date in G.C.`

FROM art_retention AS f_case
         INNER JOIN mamba_dim_client client on f_case.client_id = client.client_id

ORDER BY client.patient_name;

END //

DELIMITER ;
