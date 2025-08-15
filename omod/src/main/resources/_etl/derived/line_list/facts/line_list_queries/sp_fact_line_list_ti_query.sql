DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_ti_query;

CREATE PROCEDURE sp_fact_line_list_ti_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_status,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date       AS art_start_date,

                             cervical_cancer_screening_status    AS CCS_ScreenDoneYes,

                             treatment_end_date,
                             weight_text_                        AS Weight,
                             height,
                             date_of_event                       AS date_hiv_confirmed,
                                transferred_in_check_this_for_all_t,
                             antiretroviral_art_dispensed_dose_i AS art_dose_days,
                             regimen,
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
                                         ON follow_up.encounter_id = follow_up_9.encounter_id
                      where follow_up_date_followup_ <= COALESCE(REPORT_END_DATE, CURDATE()
                                                        )),

         first_follow_up as (select client_id,transferred_in_check_this_for_all_t, follow_up_date as ti_follow_up_date,
                                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date  ,
                                     encounter_id  )  AS r_n
                          from FollowUp ),
         ti_follow_up as (select client_id, ti_follow_up_date as follow_up_date
                          from first_follow_up
                          where r_n = 1
                            and transferred_in_check_this_for_all_t ='Yes'
                            and ti_follow_up_date >= COALESCE(REPORT_START_DATE, CURDATE())
                            and ti_follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE()))
         ,

         tmp_last_follow_up AS (SELECT *,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date desc ,
                                            encounter_id desc ) AS row_num
                                 FROM FollowUp),

         in_range_ti_follow_up AS (select *
                             from tmp_last_follow_up
                             where row_num = 1 and  follow_up_date >= COALESCE(REPORT_START_DATE, CURDATE()))





    SELECT ROW_NUMBER() OVER (ORDER BY patient_name)                                       AS `#`,
           client.patient_uuid                                                             as UUID,
           client.patient_name                                                             as `Patient Name`,
           CAST(client.mrn AS CHAR(20))                                                    as `MRN`,
           client.uan                                                                      AS UAN,
           TIMESTAMPDIFF(YEAR, client.date_of_birth, COALESCE(REPORT_END_DATE, CURDATE())) as Age,
           client.sex                                                                      as Sex,
           f_case.art_start_date                                                           as `ART Start Date EC.`,
           f_case.art_start_date                                                           as `ART Start Date GC.`,
           ti.follow_up_date                                                                  as `Follow Up Date (Date
of TI) EC.`,
           ti.follow_up_date                                                                   as `Follow Up Date (Date of TI) GC.`,
           CASE f_case.follow_up_status
               WHEN 'Alive' THEN 'Alive on ART'
               WHEN 'Restart medication' THEN 'Restart'
               WHEN 'Transferred out' THEN 'TO'
               WHEN 'Stop all' THEN 'Stop'
               WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
               WHEN 'Ran away' THEN 'Drop'
               END                                                                         AS `Latest Follow Up Status`,
           f_case.regimen                                                                  as `Last ARV Regimen`,
           f_case.art_dose_days                                                            as `Last ART Dose Days`,
           f_case.adherence                                                                as `Adherence Level`,
           f_case.treatment_end_date                                                       as `Next Visit Date EC.`,
           f_case.treatment_end_date                                                       as `Next Visit Date GC.`
    FROM in_range_ti_follow_up AS f_case
             inner JOIN mamba_dim_client client on
        f_case.client_id = client.client_id
    inner join ti_follow_up as ti on ti.client_id = f_case.client_id;

END //

DELIMITER ;
