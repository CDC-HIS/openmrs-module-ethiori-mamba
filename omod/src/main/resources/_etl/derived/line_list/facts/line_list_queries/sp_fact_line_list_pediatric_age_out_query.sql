DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_pediatric_age_out_query;

CREATE PROCEDURE sp_fact_line_list_pediatric_age_out_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH Follow_up AS (SELECT follow_up_date_followup_                            AS follow_up_date,
                              follow_up.client_id                                 AS patient_id,
                              follow_up.encounter_id                              AS encounter_id,
                              follow_up_status,
                              TIMESTAMPDIFF(YEAR, date_of_birth, COALESCE(REPORT_END_DATE,CURDATE())) AS age,
                              mrn,
                              uan,
                              sex,
                              patient_uuid,
                              date_of_birth,
                              patient_name,
                              mobile_no                                           as mobilephonenumber,
                              regimen,
                              art_antiretroviral_start_date                       as art_sart_date,
                              date_of_event                                       as date_hiv_confirmed,
                              next_visit_date,
                              antiretroviral_art_dispensed_dose_i                 as art_dose,
                              intake_a.date_enrolled_in_care                      as registration_date
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
                                left JOIN mamba_dim_client dim_client
                                     ON follow_up.client_id = dim_client.client_id
                                left join mamba_flat_encounter_intake_a intake_a
                                          on intake_a.client_id = follow_up.client_id
                       where follow_up_date_followup_
                                 <= COALESCE(REPORT_END_DATE,CURDATE()))
       , firstArtRegimen as (select patient_id,
                                    follow_up_date                                                                              as followupdate,
                                    encounter_id,
                                    ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY follow_up_date DESC, encounter_id DESC) AS rn
                             FROM Follow_up
                             WHERE regimen IS NOT NULL
                             GROUP BY patient_id, encounter_id, follow_up_date)
       , firstArtRegimen2 as (SELECT regimen, Follow_up.patient_id
                              from Follow_up
                                       INNER JOIN firstArtRegimen
                                                  on Follow_up.encounter_id = firstArtRegimen.encounter_id
                              where Follow_up.regimen IS NOT NULL
                                and firstArtRegimen.rn = 1)
       , latest_follow_up_encounter as (SELECT patient_id,
                                               encounter_id,
                                               ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY follow_up_date DESC, encounter_id DESC) AS rn
                                        from Follow_up
                                        where follow_up_status IS NOT NULL
                                          and art_sart_date is not null)


    SELECT DISTINCT patient_name                              AS `Patient Name`,
                    patient_uuid                              AS `UUID`,
                    CAST(mrn AS CHAR(20)) as mrn,
                    uan,
                    sex,
                    date_of_birth                             AS `Date of birth`,
                    date_of_birth                             AS `Date of birth EC.`,
                    TIMESTAMPDIFF(YEAR, date_of_birth, registration_date)
                                                              AS age_at_enrollment,
                    age                                       AS current_age,
                    registration_date                         as `Enrollment Date`,
                    registration_date                         as `Enrollment Date EC.`,
                    date_hiv_confirmed                        as `HIV Confirmed Date`,
                    date_hiv_confirmed                        as `HIV Confirmed Date EC.`,
                    art_sart_date                             as `Art Start Date`,
                    art_sart_date                             as `Art Start Date EC.`,
                    follow_up_date                            AS `Follow Up Date`,
                    follow_up_date                            AS `Follow Up Date EC.`,
                    CASE follow_up_status
                        WHEN 'Alive' THEN 'Alive on ART'
                        WHEN 'Restart medication' THEN 'Restart'
                        WHEN 'Transferred out' THEN 'TO'
                        WHEN 'Stop all' THEN 'Stop'
                        WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
                        WHEN 'Ran away' THEN 'Drop'
                        END AS follow_up_status,

                    firstArtRegimen2.regimen                  AS arv_regimen_when_started_art,
                    Follow_up.regimen                         AS `Regimen`,
                    art_dose                                  AS `Dose Days`,
                    next_visit_date                           AS `Next Visit Date`,
                    next_visit_date                           AS `Next Visit Date EC.`,
                    DATE_ADD(
                            date_of_birth, INTERVAL 15 YEAR
                    )                                         AS `Date of 15th Birth Day`,
                    DATE_ADD(date_of_birth, INTERVAL 15 YEAR) AS `Date of 15th Birth Day EC.`,
                    CASE
                        WHEN TIMESTAMPDIFF(YEAR, date_of_birth, COALESCE(REPORT_END_DATE,CURDATE())) >= 15 THEN 'YES'
                        ELSE 'NO'
                        END                                   AS age_out
    FROM latest_follow_up_encounter
             left join Follow_up on latest_follow_up_encounter.encounter_id = Follow_up.encounter_id
             LEFT OUTER JOIN firstArtRegimen2 on firstArtRegimen2.patient_id = Follow_up.patient_id
    where rn = 1

      and (TIMESTAMPDIFF(YEAR, date_of_birth, art_sart_date) <= 15)
      and (
        (REPORT_START_DATE is not null and REPORT_END_DATE is not null and DATE_ADD(
                date_of_birth, INTERVAL 15 YEAR
                                                                           ) BETWEEN REPORT_START_DATE AND REPORT_END_DATE)
          OR
        ((REPORT_START_DATE is null and REPORT_END_DATE is null and DATE_ADD(
                date_of_birth, INTERVAL 15 YEAR
                                                                            ) <= CURDATE()))
        )
    order by patient_name;
END //

DELIMITER ;