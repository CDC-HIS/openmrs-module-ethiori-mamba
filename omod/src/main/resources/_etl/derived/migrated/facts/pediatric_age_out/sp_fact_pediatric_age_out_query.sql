DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_pediatric_age_out_query;

CREATE PROCEDURE sp_fact_pediatric_age_out_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
WITH Follow_up AS (SELECT follow_up_date_followup_                                      AS follow_up_date,
                          follow_up.client_id                                           AS patient_id,
                          follow_up.encounter_id                                        AS encounter_id,
                          follow_up_status,
                          FLOOR(TIMESTAMPDIFF(MONTH, date_of_birth, REPORT_END_DATE) / 12) AS age,
                          mrn,
                          uan,
                          sex,
                          date_of_birth,
                          patient_name                                                  as NAME,
                          mobile_no                                                     as mobilephonenumber,
                          regimen,
                          art_antiretroviral_start_date                                 as art_sart_date,
                          diagnosis_date                                                as date_hiv_confirmed,
                          next_visit_date,
                          antiretroviral_art_dispensed_dose_i                           as art_dose,
                          intake_a.date_enrolled_in_care                                as registration_date
                   FROM mamba_flat_encounter_follow_up follow_up
                            JOIN
                        mamba_flat_encounter_follow_up_1 follow_up_1
                        ON follow_up.encounter_id = follow_up_1.encounter_id
                            join mamba_flat_encounter_follow_up_2 follow_up_2
                                 ON follow_up.encounter_id = follow_up_2.encounter_id
                            join mamba_flat_encounter_follow_up_3 follow_up_3
                                 ON follow_up.encounter_id = follow_up_3.encounter_id
                            JOIN
                        mamba_dim_client_art_follow_up dim_client
                        ON follow_up.client_id = dim_client.client_id
                            left join mamba_flat_encounter_intake_a intake_a on intake_a.client_id = follow_up.client_id
                   where follow_up_date_followup_
                             < curdate())
   , firstArtRegimen as (select patient_id,
                                Min(follow_up_date)                                                                         as followupdate,
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


SELECT DISTINCT Follow_up.patient_id,
                Follow_up.NAME                                 AS patientname,
                mrn,
                uan,
                sex,
                date_of_birth                                  AS date_of_birth_gc,
                fn_gregorian_to_ethiopian_calendar(
                        date_of_birth, 'D-M-Y'
                )                                              AS date_of_birth_et,
                #                 reg.ageatdateofenrollment,
        FLOOR(TIMESTAMPDIFF(MONTH, date_of_birth, registration_date) / 12)
            AS age_at_enrollment,
                age
                                                               AS current_age,
                fn_gregorian_to_ethiopian_calendar(
                        registration_date, 'D-M-Y'
                )                                              AS enrollment_date,
                fn_gregorian_to_ethiopian_calendar(
                        date_hiv_confirmed, 'D-M-Y'
                )                                              AS hivconfirmed_date_et,
                date_hiv_confirmed                             AS hivconfirmed_date_gc,
                fn_gregorian_to_ethiopian_calendar(
                        art_sart_date, 'D-M-Y'
                )                                              AS artstartdate_et,
                art_sart_date                                  AS artstarteddate_gc,
                follow_up_date                                 AS followupdate_gc,
                fn_gregorian_to_ethiopian_calendar(
                        follow_up_date, 'D-M-Y'
                )                                              AS followupdate_ec,
                follow_up_status,
                CASE
                    WHEN TIMESTAMPDIFF(DAY, next_visit_date, CURDATE()) >= 30
                        AND TIMESTAMPDIFF(DAY, next_visit_date, CURDATE()) <= 60 THEN '1lost'
                    WHEN TIMESTAMPDIFF(DAY, next_visit_date, CURDATE()) > 60
                        AND TIMESTAMPDIFF(DAY, next_visit_date, CURDATE()) <= 90 THEN '2lost'
                    WHEN TIMESTAMPDIFF(DAY, next_visit_date, CURDATE()) > 90 THEN 'dropped'
                    ELSE ' ' END                               AS current_status,
                firstArtRegimen2.regimen                       AS arv_regimen_when_started_art,
                Follow_up.regimen                              AS arv_regimen,
                art_dose                   AS ndays,
                CASE
                    WHEN next_visit_date = '1900-01-01' THEN NULL
                    ELSE fn_gregorian_to_ethiopian_calendar(
                            next_visit_date, 'D-M-Y'
                         )
                    END                                        AS nextvisitdate,
                DATE_ADD(
                        date_of_birth, INTERVAL 15 YEAR
                )                                              AS date_of_15th_birthday_gc,
                fn_gregorian_to_ethiopian_calendar(DATE_ADD(
                                                           date_of_birth, INTERVAL 15 YEAR
                                                   ), 'D-M-Y') AS date_of_15th_birthday_et,
                CASE
                    WHEN FLOOR(TIMESTAMPDIFF(MONTH, date_of_birth, REPORT_END_DATE) / 12) >= '15' THEN 'YES'
                    ELSE 'NO'
                    END                                        AS age_out
FROM latest_follow_up_encounter
         left join Follow_up on latest_follow_up_encounter.encounter_id = Follow_up.encounter_id


         LEFT OUTER JOIN firstArtRegimen2 on firstArtRegimen2.patient_id = Follow_up.patient_id
where rn = 1

  and (FLOOR(TIMESTAMPDIFF(MONTH, date_of_birth, registration_date) / 12) <= '15')
order by patientname;
END //

DELIMITER ;