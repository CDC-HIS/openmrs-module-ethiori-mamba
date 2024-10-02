DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_vl_eligibility_query;

CREATE PROCEDURE  sp_fact_vl_eligibility_query(
    IN REPORT_END_DATE DATE
)
BEGIN

    WITH Follow_up AS (select follow_up_date_followup_            as follow_up_date,
                              follow_up.client_id                 as patient_id,
                              follow_up.encounter_id              as encounter_id,
                              date_of_reported_hiv_viral_load     as viral_load_sent_date,
                              date_viral_load_results_received    as viral_load_performed_date,
                              regimen_change,
                              follow_up_status,
                              art_antiretroviral_start_date       as art_start_date,
                              viral_load_test_status              as viral_load_status,
                              hiv_viral_load                      as viral_load_count,
                              weight_text_                          as weight,
                              diagnosis_date               as date_hiv_confirmed,
                              antiretroviral_art_dispensed_dose_i as arv_dispensed_dose_days,
                              regimen,
                              next_visit_date                   as next_visit_date,
                              routine_viral_load_test_indication  as routine_viral_load,
                              date_of_last_menstrual_period_lmp_  as lmp,
                              currently_breastfeeding_child       as BreastFeeding,
                              pregnancy_status                    as IsPregnant,
                              treatment_end_date,
                              mrn,
                              uan
                       from mamba_flat_encounter_follow_up follow_up
                                inner join mamba_flat_encounter_follow_up_1 follow_up_1
                                           on follow_up.encounter_id = follow_up_1.encounter_id
                                inner join mamba_flat_encounter_follow_up_2 follow_up_2
                                           on follow_up.encounter_id = follow_up_2.encounter_id
                                left join mamba_dim_client_art_follow_up dim_client
                                          on follow_up.client_id = dim_client.client_id
                       WHERE follow_up_1.follow_up_date_followup_ <= REPORT_END_DATE),
         VLSentdate AS (SELECT Follow_up.patient_id, MAX(viral_load_sent_date) AS VL_Sent_Date
                        FROM Follow_up
                        where
                            viral_load_sent_date is not null
                        GROUP BY Follow_up.patient_id),
         SwitchSubdate AS (SELECT Follow_up.patient_id, MAX(follow_up_date) AS SwitchDate
                           FROM Follow_up
                           WHERE (regimen_change is not null and regimen_change = 'Regimen switch type')
                           GROUP BY Follow_up.patient_id),
         VLPerfdate1 AS (SELECT Follow_up.patient_id, MAX(viral_load_performed_date) AS viral_load_perform_date
                         FROM Follow_up
                         WHERE follow_up_status IS NOT NULL
                           AND art_start_date is not null
                           and viral_load_performed_date is not null
                         GROUP BY Follow_up.patient_id) ,
         VLPerfdate2 AS (SELECT MAX(encounter_id) as encounter_id
                         FROM Follow_up
                                  INNER JOIN VLPerfdate1 on Follow_up.patient_id = VLPerfdate1.patient_id
                             AND Follow_up.viral_load_performed_date = VLPerfdate1.viral_load_perform_date
                         GROUP BY Follow_up.patient_id, Follow_up.viral_load_performed_date),
         VLPerfdate3 AS (SELECT
                             Follow_up.encounter_id,
                             Follow_up.patient_id,
                             viral_load_performed_date,
                             Follow_up.viral_load_status,
                             IF(Follow_up.viral_load_count REGEXP '^[0-9]+(\.[0-9]+)?$' > 0,
                                CAST(viral_load_count AS DECIMAL(12, 2)), NULL) AS viral_load_count,
                             CASE
                                 WHEN VLSentdate.VL_Sent_Date IS NOT NULL AND
                                      VLSentdate.VL_Sent_Date != '1900-01-01 00:00:00.000'
                                     THEN VLSentdate.VL_Sent_Date
                                 WHEN Follow_up.viral_load_performed_date IS NOT NULL
                                     AND Follow_up.viral_load_performed_date != '1900-01-01 00:00:00.000'
                                     THEN Follow_up.viral_load_performed_date
                                 ELSE '1900-01-01 00:00:00.000'
                                 END AS viral_load_ref_date
                         FROM Follow_up
                                  INNER JOIN VLPerfdate2 ON Follow_up.encounter_id = VLPerfdate2.encounter_id
                                  LEFT JOIN VLSentdate ON Follow_up.patient_id = VLSentdate.patient_id),
         temp1 AS (SELECT patient_id, MAX(follow_up_date) AS FollowupDate
                   FROM Follow_up
                   WHERE follow_up_status IS NOT NULL
                     and art_start_date
                   GROUP BY patient_id),
         temp2 AS (SELECT MAX(Follow_up.encounter_id) as encounter_id
                   FROM Follow_up
                            INNER JOIN temp1 on Follow_up.patient_id = temp1.patient_id AND
                                                Follow_up.follow_up_date = temp1.FollowupDate
                   GROUP BY Follow_up.patient_id, Follow_up.follow_up_date),
         temp3 AS (SELECT dim_clinet.mrn,-- reg.PatientId as mrn,
                          dim_clinet.patient_name,-- reg.fullname,
                          dim_clinet.mobile_no,-- address.MobilePhoneNumber,
                          'PhoneNumber',-- address.PhoneNumber,
                          timestampdiff(YEAR, dim_clinet.date_of_birth, curdate()) AS age,-- dbo.fn_Age(reg.DateOfBirth, CURDATE()) AS age,
                          dim_clinet.sex,-- reg.Sex,
                          Follow_up.encounter_id,-- f_case.Id,
                          Follow_up.patient_id,-- f_case.PatientId,
                          dim_clinet.uan,-- f_case.UniqueArtNumber,
                          weight,-- f_case.[Weight],
                          date_hiv_confirmed,-- f_case.date_hiv_confirmed,
                          art_start_date,-- f_case.art_start_date,
                          follow_up_date,-- f_case.FollowUpDate,
                          IsPregnant, -- f_case.IsPregnant,
                          arv_dispensed_dose_days,-- f_case.ARVDispendsedDose,
                          regimen,-- f_case.art_dose,
                          next_visit_date,-- f_case.next_visit_date,
                          follow_up_status,-- f_case.follow_up_status,
                          treatment_end_date, -- 'f_case.art_dose_End',
                          vlperfdate.viral_load_performed_date,-- vlperfdate.viral_load_perform_date,
                          vlperfdate.viral_load_status,-- vlperfdate.viral_load_status,
                          vlperfdate.viral_load_count,-- vlperfdate.viral_load_count,
                          vlsentdate.VL_Sent_Date,-- vlsentdate.VL_Sent_Date,
                          vlperfdate.viral_load_ref_date,-- vlperfdate.viral_load_ref_date,
                          sub_switch_date.SwitchDate                               as date_regimen_change,
                          Follow_up.follow_up_date                                 as FollowUpDate,
                          CASE
                              WHEN
                                  ((Follow_up.follow_up_status = 'Restart medication'))
                                  THEN DATE_ADD(Follow_up.follow_up_date, INTERVAL 91 DAY)
                              WHEN
                                  (
                                      (routine_viral_load is not null) -- Routine viral load test indication 'Routine viral load test indication'
                                      )
                                  THEN DATE_ADD(Follow_up.follow_up_date, INTERVAL 91 DAY)
                              WHEN
                                  (
                                      (routine_viral_load is null)
                                      )
                                  THEN DATE_ADD(Follow_up.follow_up_date, INTERVAL 181 DAY)
                              WHEN (
                                  (SwitchDate IS not NULL and SwitchDate != '1900-01-01 00:00:00.000')
                                  )
                                  THEN date_add(SwitchDate, INTERVAL 181 DAY)

                              WHEN (
                                  (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) <= 12)
                                      AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date IS NULL OR
                                           vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS NULL))
                                  THEN DATE_ADD(Follow_up.art_start_date, INTERVAL 181 DAY)

                              WHEN ((TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) <= 12)
                                  AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                  AND (Follow_up.BreastFeeding is null OR Follow_up.BreastFeeding = 'No')
                                  AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                       (SwitchDate is not null and SwitchDate < vlperfdate.viral_load_ref_date))
                                  AND (vlperfdate.viral_load_ref_date IS NOT NULL AND
                                       vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                  AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51))
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 181 DAY)

                              WHEN ((TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 12)
                                  AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                  AND (Follow_up.BreastFeeding is null OR Follow_up.BreastFeeding = 'No')
                                  AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                       (SwitchDate is not null and SwitchDate <= vlperfdate.viral_load_ref_date))
                                  AND (vlperfdate.viral_load_ref_date > '1900-01-01' and
                                       vlperfdate.viral_load_ref_date IS NOT NULL)
                                  AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51))
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 365 DAY)

                              WHEN ((TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 12)
                                  AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                  AND (Follow_up.BreastFeeding is null OR Follow_up.BreastFeeding = 'No')
                                  AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000')
                                  AND (vlperfdate.viral_load_ref_date = '1900-01-01' or
                                       vlperfdate.viral_load_ref_date IS NULL)
                                  AND (vlperfdate.viral_load_count IS NULL))
                                  THEN REPORT_END_DATE

                              WHEN (
                                  (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) <= 3)
                                      AND (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.BreastFeeding is null or Follow_up.BreastFeeding = 'No')
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000')
                                      AND
                                  (vlperfdate.viral_load_ref_date = '1900-01-01' or vlperfdate.viral_load_ref_date IS NULL)
                                      AND (vlperfdate.viral_load_count IS NULL)
                                  )
                                  THEN DATE_ADD(Follow_up.art_start_date, INTERVAL 91 DAY)

                              WHEN (
                                  (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 3)
                                      AND
                                  (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY)) < 34 and
                                   Follow_up.lmp is not null)
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51)
                                      AND (Abs(TIMESTAMPDIFF(MONTH, vlperfdate.viral_load_ref_date, REPORT_END_DATE)) >= 3)
                                      AND (Follow_up.IsPregnant = 'Yes')
                                  )
                                  THEN REPORT_END_DATE

                              WHEN (
                                  (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 3)
                                      AND (Follow_up.lmp is not null)
                                      AND (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY)) < 34)
                                      AND (vlperfdate.viral_load_ref_date IS NULL or
                                           vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000')
                                      AND (Follow_up.IsPregnant = 'Yes')
                                  )
                                  THEN REPORT_END_DATE

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 3)
                                      AND (Follow_up.lmp is null or Follow_up.lmp = '1900-01-01 00:00:00.000')
                                      AND
                                  (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.follow_up_date, INTERVAL 280 DAY)) <
                                   34)
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51)
                                      AND (TIMESTAMPDIFF(MONTH, vlperfdate.viral_load_ref_date, REPORT_END_DATE) >= 3)
                                  )
                                  THEN REPORT_END_DATE


                              WHEN (
                                  (Follow_up.lmp is not null and Follow_up.lmp != '1900-01-01 00:00:00.000')
                                      AND (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY)) < 34)
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (Follow_up.IsPregnant = 'Yes')
                                  )
                                  THEN DATE_ADD(Follow_up.lmp, INTERVAL 239 DAY)

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.lmp is not null and Follow_up.lmp != '1900-01-01 00:00:00.000')
                                      AND (DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY) <= REPORT_END_DATE)
                                      AND (vlperfdate.viral_load_ref_date IS NULL OR
                                           vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000' or
                                           vlperfdate.viral_load_ref_date < DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY))
                                  )
                                  THEN DATE_ADD(Follow_up.lmp, INTERVAL 371 DAY)

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.lmp is null)
                                      AND (vlperfdate.viral_load_ref_date IS NULL or
                                           vlperfdate.viral_load_ref_date < Follow_up.follow_up_date)
                                  )
                                  THEN DATE_ADD(Follow_up.follow_up_date, INTERVAL 371 DAY)

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.lmp is not null and Follow_up.lmp != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date > (Follow_up.lmp))
                                  )
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 181 DAY)

                              WHEN (
                                  (Follow_up.BreastFeeding = 'Yes')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date < Follow_up.follow_up_date and
                                           TIMESTAMPDIFF(DAY, vlperfdate.viral_load_ref_date, REPORT_END_DATE) > 180)
                                  )
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 181 DAY)

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS not NULL and
                                   vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND
                                  (vlperfdate.viral_load_count IS not NULL and vlperfdate.viral_load_count <= 1000 and
                                   vlperfdate.viral_load_count >= 51)
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                           (SwitchDate is not null and SwitchDate < vlperfdate.viral_load_ref_date))
                                  )
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 91 DAY)

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS not NULL and
                                   vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS not NULL and vlperfdate.viral_load_count > 1000)
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                           (SwitchDate is not null and SwitchDate < vlperfdate.viral_load_ref_date))
                                  )
                                  THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 91 DAY)

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS NULL or
                                   vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000')
                                      AND (SwitchDate IS not NULL and SwitchDate != '1900-01-01 00:00:00.000')
                                  )
                                  THEN DATE_ADD(SwitchDate, INTERVAL 181 DAY)

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS not NULL and
                                   vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (SwitchDate IS not NULL and SwitchDate != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date <= SwitchDate)
                                  )
                                  THEN DATE_ADD(SwitchDate, INTERVAL 181 DAY)

                              When (
                                  (Follow_up.BreastFeeding = 'Yes')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (TIMESTAMPDIFF(DAY, vlperfdate.viral_load_ref_date, REPORT_END_DATE) >= 90)
                                  )
                                  THEN REPORT_END_DATE

                              When (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (TIMESTAMPDIFF(DAY, vlperfdate.viral_load_ref_date, REPORT_END_DATE) >= 90)
                                  )
                                  THEN REPORT_END_DATE

                              ELSE REPORT_END_DATE End                                   AS eligiblityDate,

                          CASE
                              WHEN (
                                  (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) <= 12)
                                      AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date IS NULL OR
                                           vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS NULL))
                                  THEN '1st VL after 6 months'

                              WHEN ((TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) <= 12)
                                  AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                  AND (Follow_up.BreastFeeding is null OR Follow_up.BreastFeeding = 'No')
                                  AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                       (SwitchDate is not null and SwitchDate < vlperfdate.viral_load_ref_date))
                                  AND (vlperfdate.viral_load_ref_date IS NOT NULL AND
                                       vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                  AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51))
                                  THEN '2nd VL at 12 months'

                              WHEN ((TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 12)
                                  AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                  AND (Follow_up.BreastFeeding is null OR Follow_up.BreastFeeding = 'No')
                                  AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                       (SwitchDate is not null and SwitchDate <= vlperfdate.viral_load_ref_date))
                                  AND (vlperfdate.viral_load_ref_date > '1900-01-01' or
                                       vlperfdate.viral_load_ref_date IS NOT NULL)
                                  AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51))
                                  THEN 'Annual VL 1'

                              WHEN ((TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 12)
                                  AND (Follow_up.IsPregnant is null OR Follow_up.IsPregnant = 'No')
                                  AND (Follow_up.BreastFeeding is null OR Follow_up.BreastFeeding = 'No')
                                  AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000')
                                  AND (vlperfdate.viral_load_ref_date = '1900-01-01' or
                                       vlperfdate.viral_load_ref_date IS NULL)
                                  AND (vlperfdate.viral_load_count IS NULL))
                                  THEN 'Annual VL 2'

                              WHEN (
                                  (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) <= 3)
                                      AND (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.BreastFeeding is null or Follow_up.BreastFeeding = 'No')
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000')
                                      AND
                                  (vlperfdate.viral_load_ref_date = '1900-01-01' or vlperfdate.viral_load_ref_date IS NULL)
                                      AND (vlperfdate.viral_load_count IS NULL)
                                  )
                                  THEN '1st VL at 3 months(Pregnant)'

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 3)
                                      AND (Follow_up.lmp is not null)
                                      AND (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY)) < 34)
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51)
                                      AND (TIMESTAMPDIFF(MONTH, vlperfdate.viral_load_ref_date, REPORT_END_DATE) >= 3)
                                  )
                                  THEN 'VL at 1st ANC 1'

                              WHEN (
                                  (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 3)
                                      and (Follow_up.lmp is not null)
                                      AND (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY)) < 34)
                                      AND (vlperfdate.viral_load_ref_date IS NULL or
                                           vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000')
                                      AND (Follow_up.IsPregnant = 'Yes')
                                  )
                                  THEN 'VL at 1st ANC 2'

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) > 3)
                                      AND (Follow_up.lmp is null or Follow_up.lmp = '1900-01-01 00:00:00.000')
                                      AND
                                  (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.follow_up_date, INTERVAL 280 DAY)) <
                                   34)
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 51)
                                      AND (TIMESTAMPDIFF(MONTH, vlperfdate.viral_load_ref_date, REPORT_END_DATE) >= 3)
                                  )
                                  THEN 'VL at 1st ANC 3'

                              WHEN (
                                  (Follow_up.lmp is not null and Follow_up.lmp != '1900-01-01 00:00:00.000')
                                      AND (TIMESTAMPDIFF(WEEK, REPORT_END_DATE, DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY)) < 34)
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (Follow_up.IsPregnant = 'Yes')
                                  )
                                  THEN 'VL at 34-36 weeks gestation'

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.lmp is not null and Follow_up.lmp != '1900-01-01 00:00:00.000')
                                      AND (DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY) <= REPORT_END_DATE)
                                      AND (vlperfdate.viral_load_ref_date IS NULL OR
                                           vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000' or
                                           vlperfdate.viral_load_ref_date < DATE_ADD(Follow_up.lmp, INTERVAL 280 DAY))
                                  )
                                  THEN '3 months after delivery 1'

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.lmp is null)
                                      AND (vlperfdate.viral_load_ref_date IS NULL or
                                           vlperfdate.viral_load_ref_date < Follow_up.follow_up_date)
                                  )
                                  THEN '3 months after delivery 2'

                              WHEN (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (Follow_up.lmp is not null and Follow_up.lmp != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date > (Follow_up.lmp))
                                  )
                                  THEN '6 months after 1st VL at PNC'

                              WHEN (
                                  (Follow_up.BreastFeeding = 'Yes')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date < Follow_up.follow_up_date and
                                           TIMESTAMPDIFF(DAY, vlperfdate.viral_load_ref_date, REPORT_END_DATE) > 180)
                                  )
                                  THEN 'Every 6 months untill MTCT ends'

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS not NULL and
                                   vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND
                                  (vlperfdate.viral_load_count IS not NULL and vlperfdate.viral_load_count <= 1000 and
                                   vlperfdate.viral_load_count >= 51)
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                           (SwitchDate is not null and SwitchDate < vlperfdate.viral_load_ref_date))
                                  )
                                  THEN 'Repeat VL test'

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS not NULL and
                                   vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_count IS not NULL and vlperfdate.viral_load_count > 1000)
                                      AND (SwitchDate IS NULL OR SwitchDate = '1900-01-01 00:00:00.000' or
                                           (SwitchDate is not null and SwitchDate < vlperfdate.viral_load_ref_date))
                                  )
                                  THEN 'Confirmatory VL'

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS NULL or
                                   vlperfdate.viral_load_ref_date = '1900-01-01 00:00:00.000')
                                      AND (SwitchDate IS not NULL and SwitchDate != '1900-01-01 00:00:00.000')
                                  )
                                  THEN '1st VL at 6 months post regimen change/switch 1'

                              WHEN (
                                  (vlperfdate.viral_load_ref_date IS not NULL and
                                   vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (SwitchDate IS not NULL and SwitchDate != '1900-01-01 00:00:00.000')
                                      AND (vlperfdate.viral_load_ref_date <= SwitchDate)
                                  )
                                  THEN '1st VL at 6 months post regimen change/switch 2'

                              When (
                                  (Follow_up.BreastFeeding = 'Yes')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (TIMESTAMPDIFF(DAY, vlperfdate.viral_load_ref_date, REPORT_END_DATE) >= 90)
                                  )
                                  THEN 'Other 1'

                              When (
                                  (Follow_up.IsPregnant = 'Yes')
                                      AND (vlperfdate.viral_load_ref_date IS not NULL and
                                           vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000')
                                      AND (TIMESTAMPDIFF(DAY, vlperfdate.viral_load_ref_date, REPORT_END_DATE) >= 90)
                                  )
                                  THEN 'Other 2'


                              ELSE 'Other 3' End                                   AS vl_key,

                          CASE
                              WHEN (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE) <= 12)
                                  THEN (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE))
                              ELSE (TIMESTAMPDIFF(MONTH, Follow_up.art_start_date, REPORT_END_DATE))
                              END                                                  AS datediffer,

                          CASE
                              WHEN 1 = 1
                                  THEN DATE_ADD(Follow_up.art_start_date, INTERVAL
                                                365 * TIMESTAMPDIFF(YEAR, Follow_up.art_start_date, REPORT_END_DATE) DAY)
                              ELSE DATE_ADD(Follow_up.art_start_date, INTERVAL
                                            365 * TIMESTAMPDIFF(YEAR, Follow_up.art_start_date, REPORT_END_DATE) DAY)
                              END                                                  AS datesecond,
                          Follow_up.BreastFeeding,
                          Follow_up.lmp
                   FROM Follow_up
                            INNER JOIN temp2 ON Follow_up.encounter_id = temp2.encounter_id
                            LEFT JOIN VLPerfdate3 as vlperfdate ON vlperfdate.patient_id = Follow_up.patient_id
                            LEFT join VLSentdate as vlsentdate ON vlsentdate.patient_id = Follow_up.patient_id
                            LEFT join SwitchSubdate as sub_switch_date ON sub_switch_date.patient_id = Follow_up.patient_id
                            INNER join mamba_dim_client_art_follow_up dim_clinet
                                       on Follow_up.patient_id = dim_clinet.client_id),

         temp4 as (SELECT temp3.mrn,
                          temp3.patient_name,
                          temp3.uan,
                          temp3.mobile_no,
                          ' ',
                          temp3.age,
                          temp3.sex,
                          temp3.weight,
                          temp3.date_hiv_confirmed date_hiv_confirmed,
                          temp3.art_start_date      as art_start_date,
                          temp3.follow_up_date      as follow_up_date,
                          IsPregnant,
                          temp3.regimen                                                   AS ARVDispendsedDose,
                          arv_dispensed_dose_days,
                          temp3.next_visit_date     as next_visit_date,
                          temp3.follow_up_status,
                          treatment_end_date        as treatment_end_date,
                          viral_load_performed_date as viral_load_performed_date,
                          viral_load_status,
                          viral_load_count,
                          VL_Sent_Date              as VL_Sent_Date,
                          viral_load_ref_date       as viral_load_ref_date,
                          date_regimen_change       as date_regimen_change,
                          eligiblityDate            as eligiblityDate,
                          vl_key,
                          CASE
                              WHEN IsPregnant = 'Yes' or BreastFeeding = 'Yes' THEN 'Yes'
                              ELSE 'No' END                                               AS PMTCT_ART
                   FROM temp3
             -- WHERE follow_up_status != 'Dead'
             -- and eligiblityDate between @DateFrom and @DateTo
         )select
              mrn, patient_name as fullname, uan as UniqueArtNumber, mobile_no as MobilePhoneNumber, '' PhoneNumber,  age,  sex, weight as Weight,  date_hiv_confirmed,
              art_start_date, follow_up_date as FollowUpDate,  IsPregnant,  ARVDispendsedDose, arv_dispensed_dose_days as art_dose,  next_visit_date,  follow_up_status, treatment_end_date as art_dose_End,
              viral_load_performed_date as viral_load_perform_date,  viral_load_status,  viral_load_count, VL_Sent_Date as viral_load_sent_date,  viral_load_ref_date,
              date_regimen_change,    eligiblityDate,    vl_key,    PMTCT_ART
    from temp4;

END //

DELIMITER ;