-- $BEGIN
INSERT INTO mamba_fact_client
(client_id, patient_uuid, mrn,
 patient_name, sex, birthdate, age,
 art_start_date, current_status, current_regimen, regimen_line,
 last_visit_date, next_appointment_date, days_overdue,
 last_vl_date, last_vl_result, is_suppressed)
WITH
    -- A. Get Latest Clinical Follow-up
    LatestFollowUp AS (SELECT client_id,
                              follow_up_date_followup_                                                                             as visit_date,
                              next_visit_date,
                              follow_up_status,
                              regimen,
                              treatment_end_date,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC, encounter_id DESC) as rn
                       FROM mamba_flat_encounter_follow_up
                       WHERE follow_up_date_followup_ IS NOT NULL),

    -- B. Get Latest Viral Load
    LatestLab AS (SELECT client_id,
                         date_of_reported_hiv_viral_load                                                          as vl_date,
                         hiv_viral_load                                                                           as vl_result,
                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY date_of_reported_hiv_viral_load DESC) as rn
                  FROM mamba_flat_encounter_follow_up -- Or your specific Lab table
                  WHERE hiv_viral_load IS NOT NULL),

    -- C. Get ART Start Date
    ARTStart AS (SELECT client_id,
                        MIN(art_antiretroviral_start_date) as start_date
                 FROM mamba_flat_encounter_follow_up
                 WHERE art_antiretroviral_start_date IS NOT NULL
                 GROUP BY client_id)

SELECT c.client_id,
       c.patient_uuid,
       c.mrn,
       c.patient_name,
       c.sex,
       c.date_of_birth,
       TIMESTAMPDIFF(YEAR, c.date_of_birth, CURDATE())                    as age,

       -- ART Start
       art.start_date,

       -- Calculated Status (Sticky Logic)
       CASE
           WHEN lfu.follow_up_status IN ('Dead', 'Died') THEN 'Dead'
           WHEN lfu.follow_up_status IN ('Transferred out', 'TO') THEN 'Transferred Out'
           WHEN lfu.treatment_end_date >= CURDATE() THEN 'Active'
           WHEN lfu.visit_date IS NULL THEN 'No Clinical Contact'
           ELSE 'Lost to Follow-up'
           END                                                            as current_status,

       lfu.regimen,

       -- Simple Regimen Line Logic (Customize as needed)
       CASE
           WHEN lfu.regimen LIKE '1%' THEN 'First Line'
           WHEN lfu.regimen LIKE '2%' THEN 'Second Line'
           ELSE 'Other'
           END                                                            as regimen_line,

       lfu.visit_date,
       lfu.next_visit_date,

       -- Days Overdue (Negative means future appointment)
       DATEDIFF(CURDATE(), COALESCE(lfu.next_visit_date, lfu.visit_date)) as days_overdue,

       lab.vl_date,
       lab.vl_result,
       CASE WHEN lab.vl_result < 1000 THEN 1 ELSE 0 END                   as is_suppressed

FROM mamba_dim_client c
         LEFT JOIN LatestFollowUp lfu ON c.client_id = lfu.client_id AND lfu.rn = 1
         LEFT JOIN LatestLab lab ON c.client_id = lab.client_id AND lab.rn = 1
         LEFT JOIN ARTStart art ON c.client_id = art.client_id;
-- $END
