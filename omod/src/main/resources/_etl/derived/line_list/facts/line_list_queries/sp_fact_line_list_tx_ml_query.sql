DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tx_ml_query;

CREATE PROCEDURE sp_fact_line_list_tx_ml_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH FollowUp AS (SELECT follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_status,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date       AS art_start_date,
                             treatment_end_date,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child          breast_feeding_status,
                             antiretroviral_art_dispensed_dose_i as dose_days,
                             pregnancy_status,
                             transferred_in_check_this_for_all_t AS transferred_in,
                             adherence
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
         -- TX curr start
         tmp_latest_follow_up_start AS (SELECT client_id,
                                               follow_up_date,
                                               encounter_id,
                                               follow_up_status,
                                               treatment_end_date,
                                               art_start_date,
                                               regimen,
                                               dose_days,
                                               adherence,
                                               next_visit_date,
                                               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                        FROM FollowUp
                                        WHERE follow_up_status IS NOT NULL
                                          AND art_start_date IS NOT NULL
                                          AND follow_up_date <= REPORT_START_DATE),
         tx_curr_start AS (select *
                           from tmp_latest_follow_up_start
                           where row_num = 1
                             AND follow_up_status in ('Alive', 'Restart medication')
                             AND treatment_end_date >= REPORT_START_DATE),
         -- TX curr
         tmp_latest_follow_up AS (SELECT client_id,
                                         follow_up_date,
                                         encounter_id,
                                         follow_up_status,
                                         treatment_end_date,
                                         art_start_date,
                                         regimen,
                                         dose_days,
                                         adherence,
                                         next_visit_date,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
                                    AND follow_up_date <= REPORT_END_DATE),
         latest_follow_up as (select *
                              from tmp_latest_follow_up
                              where row_num = 1),
         tx_curr_end AS (select *
                         from tmp_latest_follow_up
                         where row_num = 1
                           AND follow_up_status in ('Alive', 'Restart medication')
                           AND treatment_end_date >= REPORT_END_DATE),

         tmp_first_follow_up as (SELECT client_id,
                                        follow_up_date,
                                        encounter_id,
                                        transferred_in,
                                        pregnancy_status,
                                        follow_up_status,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date , encounter_id ) AS row_num
                                 FROM FollowUp
                                 WHERE art_start_date IS NOT NULL),
         first_follow_up as (select * from tmp_first_follow_up where row_num = 1),

         tx_new_tmp as (select tmp_latest_follow_up.*
                        from tmp_latest_follow_up
                                 join first_follow_up on tmp_latest_follow_up.client_id = first_follow_up.client_id
                        where art_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                          AND first_follow_up.follow_up_status in ('Alive', 'Restart medication')
                          AND tmp_latest_follow_up.row_num = 1),
         tx_new as (select *
                    from tx_new_tmp),

         on_art as (select *
                    from tx_new
                    union ALL
                    select *
                    from tx_curr_start),
         interrupted_art as (select on_art.*,
                                    latest_follow_up.regimen as latest_regimen,
                                    latest_follow_up.dose_days as latest_dose_days,
                                    latest_follow_up.adherence as latest_adherence,
                                    latest_follow_up.next_visit_date as latest_next_visit_date,
                                    client.sex,
                                    client.date_of_birth,
                                    TIMESTAMPDIFF(YEAR, client.date_of_birth, REPORT_END_DATE)        as Age,
                                    patient_name,
                                    patient_uuid,
                                    CAST(client.mrn AS CHAR(20)) as mrn,
                                    uan,
                                    mobile_no,
                                    kebele,
                                    house_number,
                                    city_village,
                                    (SELECT datim_agegroup
                                     from mamba_dim_agegroup
                                     where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) = age) as fine_age_group,
                                    (SELECT normal_agegroup
                                     from mamba_dim_agegroup
                                     where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) = age) as coarse_age_group,
                                    latest_follow_up.follow_up_status                                   as latest_follow_up_status,
                                    latest_follow_up.follow_up_date                                     as latest_follow_up_date
                             from on_art
                                      join mamba_dim_client client on on_art.client_id = client.client_id
                                      join latest_follow_up
                                           on on_art.client_id = latest_follow_up.client_id
                             where on_art.client_id not in (select client_id from tx_curr_end))
    select patient_name                                             as `Patient Name`,
           patient_uuid                                             as `UUID`,
           MRN,
           UAN,
           Age,
           Sex,
           art_start_date                                           as `ART Start Date`,
           art_start_date                                           as `ART Start Date EC.`,
           latest_follow_up_date                                      as `Last Follow-up Date`,
           latest_follow_up_date                                      as `Last Follow-up Date in EC.`,
           latest_follow_up_status                                    as `Last Follow-up Status`,
           latest_regimen                                                  as Regimen,
           latest_dose_days                                                as   `ARV Dose Days`,
           latest_Adherence,
           latest_next_visit_date                                          as `Next Visit Date`,
           latest_next_visit_date                                          as `Next Visit Date EC.`,
           treatment_end_date                                       as `Last TX_CURR Date`,
           treatment_end_date                                       as `Last TX_CURR Date EC.`,
           TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) as `On Treatment For (in months)`,
           -- `On PMTCT?`,
           mobile_no                                                as `Mobile No.`,
           kebele                                                   as `Sub-City`,
           city_village                                             as   Woreda,
           house_number                                             as   `House No.`
    from interrupted_art;


END //

DELIMITER ;