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
                                         ON follow_up.encounter_id = follow_up_4.encounter_id),
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
         tmp_lost_follow_up AS (SELECT client_id,
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
         lost_follow_up as (select *
                            from tmp_lost_follow_up
                            where row_num = 1),
         tx_curr_end AS (select *
                         from tmp_lost_follow_up
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

         tx_new_tmp as (select tmp_lost_follow_up.*
                        from tmp_lost_follow_up
                                 join first_follow_up on tmp_lost_follow_up.client_id = first_follow_up.client_id
                        where art_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                          AND (first_follow_up.transferred_in is null or first_follow_up.transferred_in != 'Yes')
                          AND first_follow_up.follow_up_status in ('Alive', 'Restart medication')
                          AND tmp_lost_follow_up.row_num = 1),
         tx_new as (select *
                    from tx_new_tmp),

         on_art as (select *
                    from tx_new
                    union ALL
                    select *
                    from tx_curr_start),
         interrupted_art as (select on_art.*,
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
                                    lost_follow_up.follow_up_status                                   as lost_follow_up_status,
                                    lost_follow_up.follow_up_date                                     as lost_follow_up_date
                             from on_art
                                      join mamba_dim_client client on on_art.client_id = client.client_id
                                      join lost_follow_up
                                           on on_art.client_id = lost_follow_up.client_id
                             where on_art.client_id not in (select client_id from tx_curr_end))
    select patient_name                                             as `Patient Name`,
           patient_uuid                                             as `UUID`,
           MRN,
           UAN,
           Age,
           Sex,
           art_start_date                                           as `ART Start Date`,
           art_start_date                                           as `ART Start Date EC.`,
           lost_follow_up_date                                      as `Last Follow-up Date`,
           lost_follow_up_date                                      as `Last Follow-up Date in EC.`,
           lost_follow_up_status                                    as `Last Follow-up Status`,
           regimen                                                  as Regimen,
           dose_days                                                   `ARV Dose Days`,
           Adherence,
           next_visit_date                                          as `Next Visit Date`,
           next_visit_date                                          as `Next Visit Date EC.`,
           treatment_end_date                                       as `Last TX_CURR Date`,
           treatment_end_date                                       as `Last TX_CURR Date EC.`,
           TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) as `On Treatment For (in months)`,
           -- `On PMTCT?`,
           mobile_no                                                as `Mobile No.`,
           kebele                                                   as `Sub-City`,
           city_village                                                Woreda,
           house_number                                                `House No.`
    from interrupted_art;


END //

DELIMITER ;