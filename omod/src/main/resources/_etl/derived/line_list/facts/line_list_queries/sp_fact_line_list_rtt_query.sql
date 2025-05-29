DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tx_rtt_query;

CREATE PROCEDURE sp_fact_line_list_tx_rtt_query(IN REPORT_END_DATE DATE)
BEGIN
    WITH FollowUp AS (SELECT follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_status,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date       AS art_start_date,
                             antiretroviral_art_dispensed_dose_i,
                             treatment_end_date,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child          breast_feeding_status,
                             pregnancy_status,
                             transferred_in_check_this_for_all_t AS transferred_in,
                             cd4_count,
                             weight_text_,
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
                                               cd4_count,
                                               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                        FROM FollowUp
                                        WHERE follow_up_status IS NOT NULL
                                          AND art_start_date IS NOT NULL
                                          AND follow_up_date <= REPORT_START_DATE),           -- Param 1 @start_date
         latest_follow_up_start as (select *
                                    from tmp_latest_follow_up_start
                                    where row_num = 1),
         tx_curr_start AS (select *
                           from tmp_latest_follow_up_start
                           where row_num = 1
                             AND follow_up_status in ('Alive', 'Restart medication')
                             AND treatment_end_date >= REPORT_START_DATE),                    -- Param 2 @start_date
         interrupted_at_start AS (select latest_follow_up_start.*
                                  from latest_follow_up_start
                                           left join tx_curr_start
                                                     on latest_follow_up_start.client_id = tx_curr_start.client_id
                                  where tx_curr_start.client_id is null
                                    and latest_follow_up_start.follow_up_status != 'Transferred out'),

         -- TX curr
         tmp_latest_follow_up_end AS (SELECT client_id,
                                             follow_up_date,
                                             encounter_id,
                                             follow_up_status,
                                             treatment_end_date,
                                             art_start_date,
                                             cd4_count,
                                             regimen,
                                             weight_text_,
                                             antiretroviral_art_dispensed_dose_i,
                                             adherence,
                                             next_visit_date,
                                             ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                      FROM FollowUp
                                      WHERE follow_up_status IS NOT NULL
                                        AND art_start_date IS NOT NULL
                                        AND follow_up_date <= REPORT_END_DATE),             -- Param 3 @end_date
         -- TX curr
         tmp_restart_follow_up_end AS (SELECT client_id,
                                              follow_up_date                                                                     as restart_follow_up_date,
                                              encounter_id,
                                              follow_up_status                                                                   as restart_status,
                                              treatment_end_date,
                                              art_start_date,
                                              cd4_count                                                                          as restart_cd4_count,
                                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date , encounter_id ) AS row_num
                                       FROM FollowUp
                                       WHERE follow_up_status IS NOT NULL
                                         AND art_start_date IS NOT NULL
                                         AND follow_up_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE), -- Param 4 and 5 @start_date, @end_date
         tx_curr_end AS (select *
                         from tmp_latest_follow_up_end
                         where row_num = 1
                           AND follow_up_status in ('Alive', 'Restart medication')
                           AND treatment_end_date >= REPORT_END_DATE),                      -- Param 6 @end_date
         restart_follow_up_end as (select tmp_restart_follow_up_end.*
                                   from tmp_restart_follow_up_end
                                            join tx_curr_end on tmp_restart_follow_up_end.client_id = tx_curr_end.client_id
                                   where tmp_restart_follow_up_end.row_num = 1),

         tx_rtt as (select tx_curr_end.follow_up_date                          as latest_follow_up_date,
                           tx_curr_end.encounter_id,
                           tx_curr_end.follow_up_status                        as latest_follow_up_status,
                           tx_curr_end.treatment_end_date                      as latest_follow_up_treatment_end_date,
                           tx_curr_end.art_start_date                          as latest_follow_up_art_start_date,
                           tx_curr_end.cd4_count                               as latest_follow_up_cd4_count,
                           restart_follow_up_end.restart_cd4_count,
                           restart_follow_up_end.restart_status,
                           restart_follow_up_end.restart_follow_up_date,
                           sex,
                           date_of_birth,
                           mrn,
                           uan,
                           patient_name,
                           tx_curr_end.regimen,
                           tx_curr_end.weight_text_,
                           tx_curr_end.antiretroviral_art_dispensed_dose_i,
                           tx_curr_end.adherence,
                           tx_curr_end.next_visit_date,
                           (SELECT datim_agegroup
                            from mamba_dim_agegroup
                            where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) = age) as fine_age_group,   -- Param 7 @end_date
                           (SELECT normal_agegroup
                            from mamba_dim_agegroup
                            where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) = age) as coarse_age_group, -- Param 8 @end_date
                           interrupted_at_start.follow_up_date                 as interrupted_follow_up_follow_up_date,
                           interrupted_at_start.follow_up_status               as interrupted_follow_up_follow_up_status,
                           interrupted_at_start.treatment_end_date             as interrupted_follow_up_treatment_end_date,
                           CASE
                               WHEN interrupted_at_start.follow_up_status in ('Alive', 'Restart medication')
                                   and interrupted_at_start.treatment_end_date <= ? THEN -- Param 9 @start_date
                                   TIMESTAMPDIFF(MONTH, interrupted_at_start.treatment_end_date,
                                                 restart_follow_up_end.restart_follow_up_date)
                               ELSE TIMESTAMPDIFF(MONTH, interrupted_at_start.follow_up_date,
                                                  restart_follow_up_end.restart_follow_up_date)
                               END                                             AS 'interrupted_months'


                    from interrupted_at_start
                             join tx_curr_end
                                  on interrupted_at_start.client_id = tx_curr_end.client_id
                             join mamba_dim_client client on interrupted_at_start.client_id = client.client_id
                             join restart_follow_up_end on tx_curr_end.client_id = restart_follow_up_end.client_id)

    select patient_name                          as `Full Name`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) as Age, -- Param 10 @end_date
           Sex,
           weight_text_                          as Weight,
           latest_follow_up_art_start_date       as `ART Start Date`,
           latest_follow_up_art_start_date       as `ART Start Date E.C`,
           restart_follow_up_date                as `Date returned to treatment`,
           restart_follow_up_date                as `Date returned to treatment EC.`,
           interrupted_follow_up_follow_up_status as `Interrupted Follow-up Status`,
           Regimen,
           antiretroviral_art_dispensed_dose_i   as `ARV Dose days`,
           adherence                             as  Adherance,
           next_visit_date                       as `Next Visit Date`,
           next_visit_date                       as `Last Next Visit Date EC.`,
           latest_follow_up_treatment_end_date   as `Last Treatment End Date`,
           latest_follow_up_treatment_end_date   as `Last Treatment End Date EC.`,
           latest_follow_up_date                 as `Last Follow-up Date`,
           latest_follow_up_date                 as `Last Follow-up Date EC.`,
           latest_follow_up_status               as `Last Follow-up Status`,
           interrupted_follow_up_follow_up_date  as `Date excluded from TX_CURR`,
           latest_follow_up_treatment_end_date   as `Last TX_CURR date`,
    from tx_rtt;
END //

DELIMITER ;

