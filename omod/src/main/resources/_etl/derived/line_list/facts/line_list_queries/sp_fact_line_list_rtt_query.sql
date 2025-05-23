DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tx_rtt_query;

CREATE PROCEDURE sp_fact_line_list_tx_rtt_query(IN REPORT_END_DATE DATE)
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
                             pregnancy_status,
                             transferred_in_check_this_for_all_t AS transferred_in,
                             cd4_count
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
                                          AND follow_up_date <= REPORT_START_DATE),
         interrupted_follow_up as (select *
                                   from tmp_latest_follow_up_start
                                   where row_num = 1),
         tx_curr_start AS (select *
                           from tmp_latest_follow_up_start
                           where row_num = 1
                             AND follow_up_status in ('Alive', 'Restart medication')
                             AND treatment_end_date >= REPORT_START_DATE),
         -- TX curr
         tmp_latest_follow_up_end AS (SELECT client_id,
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
                                        AND follow_up_date <= REPORT_END_DATE),
         latest_follow_up_status as (select follow_up_status, client_id
                                     from tmp_latest_follow_up_end
                                     where row_num = 1),
         tx_curr_end AS (select *
                         from tmp_latest_follow_up_end
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
         tmp_restart_follow_up as (select client_id,
                                          follow_up_status                                                                      restart_status,
                                          cd4_count                                                                             restart_cd4_count,
                                          follow_up_date                                                                     as restart_follow_up_date,
                                          ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date , encounter_id ) AS row_num
                                   from tmp_latest_follow_up_end
                                   where tmp_latest_follow_up_end.follow_up_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         restart_follow_up as (select * from tmp_restart_follow_up where row_num = 1),
         first_follow_up as (select * from tmp_first_follow_up where row_num = 1),

         tx_new_tmp as (select tmp_latest_follow_up_end.*
                        from tmp_latest_follow_up_end
                                 join first_follow_up on tmp_latest_follow_up_end.client_id = first_follow_up.client_id
                        where art_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                          AND (first_follow_up.transferred_in is null or first_follow_up.transferred_in != 'Yes')
                          AND first_follow_up.follow_up_status in ('Alive', 'Restart medication')
                          AND tmp_latest_follow_up_end.row_num = 1),
         tx_new as (select *
                    from tx_new_tmp),

         started_art as (select *
                         from tx_new
                         union ALL
                         select *
                         from tx_curr_start),
         restarted_art as (select tx_curr_end.*,
                                  restart_follow_up.restart_cd4_count,
                                  restart_follow_up.restart_follow_up_date,
                                  restart_follow_up.restart_status,
                                  client.sex,
                                  client.date_of_birth,
                                  mrn,
                                  uan,
                                  patient_name,
                                  (SELECT datim_agegroup
                                   from mamba_dim_agegroup
                                   where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) = age) as fine_age_group,
                                  (SELECT normal_agegroup
                                   from mamba_dim_agegroup
                                   where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) = age) as coarse_age_group
                           from tx_curr_end
                                    left join restart_follow_up on tx_curr_end.client_id = restart_follow_up.client_id
                                    join mamba_dim_client client on tx_curr_end.client_id = client.client_id
                           where tx_curr_end.client_id not in (select client_id from started_art)
                             and tx_curr_end.client_id not in (select client_id
                                                               from tmp_latest_follow_up_start
                                                               where row_num = 1
                                                                 and follow_up_status = 'Transferred out')),
         tx_rtt as (select patient_name                             as `Patient Name`,
                           sex,
                           date_of_birth                               `Date Of Birth`,
                           mrn,
                           uan,
                           restarted_art.follow_up_date             as `Follow Up Date`,
                           restarted_art.follow_up_date             as `Follow Up Date EC.`,
                           restarted_art.follow_up_status           as `Follow Up Status`,
                           restarted_art.treatment_end_date         as `Treatment End Date`,
                           restarted_art.treatment_end_date         as `Treatment End Date EC.`,
                           restarted_art.art_start_date             as `Art Start Date`,
                           restarted_art.art_start_date             as `Art Start Date EC.`,
                           restarted_art.cd4_count                  as `CD4 Count`,
                           restarted_art.restart_cd4_count          as `Restart CD4 Count`,
                           restarted_art.restart_status             as `Restart Follow Up Status`,
                           restarted_art.restart_follow_up_date     as `Restart Follow Up Date`,
                           restarted_art.restart_follow_up_date     as `Restart Follow Up Date EC.`,
                           interrupted_follow_up.follow_up_date     as `Interrupted Follow Up Date`,
                           interrupted_follow_up.follow_up_date     as `Interrupted Follow Up Date EC.`,
                           interrupted_follow_up.follow_up_status   as `Interrupted Follow Up Date Status`,
                           interrupted_follow_up.treatment_end_date as `Interrupted Treatment End Date`,
                           interrupted_follow_up.treatment_end_date as `Interrupted Treatment End Date EC.`,
                           CASE
                               WHEN interrupted_follow_up.follow_up_status in ('Alive', 'Restart medication')
                                   and interrupted_follow_up.treatment_end_date <= REPORT_START_DATE THEN
                                   TIMESTAMPDIFF(MONTH, interrupted_follow_up.treatment_end_date,
                                                 restarted_art.restart_follow_up_date)
                               ELSE TIMESTAMPDIFF(MONTH, interrupted_follow_up.follow_up_date,
                                                  restarted_art.restart_follow_up_date)
                               END                                  AS 'interrupted_months'
                    from restarted_art
                             join interrupted_follow_up
                    where restarted_art.client_id = interrupted_follow_up.client_id)

    select *
    from tx_rtt;

    -- Check ti Status not included
    -- left join follow up tables
END //

DELIMITER ;

