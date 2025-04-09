DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_tx_new_datim_kp_query;

CREATE PROCEDURE sp_dim_tx_new_datim_kp_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)

WITH FollowUp AS (select follow_up.encounter_id,
                         follow_up.client_id,
                         follow_up_status,
                         follow_up_date_followup_            AS follow_up_date,
                         art_antiretroviral_start_date       AS art_start_date,
                         treatment_end_date,
                         next_visit_date,
                         regimen,
                         currently_breastfeeding_child          breast_feeding_status,
                         pregnancy_status,
                         transferred_in_check_this_for_all_t as transferred_in,
                         cd4_count
                  FROM mamba_flat_encounter_follow_up follow_up
                           JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                ON follow_up.encounter_id = follow_up_1.encounter_id
                           JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                ON follow_up.encounter_id = follow_up_2.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                     ON follow_up.encounter_id = follow_up_3.encounter_id),
     tmp_latest_follow_up as (SELECT client_id,
                                     follow_up_date                                                                             AS FollowupDate,
                                     encounter_id,
                                     follow_up_status,
                                     art_start_date,
                                     cd4_count,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND art_start_date IS NOT NULL
                                AND follow_up_date <= ?),
     latest_follow_up as (SELECT *
                          from tmp_latest_follow_up
                          where row_num = 1),
     tmp_first_follow_up as (SELECT client_id,
                                    follow_up_date                                                                     AS FollowupDate,
                                    encounter_id,
                                    transferred_in,
                                    pregnancy_status,
                                    follow_up_status,
                                    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date , encounter_id ) AS row_num
                             FROM FollowUp
                             WHERE art_start_date IS NOT NULL),
     first_follow_up as (select * from tmp_first_follow_up where row_num = 1),

     tx_new_tmp as (select latest_follow_up.client_id,
                           latest_follow_up.art_start_date,
                           latest_follow_up.FollowupDate,
                           transferred_in,
                           pregnancy_status,
                           cd4_count
                    from latest_follow_up
                             join first_follow_up on latest_follow_up.client_id = first_follow_up.client_id
                    where art_start_date BETWEEN ? AND ?
                      AND (first_follow_up.transferred_in is null or first_follow_up.transferred_in != 'Yes')
                      AND first_follow_up.follow_up_status in ('Alive', 'Restart medication')),
     tx_new as (select tx_new_tmp.client_id,
                       sex,
                       pregnancy_status,
                       date_of_birth,
                       TIMESTAMPDIFF(YEAR, date_of_birth, FollowupDate)               as age,
                       (SELECT datim_agegroup
                        from mamba_dim_agegroup
                        where age = TIMESTAMPDIFF(YEAR, date_of_birth, FollowupDate)) as fine_age_group,
                       (SELECT normal_agegroup
                        from mamba_dim_agegroup
                        where age = TIMESTAMPDIFF(YEAR, date_of_birth, FollowupDate)) as coarse_age_group,
                       cd4_count,
                       (SELECT datim_age_val
                        from mamba_dim_agegroup
                        where age = TIMESTAMPDIFF(YEAR, date_of_birth, FollowupDate)) as datim_age_val
                from tx_new_tmp
                         join mamba_dim_client client on tx_new_tmp.client_id = client.client_id)
END //

DELIMITER ;