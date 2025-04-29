DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_tx_rtt_datim_query;

CREATE PROCEDURE sp_dim_tx_rtt_datim_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN IS_COURSE_AGE_GROUP BOOLEAN, -- expected values: 0 for fine age group, 1 for coarse age group
    IN REPORT_TYPE VARCHAR(100)
)
BEGIN
    DECLARE age_group_cols VARCHAR(5000);
    DECLARE tx_rtt_query VARCHAR(6746);
    DECLARE group_query TEXT;
    DECLARE outcome_condition VARCHAR(227);
    SET session group_concat_max_len = 20000;

    IF REPORT_TYPE = 'CD4_LESS_THAN_200' THEN
        SET outcome_condition =
                ' art_start_date < ? ';
    ELSEIF REPORT_TYPE = 'CD4_GREATER_THAN_200' THEN
        SET outcome_condition =
                ' art_start_date between ? AND ? ';
    ELSEIF REPORT_TYPE = 'CD4_UNKNOWN' THEN
        SET outcome_condition = '';
    ELSEIF REPORT_TYPE = 'CD4_NOT_ELIGIBLE' THEN
        SET outcome_condition = '';
    END IF;

    IF IS_COURSE_AGE_GROUP THEN
        SELECT GROUP_CONCAT(CONCAT('SUM(CASE WHEN coarse_age_group = ''', normal_agegroup,
                                   ''' THEN count ELSE 0 END) AS `',
                                   REPLACE(normal_agegroup, '`', '``'),
                                   '`')
               )
        INTO age_group_cols
        FROM (select normal_agegroup from mamba_dim_agegroup group by normal_agegroup) as order_query;
    ELSE
        SELECT GROUP_CONCAT(CONCAT('SUM(CASE WHEN fine_age_group = ''', datim_agegroup,
                                   ''' THEN count ELSE 0 END) AS `',
                                   REPLACE(datim_agegroup, '`', '``'),
                                   '`')
               )
        INTO age_group_cols
        FROM (select datim_agegroup from mamba_dim_agegroup group by datim_agegroup) as order_query;
    END IF;

    SET tx_rtt_query = 'WITH FollowUp AS (SELECT follow_up.encounter_id,
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
                                      AND follow_up_date <= ?),
     tx_curr_start AS (select *
                       from tmp_latest_follow_up_start
                       where row_num = 1
                         AND follow_up_status in (''Alive'', ''Restart medication'')
                         AND treatment_end_date >= ?),
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
                                    AND follow_up_date <= ?),
     latest_follow_up_status as (select follow_up_status, client_id from tmp_latest_follow_up_end where row_num = 1),
     tx_curr_end AS (select *
                     from tmp_latest_follow_up_end
                     where row_num = 1
                       AND follow_up_status in (''Alive'', ''Restart medication'')
                       AND treatment_end_date >= ?),

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

     tx_new_tmp as (select tmp_latest_follow_up_end.*
                    from tmp_latest_follow_up_end
                             join first_follow_up on tmp_latest_follow_up_end.client_id = first_follow_up.client_id
                    where art_start_date BETWEEN ? AND ?
                      AND (first_follow_up.transferred_in is null or first_follow_up.transferred_in != ''Yes'')
                      AND first_follow_up.follow_up_status in (''Alive'', ''Restart medication'')
                      AND tmp_latest_follow_up_end.row_num = 1),
     tx_new as (select *
                from tx_new_tmp),

     started_art as (select *
                     from tx_new
                     union ALL
                     select *
                     from tx_curr_start),
     restarted_art as (select tx_curr_end.*,
                              client.sex,
                              client.date_of_birth,
                              mrn,
                              uan
                       from tx_curr_end
                                join mamba_dim_client client on tx_curr_end.client_id = client.client_id
                       where tx_curr_end.client_id not in (select client_id from started_art) and
                           tx_curr_end.client_id not in (select client_id from tmp_latest_follow_up_start where row_num=1
                                                                                                            and follow_up_status=''Transferred out'')
         # tmp_latest_follow_up_start is used to confirm that they are not TO on their latest follow up (before start date) as they would be ignored by started_art as they don''t qualify
     ),
    tmp_restart_follow_up as (
        select restarted_art.*,
               tmp_latest_follow_up_end.follow_up_status restart_status,
               tmp_latest_follow_up_end.cd4_count restart_cd4_count,
               tmp_latest_follow_up_end.follow_up_date as restart_follow_up_date,
               ROW_NUMBER() OVER (PARTITION BY tmp_latest_follow_up_end.client_id ORDER BY tmp_latest_follow_up_end.follow_up_date , tmp_latest_follow_up_end.encounter_id ) AS restart_row
        from tmp_latest_follow_up_end
                 join restarted_art on tmp_latest_follow_up_end.client_id=restarted_art.client_id where tmp_latest_follow_up_end.follow_up_date BETWEEN ? AND ?
    ),
     tx_rtt as ( select * from tmp_restart_follow_up where restart_row=1) ';

        SET group_query = CONCAT('
        SELECT
          sex,
          SUM(CASE WHEN ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), ' is null AND count is not null THEN count ELSE 0 END) AS ''Unknown Age'',
          ', age_group_cols, '
        FROM (
          SELECT
            sex,
            ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), ',
            COUNT(*) AS count
          FROM tb_art WHERE ', outcome_condition, '
          GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
        USING (sex)
        GROUP BY sex
        ');

    SET @sql = CONCAT(tx_rtt_query, group_query);
    PREPARE stmt FROM @sql;
    SET @start_date = REPORT_START_DATE;
    SET @end_date = REPORT_END_DATE;
    IF REPORT_TYPE = 'ALREADY_ON_ART' THEN
        EXECUTE stmt USING @end_date, @start_date , @end_date, @start_date , @end_date, @start_date;
    ELSEIF REPORT_TYPE = 'NEW_ON_ART'  THEN
        EXECUTE stmt USING @end_date, @start_date , @end_date, @start_date , @end_date, @start_date , @end_date;
    END IF;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;