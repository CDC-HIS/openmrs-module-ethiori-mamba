DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_tx_ml_datim_query;

CREATE PROCEDURE sp_dim_tx_ml_datim_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN IS_COURSE_AGE_GROUP BOOLEAN,
    IN REPORT_TYPE VARCHAR(100)
)
BEGIN

    DECLARE age_group_cols VARCHAR(5000);
    DECLARE tx_ml_query VARCHAR(6329);
    DECLARE group_query TEXT;
    DECLARE outcome_condition VARCHAR(150);
    SET session group_concat_max_len = 20000;

    IF REPORT_TYPE = 'DIED' THEN
        SET outcome_condition = ' latest_follow_up_status = ''Dead''';
    ELSEIF REPORT_TYPE = 'LESS_THAN_THREE_MONTHS' THEN
        SET outcome_condition = ' TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) < 3
  AND latest_follow_up_status not in (''Transferred out'', ''Stop all'', ''Dead'')';
    ELSEIF REPORT_TYPE = 'THREE_TO_FIVE_MONTHS' THEN
        SET outcome_condition = ' TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) BETWEEN 3 AND 5
  AND latest_follow_up_status not in (''Transferred out'', ''Stop all'', ''Dead'')';
    ELSEIF REPORT_TYPE = 'MORE_THAN_SIX_MONTHS' THEN
        SET outcome_condition = ' TIMESTAMPDIFF(MONTH, art_start_date, treatment_end_date) > 6
  AND latest_follow_up_status not in (''Transferred out'', ''Stop all'', ''Dead'')';
    ELSEIF REPORT_TYPE = 'TRANSFERRED_OUT' THEN
        SET outcome_condition = ' latest_follow_up_status = ''Transferred out''';
    ELSEIF REPORT_TYPE = 'REFUSED' THEN
        SET outcome_condition = ' latest_follow_up_status = ''Stop all''';
    ELSE
        SET  outcome_condition = '1=1';
    END IF;

    IF IS_COURSE_AGE_GROUP THEN
        SELECT GROUP_CONCAT( CONCAT('SUM(CASE WHEN coarse_age_group = ''', normal_agegroup,
                                    ''' THEN count ELSE 0 END) AS `',
                                    REPLACE(normal_agegroup, '`', '``'),
                                    '`')
               )
        INTO age_group_cols
        FROM (select normal_agegroup from mamba_dim_agegroup group by normal_agegroup) as order_query;
    ELSE
        SELECT GROUP_CONCAT( CONCAT('SUM(CASE WHEN fine_age_group = ''', datim_agegroup,
                                    ''' THEN count ELSE 0 END) AS `',
                                    REPLACE(datim_agegroup, '`', '``'),
                                    '`')
               )
        INTO age_group_cols
        FROM (select datim_agegroup from mamba_dim_agegroup group by datim_agegroup) as order_query;
    END IF;

    SET tx_ml_query = 'WITH FollowUp AS (SELECT follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_status,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date       AS art_start_date,
                             treatment_end_date,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child          breast_feeding_status,
                             pregnancy_status,
                             transferred_in_check_this_for_all_t AS transferred_in
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
                                             ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                      FROM FollowUp
                                      WHERE follow_up_status IS NOT NULL
                                        AND art_start_date IS NOT NULL
                                        AND follow_up_date <= ?),
         latest_follow_up_status as (select follow_up_status, client_id
                                     from tmp_latest_follow_up_end
                                     where row_num = 1),
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
         interrupted_art as (select started_art.*,
                                    client.sex,
                                    client.date_of_birth,
                                    (SELECT datim_agegroup from mamba_dim_agegroup where TIMESTAMPDIFF(YEAR,date_of_birth,?)=age) as fine_age_group,
                                    (SELECT normal_agegroup from mamba_dim_agegroup where TIMESTAMPDIFF(YEAR,date_of_birth,?)=age) as coarse_age_group,
                                    latest_follow_up_status.follow_up_status as latest_follow_up_status
                             from started_art
                                      join mamba_dim_client client on started_art.client_id = client.client_id
                                      join latest_follow_up_status
                                           on started_art.client_id = latest_follow_up_status.client_id
                             where started_art.client_id not in (select client_id from tx_curr_end)) ';
    IF REPORT_TYPE = 'TOTAL' THEN
        SET group_query = 'SELECT COUNT(*) AS NUMERATOR FROM interrupted_art';
    ELSE
    SET group_query = CONCAT('
        SELECT
          sex,
          SUM(CASE WHEN ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), ' is null AND count is not null THEN count ELSE 0 END) AS ''Unknown Age'',
          ', age_group_cols, ' ,
        SUM(CASE WHEN count is not null THEN count ELSE 0 END) as Subtotal
        FROM (
          SELECT
            sex,
            ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), ',
            COUNT(*) AS count
          FROM interrupted_art WHERE ', outcome_condition, '
          GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
        USING (sex)
        GROUP BY sex
        ');
    END IF;
    # SELECT CONCAT(tx_ml_query, group_query);
    SET @sql = CONCAT(tx_ml_query, group_query);
    PREPARE stmt FROM @sql;
    SET @start_date = REPORT_START_DATE;
    SET @end_date = REPORT_END_DATE;
    EXECUTE stmt USING @start_date, @start_date, @end_date, @end_date, @start_date, @end_date, @end_date, @end_date;
    DEALLOCATE PREPARE stmt;

END //

DELIMITER ;