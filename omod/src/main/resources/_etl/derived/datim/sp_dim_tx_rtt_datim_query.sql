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
    DECLARE tx_rtt_query VARCHAR(9658);
    DECLARE group_query TEXT;
    DECLARE outcome_condition VARCHAR(227);
    SET session group_concat_max_len = 20000;

    IF REPORT_TYPE = 'CD4_LESS_THAN_200' THEN
        SET outcome_condition =
                ' (restart_visitect_cd4_result is null and restart_cd4_count < 200) or (restart_visitect_cd4_result = ''VISITECT <200 copies/ml'') ';
    ELSEIF REPORT_TYPE = 'CD4_GREATER_THAN_200' THEN
        SET outcome_condition =
                ' (restart_visitect_cd4_result is null and restart_cd4_count >= 200) or (restart_visitect_cd4_result = ''VISITECT <=200 copies/ml'' or restart_visitect_cd4_result = ''VISITECT >200 copies/ml'') ';
    ELSEIF REPORT_TYPE = 'CD4_UNKNOWN' THEN
        SET outcome_condition = ' restart_cd4_count is null and interrupted_months >= 6';
    ELSEIF REPORT_TYPE = 'CD4_NOT_ELIGIBLE' THEN
        SET outcome_condition = 'interrupted_months < 6';
    ELSE
        SET outcome_condition = '1=1';
    END IF;

    IF IS_COURSE_AGE_GROUP THEN
        IF REPORT_TYPE = 'CD4_LESS_THAN_200' OR REPORT_TYPE = 'CD4_GREATER_THAN_200' OR
           REPORT_TYPE = 'CD4_NOT_ELIGIBLE' OR REPORT_TYPE = 'CD4_UNKNOWN' THEN
            SELECT GROUP_CONCAT(CONCAT('SUM(CASE WHEN coarse_age_group = ''', normal_agegroup,
                                       ''' THEN count ELSE 0 END) AS `',
                                       REPLACE(normal_agegroup, '`', '``'),
                                       '`')
                   )
            INTO age_group_cols
            FROM (select normal_agegroup
                  from mamba_dim_agegroup
                  WHERE datim_age_val > 2
                  group by normal_agegroup) as order_query;
        ELSE
            SELECT GROUP_CONCAT(CONCAT('SUM(CASE WHEN coarse_age_group = ''', normal_agegroup,
                                       ''' THEN count ELSE 0 END) AS `',
                                       REPLACE(normal_agegroup, '`', '``'),
                                       '`')
                   )
            INTO age_group_cols
            FROM (select normal_agegroup from mamba_dim_agegroup group by normal_agegroup) as order_query;
        END IF;
    ELSE
        IF REPORT_TYPE = 'CD4_LESS_THAN_200' OR REPORT_TYPE = 'CD4_GREATER_THAN_200' OR
           REPORT_TYPE = 'CD4_NOT_ELIGIBLE' OR REPORT_TYPE = 'CD4_UNKNOWN' THEN
            SELECT GROUP_CONCAT(CONCAT('SUM(CASE WHEN fine_age_group = ''', datim_agegroup,
                                       ''' THEN count ELSE 0 END) AS `',
                                       REPLACE(datim_agegroup, '`', '``'),
                                       '`')
                   )
            INTO age_group_cols
            FROM (select datim_agegroup
                  from mamba_dim_agegroup
                  WHERE datim_age_val > 2
                  group by datim_agegroup) as order_query;
        ELSE
            SELECT GROUP_CONCAT(CONCAT('SUM(CASE WHEN fine_age_group = ''', datim_agegroup,
                                       ''' THEN count ELSE 0 END) AS `',
                                       REPLACE(datim_agegroup, '`', '``'),
                                       '`')
                   )
            INTO age_group_cols
            FROM (select datim_agegroup from mamba_dim_agegroup group by datim_agegroup) as order_query;
        END IF;
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
                         cd4_count,
                         visitect_cd4_result
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
                                           cd4_count,
                                           visitect_cd4_result,
                                           ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                    FROM FollowUp
                                    WHERE follow_up_status IS NOT NULL
                                      AND art_start_date IS NOT NULL
                                      AND follow_up_date <= ?),  -- Param 1 @start_date
     latest_follow_up_start as (select *
                                from tmp_latest_follow_up_start
                                where row_num = 1),
     tx_curr_start AS (select *
                       from tmp_latest_follow_up_start
                       where row_num = 1
                         AND follow_up_status in (''Alive'', ''Restart medication'')
                         AND treatment_end_date >= ?), -- Param 2 @start_date
     interrupted_at_start AS (select latest_follow_up_start.*
                              from latest_follow_up_start
                                       left join tx_curr_start
                                                 on latest_follow_up_start.client_id = tx_curr_start.client_id
                              where tx_curr_start.client_id is null
                                and latest_follow_up_start.follow_up_status != ''Transferred out''),

     -- TX curr
     tmp_latest_follow_up_end AS (SELECT client_id,
                                         follow_up_date,
                                         encounter_id,
                                         follow_up_status,
                                         treatment_end_date,
                                         art_start_date,
                                         cd4_count,
                                         visitect_cd4_result,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
                                    AND follow_up_date <= ?), -- Param 3 @end_date
     -- TX curr
     tmp_restart_follow_up_end AS (SELECT client_id,
                                         follow_up_date as restart_follow_up_date,
                                         encounter_id,
                                         follow_up_status as restart_status,
                                         treatment_end_date,
                                         art_start_date,
                                         cd4_count as restart_cd4_count,
                                         visitect_cd4_result as restart_visitect_cd4_result,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date , encounter_id ) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
                                    AND follow_up_date BETWEEN ? AND ?), -- Param 4 and 5 @start_date, @end_date
     tx_curr_end AS (select *
                     from tmp_latest_follow_up_end
                     where row_num = 1
                       AND follow_up_status in (''Alive'', ''Restart medication'')
                       AND treatment_end_date >= ?), -- Param 6 @end_date
     restart_follow_up_end as (select tmp_restart_follow_up_end.* from tmp_restart_follow_up_end
                                        join tx_curr_end on tmp_restart_follow_up_end.client_id = tx_curr_end.client_id
                                        where tmp_restart_follow_up_end.row_num=1),

     tx_rtt as (select tx_curr_end.follow_up_date as latest_follow_up_date,
                       tx_curr_end.encounter_id,
                       tx_curr_end.follow_up_status as latest_follow_up_status,
                       tx_curr_end.treatment_end_date as latest_follow_up_treatment_end_date,
                       tx_curr_end.art_start_date as latest_follow_up_art_start_date,
                       tx_curr_end.cd4_count as latest_follow_up_cd4_count,
                       tx_curr_end.visitect_cd4_result as latest_follow_up_visitect_cd4_result,
                       restart_follow_up_end.restart_cd4_count,
                       restart_follow_up_end.restart_visitect_cd4_result,
                       restart_follow_up_end.restart_status,
                       restart_follow_up_end.restart_follow_up_date,
                       sex,
                       date_of_birth,
                       mrn,
                       uan,
                       (SELECT datim_agegroup from mamba_dim_agegroup where TIMESTAMPDIFF(YEAR,date_of_birth,?)=age) as fine_age_group,  -- Param 7 @end_date
                       (SELECT normal_agegroup from mamba_dim_agegroup where TIMESTAMPDIFF(YEAR,date_of_birth,?)=age) as coarse_age_group, -- Param 8 @end_date
                       interrupted_at_start.follow_up_date as interrupted_follow_up_follow_up_date,
                       interrupted_at_start.follow_up_status as interrupted_follow_up_follow_up_status,
                       interrupted_at_start.treatment_end_date as interrupted_follow_up_treatment_end_date,
                       CASE
                           WHEN interrupted_at_start.follow_up_status in (''Alive'', ''Restart medication'')
                               and interrupted_at_start.treatment_end_date <= ? THEN   -- Param 9 @start_date
                               TIMESTAMPDIFF(MONTH, interrupted_at_start.treatment_end_date,
                                             restart_follow_up_end.restart_follow_up_date)
                           ELSE TIMESTAMPDIFF(MONTH, interrupted_at_start.follow_up_date,
                                              restart_follow_up_end.restart_follow_up_date)
                           END AS ''interrupted_months''


                from interrupted_at_start
                         join tx_curr_end
                              on interrupted_at_start.client_id = tx_curr_end.client_id
                        join mamba_dim_client client on interrupted_at_start.client_id= client.client_id
                        join restart_follow_up_end  on tx_curr_end.client_id=restart_follow_up_end.client_id
                ) ';
    IF REPORT_TYPE = 'IIT' THEN
        SET group_query = 'select ''Experienced treatment interruption of <3 months before returning to treatment'' as `IIT`,COUNT(*) as `Value` from tx_rtt where interrupted_months <3
        UNION ALL
        select ''Experienced treatment interruption of 3-5 months before returning to treatment'' ,COUNT(*) from tx_rtt where interrupted_months BETWEEN 3 AND 5
        UNION ALL
        select ''Experienced treatment interruption of 6+ months before returning to treatment'' ,COUNT(*) from tx_rtt where interrupted_months >6';
    ELSEIF REPORT_TYPE = 'TOTAL' THEN
        SET group_query = 'SELECT COUNT(*) AS NUMERATOR FROM tx_rtt';
    ELSEIF REPORT_TYPE = 'DEBUG' THEN
        SET group_query = 'SELECT * FROM tx_rtt';
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
          FROM tx_rtt WHERE ', outcome_condition, '
          GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
        USING (sex)
        GROUP BY sex
        ');
    END IF;
    SET @sql = CONCAT(tx_rtt_query, group_query);
    PREPARE stmt FROM @sql;
    SET @start_date = REPORT_START_DATE;
    SET @end_date = REPORT_END_DATE;
    EXECUTE stmt USING @start_date, @start_date , @end_date, @start_date , @end_date, @end_date , @end_date, @end_date, @start_date;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;