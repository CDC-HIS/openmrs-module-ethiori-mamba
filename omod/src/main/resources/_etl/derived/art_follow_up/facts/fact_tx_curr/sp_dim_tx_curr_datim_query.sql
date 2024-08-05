DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_tx_curr_datim_query;

CREATE PROCEDURE  sp_dim_tx_curr_datim_query(
    IN REPORT_END_DATE DATE,
    IN IS_COURSE_AGE_GROUP TINYINT(1),
    IN CD4_COUNT_GROUPAGE VARCHAR(255)
)
BEGIN

    DECLARE cd4_count_condition VARCHAR(255);
    DECLARE age_group_cols VARCHAR(5000);
    DECLARE end_date_str VARCHAR(20);

    SET session group_concat_max_len = 20000;

    -- Set CD4 count condition
    IF CD4_COUNT_GROUPAGE = 'all' THEN
        SET cd4_count_condition = '1=1';
    ELSEIF CD4_COUNT_GROUPAGE = '>200' THEN
        SET cd4_count_condition = 'cd4_count > 200';
    ELSEIF CD4_COUNT_GROUPAGE = '<200' THEN
        SET cd4_count_condition = 'cd4_count < 200';
    ELSEIF CD4_COUNT_GROUPAGE = 'unknown' THEN
        SET cd4_count_condition = 'cd4_count IS NULL';
    ELSE
        SET cd4_count_condition = '1=1';
    END IF;

    -- Set age group columns
    IF IS_COURSE_AGE_GROUP THEN
        SELECT GROUP_CONCAT(DISTINCT
                            CONCAT(
                                    'MAX(CASE WHEN coarse_age_group = ''',
                                    coarse_age_group,
                                    ''' THEN count ELSE 0 END) AS `',
                                    coarse_age_group, '`'
                            )
                            ORDER BY CAST(SUBSTRING_INDEX(coarse_age_group, '-', 1) AS UNSIGNED)
               )
        INTO age_group_cols
        FROM mamba_dim_client_art_follow_up;
    ELSE
        SELECT GROUP_CONCAT(DISTINCT
                            CONCAT(
                                    'MAX(CASE WHEN fine_age_group = ''',
                                    fine_age_group,
                                    ''' THEN count ELSE 0 END) AS `',
                                    fine_age_group, '`'
                            )
               )
        INTO age_group_cols
        FROM mamba_dim_client_art_follow_up;
    END IF;

    -- Convert REPORT_END_DATE to a string to use in the dynamic query
    SET end_date_str = DATE_FORMAT(REPORT_END_DATE, '%Y-%m-%d');

    -- Construct the dynamic SQL query
    SET @sql = CONCAT('
        SELECT
          sex,
          ', age_group_cols, '
        FROM (
        WITH LatestFollowUp AS (
        SELECT
            client_id,
            follow_up_status,
            cd4_count,
            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY followup_date DESC) AS rn_desc
        FROM mamba_fact_art_follow_up
        WHERE followup_date <= ''', end_date_str, '''
          AND art_start_date <= ''', end_date_str, '''
          AND regimen IS NOT NULL
    )
    SELECT
        sex,
         ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), ',
        COUNT(*) as count
    FROM LatestFollowUp lf
             JOIN mamba_dim_client_art_follow_up dim ON lf.client_id = dim.client_id
    WHERE lf.rn_desc = 1
      AND lf.follow_up_status = ''Alive''
      AND ', cd4_count_condition, '
    GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''MALE'' AS sex UNION SELECT ''FEMALE'') AS genders
        USING (sex)
        GROUP BY sex
    ');

    -- Prepare, execute, and deallocate the prepared statement
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END //

DELIMITER ;