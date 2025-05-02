DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_tx_new_datim_query;

-- e.g. CALL sp_dim_tx_new_datim_query('2022-10-02', '2023-10-31', '2023-10-31', 0, 'unknown');
CREATE PROCEDURE sp_dim_tx_new_datim_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN IS_COURSE_AGE_GROUP BOOLEAN, -- expected values: 0 for fine age group, 1 for coarse age group
    IN CD4_COUNT_GROUPAGE VARCHAR(255) -- expected values are: 'all', '>=200', '<200', 'unknown'
)
BEGIN

    DECLARE cd4_count_condition VARCHAR(255);
    DECLARE age_group_cols VARCHAR(5000);
    DECLARE art_start_date_start DATE;
    DECLARE art_start_date_end DATE;
    DECLARE tx_new_query VARCHAR(6000);
    DECLARE group_query VARCHAR(5000);

    SET art_start_date_start = REPORT_START_DATE;
    SET art_start_date_end = REPORT_END_DATE;

    SET session group_concat_max_len = 20000;

    IF CD4_COUNT_GROUPAGE = 'all' THEN
        SET cd4_count_condition = '1=1';
    ELSEIF CD4_COUNT_GROUPAGE = '>=200' THEN
        SET cd4_count_condition = 'cd4_count >= 200 AND (age >= 5 or age is null)';
    ELSEIF CD4_COUNT_GROUPAGE = '<200' THEN
        SET cd4_count_condition = 'cd4_count < 200 AND (age >= 5 or age is null)';
    ELSEIF CD4_COUNT_GROUPAGE = 'unknown' THEN
        SET cd4_count_condition = 'cd4_count IS NULL OR age < 5';
    ELSE
        SET cd4_count_condition = '1=1';
    END IF;

    IF IS_COURSE_AGE_GROUP THEN
        IF CD4_COUNT_GROUPAGE = '>=200' OR CD4_COUNT_GROUPAGE = '<200' THEN
            SELECT GROUP_CONCAT( CONCAT('SUM(CASE WHEN coarse_age_group = ''', normal_agegroup,
                                        ''' THEN count ELSE 0 END) AS `',
                                        REPLACE(normal_agegroup, '`', '``'),
                                        '`')
                   )
            INTO age_group_cols
            FROM (select normal_agegroup from mamba_dim_agegroup where datim_age_val > 2 group by normal_agegroup) as order_query;
        ELSE
            SELECT GROUP_CONCAT( CONCAT('SUM(CASE WHEN coarse_age_group = ''', normal_agegroup,
                                        ''' THEN count ELSE 0 END) AS `',
                                        REPLACE(normal_agegroup, '`', '``'),
                                        '`')
                   )
            INTO age_group_cols
            FROM (select normal_agegroup from mamba_dim_agegroup group by normal_agegroup) as order_query;
        END IF;
    ELSE
        IF CD4_COUNT_GROUPAGE = '>=200' OR CD4_COUNT_GROUPAGE = '<200' THEN
            SELECT GROUP_CONCAT( CONCAT('SUM(CASE WHEN fine_age_group = ''', datim_agegroup,
                                        ''' THEN count ELSE 0 END) AS `',
                                        REPLACE(datim_agegroup, '`', '``'),
                                        '`')
                   )
            INTO age_group_cols
            FROM (select datim_agegroup from mamba_dim_agegroup where datim_age_val > 2 group by datim_agegroup) as order_query;
        ELSE
            SELECT GROUP_CONCAT( CONCAT('SUM(CASE WHEN fine_age_group = ''', datim_agegroup,
                                        ''' THEN count ELSE 0 END) AS `',
                                        REPLACE(datim_agegroup, '`', '``'),
                                        '`')
                   )
            INTO age_group_cols
            FROM (select datim_agegroup from mamba_dim_agegroup group by datim_agegroup) as order_query;
        END IF;
    END IF;
    SET tx_new_query = 'WITH FollowUp AS (select follow_up.encounter_id,
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
                               LEFT JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                    ON follow_up.encounter_id = follow_up_1.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                    ON follow_up.encounter_id = follow_up_2.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                         ON follow_up.encounter_id = follow_up_4.encounter_id),
         tmp_latest_follow_up as (SELECT client_id,
                                         follow_up_date                                                                             AS FollowupDate,
                                         encounter_id,
                                         follow_up_status,
                                         art_start_date,
                                         cd4_count,
                                         breast_feeding_status,
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
                               cd4_count,
                               breast_feeding_status
                        from latest_follow_up
                                 join first_follow_up on latest_follow_up.client_id = first_follow_up.client_id
                        where art_start_date BETWEEN ? AND ?
                          AND (first_follow_up.transferred_in is null or first_follow_up.transferred_in != ''Yes'')
                          AND first_follow_up.follow_up_status in (''Alive'', ''Restart medication'')),
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
                            breast_feeding_status,
                           cd4_count
                    from tx_new_tmp
                             join mamba_dim_client client on tx_new_tmp.client_id = client.client_id) ';
    IF CD4_COUNT_GROUPAGE = 'numerator' THEN
        SET group_query = 'SELECT COUNT(*) as Numerator FROM tx_new';
    ELSEIF CD4_COUNT_GROUPAGE = 'breast_feeding' THEN
        SET group_query = 'SELECT COUNT(*) as Breastfeeding from tx_new where breast_feeding_status = ''Yes''';
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
          FROM tx_new
                WHERE ', cd4_count_condition, '
          GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
        USING (sex)
        GROUP BY sex
        ');
    END IF;
    SET @sql = CONCAT(tx_new_query,group_query);
    PREPARE stmt FROM @sql;
    SET @start_date = REPORT_START_DATE;
    SET @end_date = REPORT_END_DATE;
    EXECUTE stmt USING @end_date, @start_date, @end_date;
    DEALLOCATE PREPARE stmt;

END //

DELIMITER ;