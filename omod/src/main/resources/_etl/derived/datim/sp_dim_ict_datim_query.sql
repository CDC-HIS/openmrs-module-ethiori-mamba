DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_ict_datim_query;

CREATE PROCEDURE sp_dim_ict_datim_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN IS_COURSE_AGE_GROUP BOOLEAN,
    IN REPORT_TYPE VARCHAR(100)
)
BEGIN
    DECLARE age_group_cols VARCHAR(5000);
    DECLARE ict_query VARCHAR(6000);
    DECLARE filter_condition VARCHAR(200);
    DECLARE group_query TEXT;
    SET session group_concat_max_len = 20000;


    IF REPORT_TYPE = 'TESTING_OFFERED' THEN
        SET filter_condition = 'offered=''Yes''';
    ELSEIF REPORT_TYPE = 'TESTING_ACCEPTED' THEN
        SET filter_condition='accepted = ''Yes''';
    ELSE
        SET filter_condition = '1=1';
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

    SET ict_query = 'WITH ict_offer as (select * from mamba_flat_encounter_ict_offer where offered_date between ? AND ?) ';

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
          FROM ict_offer WHERE ',filter_condition,' GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
        USING (sex)
        GROUP BY sex
        ');
    SET @sql = CONCAT(ict_query, group_query);
    PREPARE stmt FROM @sql;
    SET @start_date = REPORT_START_DATE;
    SET @end_date = REPORT_END_DATE;
    EXECUTE stmt USING @start_date , @end_date;

    END  //
DELIMITER ;