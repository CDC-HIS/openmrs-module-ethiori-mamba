-- CALL sp_dim_tx_pvls_datim_query ('2023-10-31',0,1);
DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_tx_pvls_datim_query;

CREATE PROCEDURE sp_dim_tx_pvls_datim_query(
    IN REPORT_END_DATE DATE,
    IN IS_COURSE_AGE_GROUP BOOLEAN,
    IN REPORT_TYPE VARCHAR(100)
)
BEGIN


    DECLARE age_group_cols VARCHAR(5000);
    DECLARE pvls_query VARCHAR(6000);
    DECLARE disaggregate_query VARCHAR(6000);
SET session group_concat_max_len = 20000;


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
    SET pvls_query = 'WITH FollowUp AS (select follow_up.encounter_id,
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
                         date_viral_load_results_received    as viral_load_performed_date,
                         hiv_viral_load                      as viral_load_count,
                         viral_load_test_status,
                         viral_load_test_indication,
                         viral_load_received_                as viral_load_performed,
                         date_of_reported_hiv_viral_load     as viral_load_sent_date
                  FROM mamba_flat_encounter_follow_up follow_up
                           JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                ON follow_up.encounter_id = follow_up_1.encounter_id
                           JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                ON follow_up.encounter_id = follow_up_2.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                     ON follow_up.encounter_id = follow_up_3.encounter_id),
     tmp_latest_follow_up AS (SELECT client_id,
                                     follow_up_date                                                                             AS FollowupDate,
                                     encounter_id,
                                     follow_up_status,
                                     treatment_end_date,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND art_start_date IS NOT NULL
                                AND follow_up_date <= ? ),
     latest_follow_up AS (select *
                          from tmp_latest_follow_up
                          where row_num = 1),
     tmp_pvls as (select encounter_id,
                         client_id,
                         follow_up_status,
                         follow_up_date,
                         viral_load_performed_date,
                         pregnancy_status,
                         viral_load_count,
                         viral_load_test_status,
                         viral_load_sent_date,
                         breast_feeding_status,
                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_performed_date DESC, encounter_id DESC) AS row_num
                  from FollowUp
                  where viral_load_performed_date is not null
                    and viral_load_performed_date >= DATE_ADD( ? , INTERVAL -12 MONTH)
                    AND viral_load_performed_date <= ? ),
     tmp_pvls_2 as (select * from tmp_pvls where row_num = 1),
     pvls as (select client.client_id,
                     pregnancy_status,
                     sex,
                     date_of_birth,
                     viral_load_count,
                     viral_load_test_status,
                     follow_up_date,
                     fine_age_group,
                     coarse_age_group,
                     breast_feeding_status
              from tmp_pvls_2
                       inner join mamba_dim_client client on tmp_pvls_2.client_id = client.client_id
                    --   inner join latest_follow_up on tmp_pvls_2.client_id = latest_follow_up.client_id
              ),
     pvls_numerator as (select *
                        from pvls
                        where viral_load_count < 1000 or (viral_load_count  is null and viral_load_test_status in (''Suppressed'',''Low-level viremia''))
                        ) ';
    IF REPORT_TYPE = 'NUMERATOR' THEN
        SET disaggregate_query = CONCAT('
        SELECT
          sex,
          SUM(CASE WHEN ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), ' is null AND count is not null THEN count ELSE 0 END) AS ''Unknown Age'',
          ', age_group_cols, '
        FROM (
          SELECT
            sex,
            ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), ',
            COUNT(*) AS count
          FROM pvls_numerator
          GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
        USING (sex)
        GROUP BY sex
        ');
ELSEIF REPORT_TYPE = 'NUMERATOR_BREAST_FEEDING_PREGNANT' THEN
        SET disaggregate_query = 'SELECT COUNT(CASE WHEN pregnancy_status = ''Yes'' THEN 1 ELSE NULL END ) AS Pregnant,
       COUNT(CASE WHEN breast_feeding_status = ''Yes'' THEN 1 ELSE NULL END ) AS Breastfeeding
        FROM pvls_numerator';
ELSEIF REPORT_TYPE = 'DENOMINATOR' THEN
        SET disaggregate_query = CONCAT('
        SELECT
          sex,
          SUM(CASE WHEN ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), ' is null AND count is not null THEN count ELSE 0 END) AS ''Unknown Age'',
          ', age_group_cols, '
        FROM (
          SELECT
            sex,
            ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), ',
            COUNT(*) AS count
          FROM pvls
          GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
        USING (sex)
        GROUP BY sex
        ');
    ELSEIF REPORT_TYPE = 'DENOMINATOR_BREAST_FEEDING_PREGNANT' THEN
        SET disaggregate_query = 'SELECT COUNT(CASE WHEN pregnancy_status = ''Yes'' THEN 1 ELSE NULL END ) AS Pregnant,
       COUNT(CASE WHEN breast_feeding_status = ''Yes'' THEN 1 ELSE NULL END ) AS Breastfeeding
        FROM pvls';
END IF;
    SET @sql = CONCAT(pvls_query, disaggregate_query);
PREPARE stmt FROM @sql;
SET @end_date = REPORT_END_DATE;
EXECUTE stmt USING @end_date, @end_date, @end_date;
DEALLOCATE PREPARE stmt;

END //
DELIMITER ;