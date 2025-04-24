DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_tx_curr_datim_query;

CREATE PROCEDURE sp_dim_tx_curr_datim_query(
    IN REPORT_END_DATE DATE,
    IN IS_COURSE_AGE_GROUP BOOLEAN,
    IN ARV_GROUP INT,
    IN NUMERATOR BOOLEAN
)
BEGIN


    DECLARE age_group_cols VARCHAR(5000);
    DECLARE tx_curr_query VARCHAR(6000);
    DECLARE group_query VARCHAR(5000);
    DECLARE arv_group_condition VARCHAR(100);
    SET session group_concat_max_len = 20000;


    IF ARV_GROUP = 0 THEN
        SET arv_group_condition = 'dose_days < 90 ';
    ELSEIF ARV_GROUP = 1 THEN
        SET arv_group_condition = 'dose_days BETWEEN 90 AND 150 ';
    ELSEIF ARV_GROUP = 2 THEN
        SET arv_group_condition = 'dose_days >= 180';
    ELSE
        SET arv_group_condition = '1=1';
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
    SET tx_curr_query = 'WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id           AS PatientId,
                             follow_up_status,
                             follow_up_date_followup_      AS follow_up_date,
                             art_antiretroviral_start_date AS art_start_date,
                             treatment_end_date,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child   AS breast_feeding_status,
                             pregnancy_status,
                             antiretroviral_art_dispensed_dose_i AS dose_days
                      FROM mamba_flat_encounter_follow_up follow_up
                               JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                    ON follow_up.encounter_id = follow_up_1.encounter_id
                               JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                    ON follow_up.encounter_id = follow_up_2.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id),
         -- TX curr
         tx_curr_all AS (SELECT PatientId,
                                follow_up_date                                                                             AS FollowupDate,
                                encounter_id,
                                follow_up_status,
                                treatment_end_date,
                                dose_days,
                                ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                         FROM FollowUp
                         WHERE follow_up_status IS NOT NULL
                           AND art_start_date IS NOT NULL
                           AND follow_up_date <= ?
                         ),

         tx_curr AS (select *
                     from tx_curr_all
                     where row_num = 1
                       AND follow_up_status in (''Alive'', ''Restart medication'')
                       AND treatment_end_date >= ?),
         tx_curr_with_client as (select tx_curr.*,
                                        client.sex,
                                        client.date_of_birth,
                                        pregnancy_status,
                                        regimen,
                                        left(regimen, 1) as regimen_line,
                                        client.mrn,
                                        client.uan,
                                        fine_age_group,
                                        coarse_age_group
                                 from FollowUp
                                          inner join tx_curr on FollowUp.encounter_id = tx_curr.encounter_id
                                          left join mamba_dim_client client on tx_curr.PatientId = client.client_id) ';

IF NUMERATOR THEN
    SET group_query = ' SELECT COUNT(*) AS Subtotal from tx_curr_with_client';
ELSE
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
          FROM tx_curr_with_client WHERE ',arv_group_condition, '
          GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
        USING (sex)
        GROUP BY sex
        ');
END IF;
    SET @sql = CONCAT(tx_curr_query,group_query);
    PREPARE stmt FROM @sql;
    SET @end_date = REPORT_END_DATE;
    EXECUTE stmt USING @end_date, @end_date;
    DEALLOCATE PREPARE stmt;

END //

DELIMITER ;