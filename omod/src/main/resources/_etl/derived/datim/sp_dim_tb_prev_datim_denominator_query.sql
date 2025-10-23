DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_tb_prev_datim_denominator_query;

CREATE PROCEDURE sp_dim_tb_prev_datim_denominator_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN IS_COURSE_AGE_GROUP BOOLEAN, -- expected values: 0 for fine age group, 1 for coarse age group
    IN REPORT_TYPE VARCHAR(100)
)
BEGIN
    DECLARE age_group_cols VARCHAR(5000);
    DECLARE tb_prev_query VARCHAR(6000);
    DECLARE group_query TEXT;
    DECLARE outcome_condition VARCHAR(227);
    SET session group_concat_max_len = 20000;

    SET outcome_condition = ' 1 = 1 ';
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

    SET tb_prev_query = 'WITH FollowUp AS (select follow_up.encounter_id,
                         follow_up.client_id,
                         follow_up_status,
                         follow_up_date_followup_            AS follow_up_date,
                         art_antiretroviral_start_date       AS art_start_date,
                         treatment_end_date,
                         next_visit_date,
                         regimen,
                         currently_breastfeeding_child          breast_feeding_status,
                         pregnancy_status,
                         was_the_patient_screened_for_tuberc as tb_screened,
                         tb_screening_date,
                         tb_treatment_status,
                         transferred_in_check_this_for_all_t as transferred_in,
                         date_started_on_tuberculosis_prophy as tpt_start_date,
                         screening_test_result_tuberculosis     screening_result,
                         lf_lam_result,
                         patient_diagnosed_with_active_tuber,
                         diagnosis_date as active_tb_diagnosed_date,
                         tuberculosis_drug_treatment_start_d as tb_treatment_start_date

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

     tmp_tpt_start as (select client_id,
                              tpt_start_date,
                              art_start_date,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY tpt_start_date DESC, FollowUp.encounter_id DESC) AS row_num
                       from FollowUp
                       where tpt_start_date is not null),
     tpt as (
         select tmp_tpt_start.*,
                sex,
                date_of_birth,
                (SELECT datim_agegroup from mamba_dim_agegroup where TIMESTAMPDIFF(YEAR,date_of_birth,?)=age) as fine_age_group,
                (SELECT normal_agegroup from mamba_dim_agegroup where TIMESTAMPDIFF(YEAR,date_of_birth,?)=age) as coarse_age_group
         from tmp_tpt_start
                  join mamba_dim_client client on client.client_id=tmp_tpt_start.client_id
         where row_num=1
           and tpt_start_date >= fn_ethiopian_to_gregorian_calendar(date_add(fn_gregorian_to_ethiopian_calendar(?, ''Y-M-D''), INTERVAL -6 MONTH)) AND tpt_start_date < ? ) ,
     new_art_tpt as ( select * from tpt where art_start_date >= fn_ethiopian_to_gregorian_calendar(date_add(fn_gregorian_to_ethiopian_calendar(?, ''Y-M-D''), INTERVAL -6 MONTH)) AND art_start_date < ?),
     prev_art_tpt as ( select * from tpt where art_start_date < fn_ethiopian_to_gregorian_calendar(date_add(fn_gregorian_to_ethiopian_calendar(?, ''Y-M-D''), INTERVAL -6 MONTH))) ';
    IF REPORT_TYPE = 'TOTAL' THEN
        SET group_query = 'SELECT COUNT(*) AS DENOMINATOR FROM tpt';
    ELSEIF REPORT_TYPE = 'DEBUG' THEN
        SET group_query = 'SELECT * FROM tpt';
    ELSEIF REPORT_TYPE = 'NEW_ART' THEN
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
          FROM new_art_tpt WHERE ', outcome_condition, '
          GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
        USING (sex)
        GROUP BY sex
        ');
    ELSEIF REPORT_TYPE = 'PREV_ART' THEN
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
          FROM prev_art_tpt WHERE ', outcome_condition, '
          GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
        USING (sex)
        GROUP BY sex
        ');
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
          FROM prev_art_tpt WHERE ', outcome_condition, '
          GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
        USING (sex)
        GROUP BY sex
        ');
    END IF;
    SET @sql = CONCAT(tb_prev_query, group_query);
    PREPARE stmt FROM @sql;
    SET @start_date = REPORT_START_DATE;
    SET @end_date = REPORT_END_DATE;
    EXECUTE stmt USING @end_date, @end_date, @start_date , @start_date, @start_date , @start_date, @start_date;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;