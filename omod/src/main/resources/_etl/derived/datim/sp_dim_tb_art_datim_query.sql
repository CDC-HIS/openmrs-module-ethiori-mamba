DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_tb_art_datim_query;

CREATE PROCEDURE sp_dim_tb_art_datim_query(IN REPORT_START_DATE date,
                                           IN REPORT_END_DATE date,
                                           IN IS_COURSE_AGE_GROUP tinyint(1),
                                           IN REPORT_TYPE varchar(100)
)
BEGIN

    DECLARE age_group_cols VARCHAR(5000);
    DECLARE tx_tb_query VARCHAR(6000);
    DECLARE group_query TEXT;
    DECLARE outcome_condition VARCHAR(227);
    SET session group_concat_max_len = 20000;

    IF REPORT_TYPE = 'ALREADY_ON_ART' THEN
        SET outcome_condition =
                ' art_start_date < ? ';
    ELSEIF REPORT_TYPE = 'NEW_ON_ART' THEN
        SET outcome_condition =
                ' art_start_date between ? AND ? ';
    ELSE
        SET outcome_condition = '1=1';
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

    SET tx_tb_query = 'WITH FollowUp AS (select follow_up.encounter_id,
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
                         date_started_on_tuberculosis_prophy,
                         screening_test_result_tuberculosis     screening_result,
                         lf_lam_result,
                         patient_diagnosed_with_active_tuber,
                         diagnosis_date as active_tb_diagnosed_date,
                         tuberculosis_drug_treatment_start_d as tb_treatment_start_date,
                         date_active_tbrx_completed,
                         date_active_tbrx_dc,
                         currently_taking_tuberculosis_proph
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
                                     follow_up_date                                                                             ,
                                     encounter_id,
                                     follow_up_status,
                                     tb_screening_date,
                                     art_start_date,
                                     tb_screened,
                                     screening_result,
                                     lf_lam_result,
                                     patient_diagnosed_with_active_tuber,
                                     active_tb_diagnosed_date,
                                     tb_treatment_start_date,
                                     date_active_tbrx_completed,
                                     date_active_tbrx_dc,
                                     currently_taking_tuberculosis_proph,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND art_start_date IS NOT NULL
                                AND follow_up_date <= ?),
     tb_art as (
         select tmp_latest_follow_up.*,
                sex,
                date_of_birth,
                mrn,
                uan,
                (SELECT datim_agegroup from mamba_dim_agegroup where TIMESTAMPDIFF(YEAR,date_of_birth,?)=age) as fine_age_group,
                (SELECT normal_agegroup from mamba_dim_agegroup where TIMESTAMPDIFF(YEAR,date_of_birth,?)=age) as coarse_age_group
         from tmp_latest_follow_up
                  join mamba_dim_client client on client.client_id=tmp_latest_follow_up.client_id
         where follow_up_status in (''Alive'',''Restart medication'')
           and row_num=1
           and art_start_date <= ?
           and (active_tb_diagnosed_date <= ? OR tb_treatment_start_date <= ?)
           and (
             (date_active_tbrx_dc is null and date_active_tbrx_completed is null and  (DATE_ADD(tb_treatment_start_date, INTERVAL 1 YEAR ) >= ? ))
                 OR
             (date_active_tbrx_completed > ? OR date_active_tbrx_dc > ? )
             )
     ) ';
    IF REPORT_TYPE = 'TOTAL' THEN
        SET group_query = 'SELECT COUNT(*) AS NUMERATOR FROM tb_art';
    ELSEIF REPORT_TYPE = 'DEBUG' THEN
        SET group_query = 'SELECT * FROM tb_art';
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
          FROM tb_art WHERE ', outcome_condition, '
          GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
        USING (sex)
        GROUP BY sex
        ');
    END IF;
    SET @sql = CONCAT(tx_tb_query, group_query);
    PREPARE stmt FROM @sql;
    SET @start_date = REPORT_START_DATE;
    SET @end_date = REPORT_END_DATE;
    IF REPORT_TYPE = 'ALREADY_ON_ART' THEN
        EXECUTE stmt USING @end_date, @end_date, @end_date, @end_date , @end_date, @end_date , @end_date, @end_date, @end_date, @start_date;
    ELSEIF REPORT_TYPE = 'NEW_ON_ART' THEN
        EXECUTE stmt USING @end_date, @end_date, @end_date, @end_date , @end_date, @end_date , @end_date, @end_date, @end_date, @start_date , @end_date;
    ELSEIF REPORT_TYPE = 'TOTAL' OR REPORT_TYPE = 'DEBUG' THEN
        EXECUTE stmt USING @end_date, @end_date, @end_date, @end_date , @end_date, @end_date , @end_date, @end_date, @end_date;
    END IF;
    DEALLOCATE PREPARE stmt;

END //

DELIMITER ;
