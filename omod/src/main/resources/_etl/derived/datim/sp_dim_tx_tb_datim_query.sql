DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_tx_tb_datim_query;

CREATE PROCEDURE sp_dim_tx_tb_datim_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN IS_COURSE_AGE_GROUP BOOLEAN, -- expected values: 0 for fine age group, 1 for coarse age group
    IN REPORT_TYPE VARCHAR(100)
)
BEGIN
    DECLARE age_group_cols VARCHAR(5000);
    DECLARE tx_tb_query VARCHAR(6000);
    DECLARE group_query TEXT;
    DECLARE outcome_condition VARCHAR(227);
    SET session group_concat_max_len = 20000;

    IF REPORT_TYPE = 'NEW_ART_POSITIVE' THEN
        SET outcome_condition =
                ' art_start_date between ? AND ? and screening_result=''Positive'' ';
    ELSEIF REPORT_TYPE = 'NEW_ART_NEGATIVE' THEN
        SET outcome_condition =
                ' art_start_date between ? AND ?  and screening_result != ''Positive''  ';
    ELSEIF REPORT_TYPE = 'PREV_ART_POSITIVE' THEN
        SET outcome_condition =
                ' art_start_date < ? and follow_up_status in (''Alive'', ''Restart medication'') and screening_result = ''Positive'' ';
    ELSEIF REPORT_TYPE = 'PREV_ART_NEGATIVE' THEN
        SET outcome_condition =
                ' art_start_date < ? and follow_up_status in (''Alive'', ''Restart medication'')  and screening_result != ''Positive'' ';
    ELSEIF REPORT_TYPE = 'SCREEN_TYPE' THEN
        SET outcome_condition = ' 1=1 ';
    ELSEIF REPORT_TYPE = 'SPECIMEN_SENT' THEN
        SET outcome_condition = ' 1=1 ';
    ELSEIF REPORT_TYPE = 'DIAGNOSTIC_TEST' THEN
        SET outcome_condition = ' 1=1 ';
    ELSEIF REPORT_TYPE = 'POSITIVE_RESULT' THEN
        SET outcome_condition = ' 1=1 ';
    ELSEIF REPORT_TYPE = 'NUMERATOR_NEW' THEN
        SET outcome_condition =' art_start_date between ? AND ? and tb_treatment_start_date between ? AND ? ';
    ELSEIF REPORT_TYPE = 'NUMERATOR_TOTAL' THEN
        SET outcome_condition =' tb_treatment_start_date between ? AND ? ';
    ELSEIF REPORT_TYPE = 'NUMERATOR_PREV' THEN
        SET outcome_condition =' art_start_date < ? and follow_up_status in (''Alive'', ''Restart medication'') and tb_treatment_start_date between ? AND ? ';
    ELSE
        SET  outcome_condition = '1=1';
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
                         gene_xpert_result,
                         diagnostic_test as other_diagnostic_test,
                         tb_diagnostic_test_result           as         other_tb_diagnostic_result,
                         specimen_sent_to_lab,
                         tuberculosis_drug_treatment_start_d as tb_treatment_start_date,
                         date_active_tbrx_completed as tb_treatment_completed_date
                  FROM mamba_flat_encounter_follow_up follow_up
                               LEFT JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                    ON follow_up.encounter_id = follow_up_1.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                    ON follow_up.encounter_id = follow_up_2.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                         ON follow_up.encounter_id = follow_up_4.encounter_id
),
     tmp_latest_follow_up
         as (select client_id,
                    encounter_id,
                    follow_up_status,
                    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
             from FollowUp
             where follow_up_date <= ?),
     tmp_latest_screeing_follow_up as (SELECT client_id,
                                              follow_up_date                                                                                AS FollowupDate,
                                              encounter_id,
                                              follow_up_status,
                                              tb_screening_date,
                                              art_start_date,
                                              tb_screened,
                                              screening_result,
                                              lf_lam_result,
                                              gene_xpert_result,
                                              other_diagnostic_test,
                                              other_tb_diagnostic_result,
                                              specimen_sent_to_lab,
                                              tb_treatment_start_date,
                                              tb_treatment_completed_date,
                                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY tb_screening_date DESC, encounter_id DESC) AS row_num
                                       FROM FollowUp
                                       WHERE follow_up_status IS NOT NULL
                                         AND art_start_date IS NOT NULL
                                         AND tb_screening_date is not null
                                         AND tb_screened = ''Yes''
                                         and tb_screening_date between ? AND ?),
     latest_follow_up as (select * from tmp_latest_follow_up where row_num = 1),
     latest_screeing_follow_up as (select * from tmp_latest_screeing_follow_up where row_num = 1),
     tb_screening as (SELECT follow_up.*,
                             sex,
                             date_of_birth,
                             fine_age_group,
                             coarse_age_group,
                             mrn,
                             uan,
                             latest_follow_up.follow_up_status as latest_follow_up_staus
                      from latest_screeing_follow_up follow_up
                               inner join mamba_dim_client client on follow_up.client_id = client.client_id
                               left join latest_follow_up on follow_up.client_id = latest_follow_up.client_id) ';
    IF REPORT_TYPE = 'SCREEN_TYPE' THEN
        SET group_query = 'select SUM(CASE WHEN other_diagnostic_test = ''Chest X-Ray'' is not null THEN 1 ELSE 0 END) as `Chest X-Ray` ,
       SUM(CASE WHEN other_diagnostic_test != ''Chest X-Ray'' is null THEN 1 ELSE 0 END) AS ''Symptom Screen Only'',
       0 AS `Molecular WHO-Recommended Diagnostic Test(mWRD)`
 from tb_screening';
    ELSEIF REPORT_TYPE = 'SPECIMEN_SENT' THEN
        SET group_query = ' select  SUM(CASE WHEN other_diagnostic_test = ''Smear microscopy only'' is not null THEN 1 ELSE 0 END) as `Smear Microscopy Only`,
        0 AS `mWRD: Molecular WHO-Recommended Diagnostic PCR (with or without other testing)`,
        SUM(CASE WHEN other_diagnostic_test = ''Additional test other than Gene-Xpert'' is not null THEN 1 ELSE 0 END) as `Additional test other than mWRD`
from tb_screening where specimen_sent_to_lab=''Yes'' ';
    ELSEIF REPORT_TYPE = 'DIAGNOSTIC_TEST' THEN
        SET group_query = ' select  ''Number of ART patients who had a specimen sent for bacteriologic diagnosis of active TB disease'' AS `Name`,COUNT(*) AS `Value` from tb_screening where specimen_sent_to_lab=''Yes'' ';
    ELSEIF REPORT_TYPE='POSITIVE_RESULT' THEN
        SET group_query = ' select  ''Number of ART patients who had a positive result returned for bacteriologic diagnosis of active TB disease'' AS `Name`,COUNT(*) AS `Value` from tb_screening where specimen_sent_to_lab=''Yes'' and screening_result = ''Positive'' ';
    ELSEIF REPORT_TYPE='NUMERATOR_TOTAL' THEN
        SET group_query = CONCAT(' select COUNT(*) as `Subtotal` FROM tb_screening WHERE ',outcome_condition);
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
          FROM tb_screening WHERE ', outcome_condition, '
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
    IF REPORT_TYPE = 'PREV_ART_POSITIVE' OR REPORT_TYPE = 'PREV_ART_NEGATIVE' THEN
        EXECUTE stmt USING @end_date, @start_date , @end_date, @end_date;
    ELSEIF REPORT_TYPE = 'NEW_ART_POSITIVE' OR REPORT_TYPE = 'NEW_ART_NEGATIVE' OR REPORT_TYPE='NUMERATOR_TOTAL' THEN
        EXECUTE stmt USING @end_date, @start_date , @end_date, @start_date ,@end_date;
    ELSEIF REPORT_TYPE = 'NUMERATOR_PREV' THEN
        EXECUTE stmt USING @end_date, @start_date , @end_date, @end_date , @end_date, @end_date;
    ELSEIF REPORT_TYPE = 'NUMERATOR_NEW' THEN
        EXECUTE stmt USING @end_date, @start_date , @end_date, @start_date ,@end_date, @start_date ,@end_date;
    ELSE
        EXECUTE stmt USING @end_date, @start_date , @end_date;
    END IF;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;