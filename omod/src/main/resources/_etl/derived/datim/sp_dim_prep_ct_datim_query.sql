DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_prep_ct_datim_query;

CREATE PROCEDURE sp_dim_prep_ct_datim_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN IS_COURSE_AGE_GROUP BOOLEAN,
    IN REPORT_TYPE VARCHAR(100)
)
BEGIN

    DECLARE age_group_cols VARCHAR(5000);
    DECLARE prep_query VARCHAR(6278);
    DECLARE group_query TEXT;

    SET session group_concat_max_len = 20000;


    SET prep_query = CONCAT('WITH PreExposure AS (select screening.client_id,
                            screening.encounter_id,
                            screening.encounter_datetime,
                            screening.sex_worker,
                            screening.prep_started,
                            screening.type_of_client,
                            screening_1.treatment_start_date,
                            screening_1.do_you_have_an_hiv_positive_partner,
                            screening_1.antiretroviral_art_dispensed_dose_in_days as dose_dispensed,
                            follow_up.follow_up_status,
                            follow_up.pregnancy_status                            as f_pregnancy_status,
                            follow_up_1.prep_dose_end_date,
                            follow_up_1.followup_date_followup_1,
                            screening.screening_date,
                            final_hiv_test_result,
                            currently_breastfeeding_child
                     FROM mamba_flat_encounter_pre_exposure_scree screening
                              left join mamba_flat_encounter_pre_exposure_scree_1 screening_1
                                        on screening.encounter_id = screening_1.encounter_id
                              LEFT JOIN mamba_flat_encounter_pre_exposure_follo follow_up
                                        on screening.client_id = follow_up.client_id
                              LEFT JOIN mamba_flat_encounter_pre_exposure_follo_1 follow_up_1
                                        on follow_up.encounter_id = follow_up_1.encounter_id),
     tmp_latest_follow_up as (select prep_dose_end_date,
                                     prep_started,
                                     date_of_birth,
                                     sex,
                                     client.uan,
                                     client.mrn,
                                     (SELECT datim_agegroup
                                      from mamba_dim_agegroup
                                      where TIMESTAMPDIFF(YEAR, date_of_birth, ''',REPORT_END_DATE,''' ) = age) as fine_age_group,   -- Param 1 @end_date
                                     (SELECT normal_agegroup
                                      from mamba_dim_agegroup
                                      where TIMESTAMPDIFF(YEAR, date_of_birth, ''',REPORT_END_DATE,''') = age) as coarse_age_group, -- Param 2 @end_date
                                     do_you_have_an_hiv_positive_partner,
                                     sex_worker,
                                     treatment_start_date,
                                     follow_up_status,
                                     type_of_client,
                                     client.client_id,
                                     followup_date_followup_1,
                                     dose_dispensed,
                                     final_hiv_test_result,
                                     f_pregnancy_status,
                                     currently_breastfeeding_child,
                                     ROW_NUMBER() OVER (
                                         PARTITION BY PreExposure.client_id
                                         ORDER BY
                                             PreExposure.followup_date_followup_1 DESC,
                                             PreExposure.encounter_id DESC
                                         )                                               AS row_num
                              from PreExposure
                                       join mamba_dim_client client on PreExposure.client_id = client.client_id
                              where (followup_date_followup_1 <= ''',REPORT_END_DATE,'''
                                  or followup_date_followup_1 is null)),
     prep_new as (select *
                  from tmp_latest_follow_up
                  WHERE treatment_start_date BETWEEN ''',REPORT_START_DATE,''' AND ''',REPORT_END_DATE,'''
                    AND type_of_client = ''New client''
                    AND row_num = 1),
     prep as (select *
              from tmp_latest_follow_up
              where row_num = 1
                  and ((prep_dose_end_date >= ''',REPORT_END_DATE,''' or
                        DATE_ADD(treatment_start_date, INTERVAL dose_dispensed DAY) >=
                        ''',REPORT_END_DATE,''')
                        )
                 or (followup_date_followup_1 BETWEEN ''',REPORT_START_DATE,''' AND ''',REPORT_END_DATE,''' AND
                     (final_hiv_test_result = ''Positive'' OR follow_up_status = ''Restart medication'' OR
                      follow_up_status = ''Alive'')) -- Param 8 @end_date, Param 9 @end_date
                  and prep_started = ''Yes''
                  and client_id not in (select client_id from prep_new)) ');
    IF IS_COURSE_AGE_GROUP THEN
        SELECT GROUP_CONCAT(CONCAT('SUM(CASE WHEN coarse_age_group = ''', normal_agegroup,
                                   ''' THEN count ELSE 0 END) AS `',
                                   REPLACE(normal_agegroup, '`', '``'),
                                   '`')
               )
        INTO age_group_cols
        FROM (select normal_agegroup
              from mamba_dim_agegroup
              where mamba_dim_agegroup.datim_age_val > 4
              group by normal_agegroup) as order_query;
    ELSE
        SELECT GROUP_CONCAT(CONCAT('SUM(CASE WHEN fine_age_group = ''', datim_agegroup,
                                   ''' THEN count ELSE 0 END) AS `',
                                   REPLACE(datim_agegroup, '`', '``'),
                                   '`')
               )
        INTO age_group_cols
        FROM (select datim_agegroup
              from mamba_dim_agegroup
              where mamba_dim_agegroup.datim_age_val > 4
              group by datim_agegroup) as order_query;
    END IF;
    IF REPORT_TYPE = 'AGE_SEX' THEN
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
          FROM prep
          GROUP BY sex, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
        USING (sex)
        GROUP BY sex
        ');
    ELSEIF REPORT_TYPE = 'TOTAL' THEN
        SET group_query = 'SELECT COUNT(*) AS NUMERATOR FROM prep';
    ELSEIF REPORT_TYPE= 'DEBUG' THEN
        SET group_query = 'SELECT * FROM FROM prep';
    ELSEIF REPORT_TYPE = 'TEST_RESULT' THEN
        SET group_query = 'select ''Positive'' as `Name`, COUNT(*) as count from prep where final_hiv_test_result=''Positive''
        UNION ALL
        select ''Negative'' as `Name`, COUNT(*) from prep where final_hiv_test_result in (''Negative result'',''Negative'')
        UNION ALL
        select ''Other'' as `Name`, COUNT(*) from prep where final_hiv_test_result NOT IN (''Negative result'',''Positive'',''Negative'')';
    ELSEIF REPORT_TYPE = 'PREP_TYPE' THEN
        SET group_query = 'select ''Oral'' as `Name`, COUNT(*) as count from prep
        UNION ALL
        select ''Injectable'' as `Name`, 0
        UNION ALL
        select ''Other'' as `Name`, 0';
    ELSEIF REPORT_TYPE = 'PREGNANT_BF' THEN
        SET group_query = 'select ''Pregnant'' as `Name`, COUNT(*) as count from prep where f_pregnancy_status = ''Yes''
        UNION ALL
        select ''Breastfeeding'' as `Name`, COUNT(*) from prep where currently_breastfeeding_child = ''Yes''';
    ELSEIF REPORT_TYPE = 'FACILITY' THEN
        SET group_query = 'select ''Facility'' as `Name`, COUNT(*) as count from prep
        UNION ALL
        select ''Community (Associated with Facility)'' as `Name`, 0';
    END IF;
    SET @sql = CONCAT(prep_query, group_query);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END //

DELIMITER ;