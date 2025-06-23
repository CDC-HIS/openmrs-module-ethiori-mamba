DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_cxca_scrn_datim_query;

CREATE PROCEDURE sp_dim_cxca_scrn_datim_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN IS_COURSE_AGE_GROUP BOOLEAN,
    IN REPORT_TYPE VARCHAR(100)
)
BEGIN

    DECLARE age_group_cols TEXT;
    DECLARE cxca_scrn_query TEXT;
    DECLARE cxca_visit_condition TEXT;
    DECLARE group_query TEXT;

    SET session group_concat_max_len = 20000;

    IF REPORT_TYPE = 'FIRST_TIME_SCREENING' THEN
        SET cxca_visit_condition =
                ' visit_type = ''Initial visit'' ';
    ELSEIF REPORT_TYPE = 'RE_SCREENING' THEN
        SET cxca_visit_condition =
                ' visit_type = ''Re-screening for cervical cancer after 5 year'' OR visit_type = ''Re-screening for cervical cancer after 3 year'' OR visit_type = ''Re-screening for cervical cancer after 2 year'' OR visit_type = ''Re-screening for cervical cancer after 1 year'' ';
    ELSEIF REPORT_TYPE = 'POST_TREATMENT' THEN
        SET cxca_visit_condition =
                ' visit_type = ''Post-treatment follow-up other'' OR visit_type = ''Post-treatment follow-up at 1 year'' OR visit_type =''Post-treatment follow-up to 6 weeks'' ';
    ELSE
        SET cxca_visit_condition = '1=1';
    END IF;

    SET cxca_scrn_query = 'WITH FollowUp as (select follow_up.encounter_id,
                         follow_up.client_id,
                         cervical_cancer_screening_status          as cx_ca_screening_status,
                         cervical_cancer_screening_method_strategy as screening_method,
                         via_screening_result,
                         hpv_dna_screening_result,
                         follow_up_date_followup_                  as follow_up_date,
                         date_hpv_test_was_done                    as hpv_date,
                         date_visual_inspection_of_the_cervi       as via_date,
                         cytology_sample_collection_date           as cytology_date,
                         follow_up_status,
                         treatment_end_date,
                         antiretroviral_art_dispensed_dose_i       as dose_days,
                         art_antiretroviral_start_date             as art_start_date,
                         cytology_result,
                         purpose_for_visit_cervical_screening      as visit_type,
                         hpv_dna_result_received_date              as hpv_received_date,
                         date_cytology_result_received             as cytology_received_date
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
     tmp_latest_follow_up AS (SELECT client_id,
                                     follow_up_date,
                                     encounter_id,
                                     follow_up_status,
                                     treatment_end_date,
                                     dose_days,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND art_start_date IS NOT NULL
         --  AND follow_up_date <= ?
     ),

     currently_on_art AS (select *
                          from tmp_latest_follow_up
                          where row_num = 1
                            AND follow_up_status in (''Alive'', ''Restart medication'')),
     tmp_cx_screened as (select FollowUp.*,
                                ROW_NUMBER() over (PARTITION BY FollowUp.client_id ORDER BY via_date DESC,hpv_received_date DESC,cytology_received_date DESC , FollowUp.encounter_id DESC) as row_num
                         from FollowUp
                                  join currently_on_art on FollowUp.client_id = currently_on_art.client_id
                         where cx_ca_screening_status = ''Cervical cancer screening performed''
                           and ((via_date BETWEEN ? AND ?
                                    -- AND screening_method != ''Human Papillomavirus test''
                               ) OR
                                (hpv_received_date BETWEEN ? AND ?) OR (cytology_received_date BETWEEN ? AND ?))),
     cx_screened as (select tmp_cx_screened.*,
                            CASE


                                #  WHEN screening_method=''Human Papillomavirus test'' and hpv_dna_screening_result =''Unknown'' then ''Cervical Cancer screen: Suspected Cancer''

                                WHEN screening_method = ''Visual Inspection of the Cervix with Acetic Acid (VIA)'' and
                                     via_screening_result = ''VIA negative'' then ''Cervical Cancer screen: Negative''
                                WHEN screening_method = ''Visual Inspection of the Cervix with Acetic Acid (VIA)'' and
                                     (via_screening_result = ''VIA positive: eligible for cryo/thermo-coagulation'' or
                                      via_screening_result = ''VIA positive: eligible for cryo/thermo-coagula'' or
                                      via_screening_result = ''VIA positive: non-eligible for cryo/thermo-coagulation'' or
                                      via_screening_result = ''VIA positive: non-eligible for cryo/thermo-coagula'')
                                    then ''Cervical Cancer screen: Positive''
                                WHEN screening_method = ''Visual Inspection of the Cervix with Acetic Acid (VIA)'' and
                                     via_screening_result = ''Suspicious for cervical cancer'' or via_screening_result = ''suspected cervical cancer''
                                    then ''Cervical Cancer screen: Suspected Cancer''

                                WHEN screening_method = ''Human Papillomavirus test'' and
                                     hpv_dna_screening_result = ''Negative result''
                                    then ''Cervical Cancer screen: Negative''
                                WHEN screening_method = ''Human Papillomavirus test'' and
                                     hpv_dna_screening_result = ''Positive''
                                    and
                                     (via_screening_result = ''VIA positive: eligible for cryo/thermo-coagulation'' or
                                      via_screening_result = ''VIA positive: eligible for cryo/thermo-coagula'' or
                                      via_screening_result = ''VIA positive: non-eligible for cryo/thermo-coagulation'')
                                    then ''Cervical Cancer screen: Positive''
                                WHEN screening_method = ''Human Papillomavirus test'' and
                                     hpv_dna_screening_result = ''Positive''
                                    and via_screening_result = ''VIA negative'' then ''Cervical Cancer screen: Negative''
                                WHEN screening_method = ''Cytology'' and
                                     (cytology_result = ''Negative'' OR cytology_result = ''ASCUS'')
                                    THEN ''Cervical Cancer screen: Negative''
                                WHEN screening_method = ''Cytology'' and cytology_result = ''> Ascus''
                                    THEN ''Cervical Cancer screen: Positive''
                                END                                             AS screening_result,
                            client.uan,
                            client.mrn,
                            client.sex,
                            client.date_of_birth,
                            (SELECT datim_agegroup
                             from mamba_dim_agegroup
                             where TIMESTAMPDIFF(YEAR, date_of_birth, ?) = age) as fine_age_group,
                            (SELECT normal_agegroup
                             from mamba_dim_agegroup
                             where TIMESTAMPDIFF(YEAR, date_of_birth, ?) = age) as coarse_age_group
                     from tmp_cx_screened
                              join mamba_dim_client client
                                   on tmp_cx_screened.client_id = client.client_id
                     where row_num = 1
                       and TIMESTAMPDIFF(YEAR, date_of_birth, follow_up_date) >= 15
                       and not (screening_method = ''Human Papillomavirus test'' and
                                hpv_dna_screening_result = ''Positive'' and via_screening_result is null)
     ) ';
    IF IS_COURSE_AGE_GROUP THEN
        SELECT GROUP_CONCAT(CONCAT('SUM(CASE WHEN coarse_age_group = ''', normal_agegroup,
                                   ''' THEN count ELSE 0 END) AS `',
                                   REPLACE(normal_agegroup, '`', '``'),
                                   '`')
               )
        INTO age_group_cols
        FROM (select normal_agegroup
              from mamba_dim_agegroup
              where mamba_dim_agegroup.datim_age_val > 4 and mamba_dim_agegroup.datim_age_val <= 12
              group by normal_agegroup) as order_query;
    ELSE
        SELECT GROUP_CONCAT(CONCAT('SUM(CASE WHEN fine_age_group ',
                                   CASE
                                       -- If the datim_agegroup is '50-54' or '55-59',
                                       -- aggregate them under the '50+' column.
                                       WHEN datim_agegroup IN ('50-54', '55-59','60-64','65+') THEN
                                           'IN (''50-54'', ''55-59'', ''60-64'', ''65+'')' -- Condition for the aggregated '50+' column
                                       ELSE
                                           CONCAT(' = ','''', datim_agegroup,'''') -- Condition for all other individual age groups
                                       END,
                                   ' THEN count ELSE 0 END) AS `',
                                   REPLACE(
                                           CASE
                                               WHEN datim_agegroup IN ('50-54', '55-59','60-64','65+') THEN '50+'
                                               ELSE datim_agegroup
                                               END,
                                           '`', '``'),
                                   '`')
               )
        INTO age_group_cols
        FROM (select datim_agegroup
              from mamba_dim_agegroup
              where mamba_dim_agegroup.datim_age_val > 4 and mamba_dim_agegroup.datim_age_val <= 12
              group by datim_agegroup) as order_query;
    END IF;
    IF REPORT_TYPE = 'TOTAL' THEN
        SET group_query = 'SELECT COUNT(*) AS NUMERATOR FROM cx_screened';
    ELSEIF REPORT_TYPE = 'DEBUG' THEN
        SET group_query = 'SELECT * FROM cx_screened';
    ELSE
        SET group_query = CONCAT('
        SELECT
        CASE
        WHEN screening_result=''Cervical Cancer screen: Negative'' THEN ''Cervical Cancer screen: Negative''
        WHEN screening_result=''Cervical Cancer screen: Positive'' THEN ''Cervical Cancer screen: Positive''
        WHEN screening_result=''Cervical Cancer screen: Suspected Cancer'' THEN ''Cervical Cancer screen: Suspected Cancer'' END AS screening_result,
        SUM(CASE WHEN ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), ' is null AND count is not null THEN count ELSE 0 END) AS ''Unknown Age'',
          ', age_group_cols, ' ,
        SUM(CASE WHEN count is not null THEN count ELSE 0 END) as Subtotal
        FROM (
          SELECT
            screening_result,
            ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), ',
            COUNT(*) AS count
          FROM cx_screened WHERE ', cxca_visit_condition, '
          GROUP BY screening_result, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Cervical Cancer screen: Negative'' AS screening_result
                     UNION SELECT ''Cervical Cancer screen: Positive''
                     UNION SELECT ''Cervical Cancer screen: Suspected Cancer'') AS screening_result
        USING (screening_result)
        GROUP BY screening_result
        ');
    END IF;
    SET @sql = CONCAT(cxca_scrn_query, group_query);
    PREPARE stmt FROM @sql;
    SET @start_date = REPORT_START_DATE;
    SET @end_date = REPORT_END_DATE;
    EXECUTE stmt USING  @start_date, @end_date , @start_date, @end_date , @start_date, @end_date, @end_date, @end_date;

END //

DELIMITER ;