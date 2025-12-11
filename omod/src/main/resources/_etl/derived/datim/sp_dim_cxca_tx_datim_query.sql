DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_cxca_tx_datim_query;

CREATE PROCEDURE sp_dim_cxca_tx_datim_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN IS_COURSE_AGE_GROUP BOOLEAN,
    IN REPORT_TYPE VARCHAR(100)

)
BEGIN

    DECLARE age_group_cols VARCHAR(5000);
    DECLARE cxca_tx_query text;
    DECLARE cxca_visit_condition VARCHAR(1000);
    DECLARE group_query TEXT;

    SET session group_concat_max_len = 20000;

    IF REPORT_TYPE = 'FIRST_TIME_SCREENING' THEN
        SET cxca_visit_condition =
                ' visit_type = ''Initial visit'' ';
    ELSEIF REPORT_TYPE = 'RE_SCREENING' THEN
        SET cxca_visit_condition =
                ' visit_type = ''Re-screening for cervical cancer after 5 year'' OR visit_type = ''Re-screening for cervical cancer after 3 year'' OR visit_type = ''Re-screening for cervical cancer after 1 year'' ';
    ELSEIF REPORT_TYPE = 'POST_TREATMENT' THEN
        SET cxca_visit_condition =
                ' visit_type = ''Post-treatment follow-up other'' OR visit_type = ''Post-treatment follow-up at 1 year'' OR visit_type =''Post-treatment follow-up to 6 weeks'' ';
    ELSE
        SET  cxca_visit_condition = '1=1';
    END IF;

    SET cxca_tx_query = CONCAT('WITH FollowUp as (select follow_up.encounter_id,
                         follow_up.client_id,
                         cervical_cancer_screening_status          as cx_ca_screening_status,
                         cervical_cancer_screening_method_strategy as screening_type,
                         via_screening_result,
                         hpv_dna_screening_result,
                         follow_up_date_followup_                  as follow_up_date,
                         date_hpv_test_was_done                    as hpv_date,
                         date_visual_inspection_of_the_cervi       as via_date,
                         treatment_of_precancerous_lesions_of_the_cervix as treatment,
                         treatment_start_date,
                         art_antiretroviral_start_date as art_start_date,
                         purpose_for_visit_cervical_screening as visit_type
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
     tmp_cx_rx as (select *,
                          ROW_NUMBER() over (PARTITION BY client_id ORDER BY treatment_start_date DESC, encounter_id DESC) as row_num
                   from FollowUp
                   where treatment_start_date BETWEEN ''',REPORT_START_DATE,''' AND ''',REPORT_END_DATE,''' and art_start_date is not null),

     cx_rx as (select tmp_cx_rx.*,
                      client.date_of_birth,
                      client.sex,
                      (SELECT datim_agegroup from mamba_dim_agegroup where TIMESTAMPDIFF(YEAR,date_of_birth, ''',REPORT_END_DATE,''' )=age) as fine_age_group,
                      (SELECT normal_agegroup from mamba_dim_agegroup where TIMESTAMPDIFF(YEAR,date_of_birth, ''',REPORT_END_DATE,''' )=age) as coarse_age_group
               from tmp_cx_rx
                        left join mamba_dim_client client on tmp_cx_rx.client_id = client.client_id
               where row_num = 1 and current_age >= 15 ) ');
    IF IS_COURSE_AGE_GROUP THEN
        SELECT GROUP_CONCAT(CONCAT('SUM(CASE WHEN coarse_age_group = ''', normal_agegroup,
                                   ''' THEN count ELSE 0 END) AS `',
                                   REPLACE(normal_agegroup, '`', '``'),
                                   '`')
               )
        INTO age_group_cols
        FROM (select normal_agegroup from mamba_dim_agegroup where mamba_dim_agegroup.datim_age_val > 4 group by normal_agegroup) as order_query;
    ELSE
        SELECT GROUP_CONCAT(CONCAT('SUM(CASE WHEN fine_age_group = ''', datim_agegroup,
                                   ''' THEN count ELSE 0 END) AS `',
                                   REPLACE(datim_agegroup, '`', '``'),
                                   '`')
               )
        INTO age_group_cols
        FROM (select datim_agegroup from mamba_dim_agegroup where mamba_dim_agegroup.datim_age_val > 4 group by datim_agegroup) as order_query;
    END IF;
    IF REPORT_TYPE = 'TOTAL' THEN
        SET group_query = 'SELECT COUNT(*) AS NUMERATOR FROM cx_rx';
    ELSEIF REPORT_TYPE = 'DEBUG' THEN
        SET group_query = 'SELECT * FROM cx_rx';
    ELSE
    SET group_query = CONCAT('
        SELECT
        CASE
        WHEN treatment=''Cryosurgery of lesion of cervix'' THEN ''Cryotherapy''
        WHEN treatment=''Loop electrosurgical excision procedure of cervix'' THEN ''LEEP''
        WHEN treatment=''Thermocauterization of cervix'' THEN ''Thermocoagulation'' END AS treatment,
          SUM(CASE WHEN ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), ' is null AND count is not null THEN count ELSE 0 END) AS ''Unknown Age'',
          ', age_group_cols, ' ,
        SUM(CASE WHEN count is not null THEN count ELSE 0 END) as Subtotal
        FROM (
          SELECT
            treatment,
            ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), ',
            COUNT(*) AS count
          FROM cx_rx WHERE ', cxca_visit_condition, '
          GROUP BY treatment, ', IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Cryosurgery of lesion of cervix'' AS treatment
                     UNION SELECT ''Loop electrosurgical excision procedure of cervix''
                     UNION SELECT ''Thermocauterization of cervix'') AS treatment
        USING (treatment)
        GROUP BY treatment
        ');
    END IF;
    SET @sql = CONCAT(cxca_tx_query, group_query);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;

END //

DELIMITER ;