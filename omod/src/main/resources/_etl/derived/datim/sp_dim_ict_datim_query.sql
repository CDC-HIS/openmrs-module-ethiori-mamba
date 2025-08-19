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
    DECLARE source_cte VARCHAR(100);
    SET session group_concat_max_len = 20000;


    IF REPORT_TYPE = 'TESTING_OFFERED' THEN
        SET filter_condition = ' offer.offered_date is not null ';
    ELSEIF REPORT_TYPE = 'TESTING_ACCEPTED' THEN
        SET filter_condition = ' accepted = ''Yes'' ';
    ELSEIF REPORT_TYPE = 'KNOWN_POSITIVE' THEN
        SET filter_condition = ' prior_hiv_test_result = ''Positive'' elicited_date between ? AND ? ';
    ELSEIF REPORT_TYPE = 'NEW_POSITIVE' THEN
        SET filter_condition = ' prior_hiv_test_result != ''Positive'' and hiv_test_result = ''Positive'' and hiv_test_date BETWEEN ? AND ? ';
    ELSEIF REPORT_TYPE = 'NEW_NEGATIVE' THEN
        SET filter_condition = ' prior_hiv_test_result != ''Positive'' and hiv_test_result = ''Negative result'' and hiv_test_date BETWEEN ? AND ? ';
    ELSE
        SET filter_condition = ' 1=1 ';
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
    IF REPORT_TYPE = 'TESTING_OFFERED' THEN
        SET source_cte = 'offer';
    END IF;
    SET ict_query = ' With offer_list as (select client.client_id,
                           client.mrn,
                           client.uan,
                           (SELECT datim_agegroup
                                      from mamba_dim_agegroup
                                      where TIMESTAMPDIFF(YEAR, date_of_birth, ?) = age) as fine_age_group,
                                     (SELECT normal_agegroup
                                      from mamba_dim_agegroup
                                      where TIMESTAMPDIFF(YEAR, date_of_birth, ?) = age) as coarse_age_group,
                           client.sex,
                           offer.offered_date,
                           offered,
                           offer.yes,
                           offer.no,
                           accepted,
                           accepted_date,
                           ROW_NUMBER() over (PARTITION BY client.client_id ORDER BY offered_date DESC ) as row_num
                    from mamba_flat_encounter_ict_general ict_general
                             join mamba_flat_encounter_ict_offer offer on ict_general.client_id = offer.client_id
                             join mamba_dim_client client on ict_general.client_id = client.client_id
                    where offered_date BETWEEN ? AND ?),
     offer as (select * from offer_list where row_num = 1),
          contact_list as (select contact.client_id,
                             contact.elicited_date,
                             contact.hiv_test_date,
                             contact.hiv_test_result,
                             prior_hiv_test_result,
                             prior_test_date_estimated,
                      from
                          mamba_flat_encounter_index_contact_followup contact
                              left join mamba_flat_encounter_index_contact_followup_1 contact_1
                                        on contact.encounter_id = contact_1.encounter_id) ';

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
          FROM ', source_cte, ' WHERE ', filter_condition, ' GROUP BY sex, ',
                             IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
        ) AS subquery
        RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
        USING (sex)
        GROUP BY sex
        ');
    SET @sql = CONCAT(ict_query, group_query);
    PREPARE stmt FROM @sql;
    SET @start_date = REPORT_START_DATE;
    SET @end_date = REPORT_END_DATE;
    EXECUTE stmt USING @end_date, @end_date, @start_date , @end_date;

END //
DELIMITER ;