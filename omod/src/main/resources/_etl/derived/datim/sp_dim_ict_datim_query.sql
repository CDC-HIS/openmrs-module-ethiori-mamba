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
    ELSEIF REPORT_TYPE = 'ELICITED' THEN
        SET filter_condition = ' elicited_date BETWEEN ? AND ? ';
    ELSEIF REPORT_TYPE = 'KNOWN_POSITIVE' THEN
        SET filter_condition =
                ' prior_hiv_test_result = ''Positive'' and elicited_date between ? AND ? AND hiv_test_result IS NULL ';
    ELSEIF REPORT_TYPE = 'DOCUMENTED_NEGATIVE' THEN
        SET filter_condition =
                ' prior_hiv_test_result = ''Negative result'' and hiv_test_result != ''Positive'' and age < 15 and elicited_date BETWEEN ? AND ? ';
    ELSEIF REPORT_TYPE = 'NEW_POSITIVE' THEN
        SET filter_condition =
                ' hiv_test_result = ''Positive'' and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN ? AND ? ';
    ELSEIF REPORT_TYPE = 'NEW_NEGATIVE' THEN
        SET filter_condition =
                ' hiv_test_result = ''Negative result'' and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN ? AND ? AND age > 14 ';
    ELSEIF REPORT_TYPE = 'ICT_TOTAL' THEN
        SET filter_condition = ' hiv_test_date BETWEEN ? AND ? and hiv_test_result is not null ';
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
    ELSEIF REPORT_TYPE = 'DOCUMENTED_NEGATIVE' THEN
        SELECT GROUP_CONCAT(CONCAT('SUM(CASE WHEN fine_age_group = ''', datim_agegroup,
                                   ''' THEN count ELSE 0 END) AS `',
                                   REPLACE(datim_agegroup, '`', '``'),
                                   '`')
               )
        INTO age_group_cols
        FROM (select datim_agegroup
              from mamba_dim_agegroup
              where datim_age_val between 2 and 4
              group by datim_agegroup) as order_query;
    ELSE
        SELECT GROUP_CONCAT(CONCAT('SUM(CASE WHEN fine_age_group = ''', datim_agegroup,
                                   ''' THEN count ELSE 0 END) AS `',
                                   REPLACE(datim_agegroup, '`', '``'),
                                   '`')
               )
        INTO age_group_cols
        FROM (select datim_agegroup from mamba_dim_agegroup group by datim_agegroup) as order_query;
    END IF;
    IF REPORT_TYPE = 'TESTING_OFFERED' OR REPORT_TYPE = 'TESTING_ACCEPTED' THEN
        SET source_cte = ' offer ';
    ELSE
        SET source_cte = ' contact_list ';
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
                           TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
                           client.sex,
                           offer.offered_date,
                           offered,
                           offer.yes,
                           offer.no,
                           accepted,
                           accepted_date,
                           number_of_male_contacts_above_the_age_of_15,
                           number_of_male_contacts_below_the_age_of_15,
                           number_of_female_contacts_above_the_age_of_15,
                           number_of_female_contacts_below_the_age_of_15,
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
       date_of_case_closure,
       encounter.encounter_id,
       encounter.uuid,
       coalesce(contact_birthdate,index_contact.date_of_birth) as birthdate,

       CASE
           WHEN COALESCE(index_contact.sex, contact_1.respondent_gender) IN
                (''F'', ''Female gender'') THEN ''Female''
           WHEN COALESCE(index_contact.sex, contact_1.respondent_gender) IN
                (''M'', ''Male gender'') THEN ''Male''
           ELSE COALESCE(index_contact.sex, contact_1.respondent_gender)
           END AS sex,

       (SELECT datim_agegroup
        from mamba_dim_agegroup
        where TIMESTAMPDIFF(YEAR, coalesce(contact_birthdate,index_contact.date_of_birth), ?) = age) as fine_age_group,
       (SELECT normal_agegroup
        from mamba_dim_agegroup
        where TIMESTAMPDIFF(YEAR, coalesce(contact_birthdate,index_contact.date_of_birth), ?) = age) as coarse_age_group,
       TIMESTAMPDIFF(YEAR, coalesce(contact_birthdate,index_contact.date_of_birth), ?) as age
from mamba_flat_encounter_index_contact_followup contact
         left join mamba_flat_encounter_index_contact_followup_1 contact_1
                   on contact.encounter_id = contact_1.encounter_id
         left join mamba_flat_encounter_ict_general g on contact.client_id = g.client_id
         left join mamba_dim_client index_client on contact.client_id = index_client.client_id
         left join mamba_dim_encounter encounter on contact.encounter_id = encounter.encounter_id
         left join mamba_dim_person_attribute attribute on encounter.uuid = attribute.value
         left join mamba_dim_client index_contact on attribute.person_id = index_contact.client_id) ';

    IF REPORT_TYPE = 'ICT_TOTAL' THEN
        SET group_query = CONCAT('SELECT COUNT(*) as Numerator FROM contact_list WHERE ', filter_condition);
    ELSEIF REPORT_TYPE = 'ELICITED' THEN
        SET IS_COURSE_AGE_GROUP = 1;
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
              FROM ', source_cte, ' WHERE ', filter_condition, ' GROUP BY sex, ',
                                 IF(IS_COURSE_AGE_GROUP, 'coarse_age_group', 'fine_age_group'), '
            ) AS subquery
            RIGHT JOIN (SELECT ''Female'' AS sex UNION SELECT ''Male'') AS genders
            USING (sex)
            GROUP BY sex
            ');
    END IF;
    SET @sql = CONCAT(ict_query, group_query);
    PREPARE stmt FROM @sql;
    SET @start_date = REPORT_START_DATE;
    SET @end_date = REPORT_END_DATE;
    IF REPORT_TYPE = 'TESTING_OFFERED' OR REPORT_TYPE = 'TESTING_ACCEPTED' THEN
        EXECUTE stmt USING @end_date, @end_date, @end_date, @start_date , @end_date, @end_date, @end_date, @end_date;
    ELSEIF REPORT_TYPE = 'ELICITED' THEN
        EXECUTE stmt USING @end_date, @end_date, @end_date, @start_date , @end_date, @end_date, @end_date, @end_date, @start_date , @end_date;
    ELSE
        EXECUTE stmt USING @end_date, @end_date, @end_date, @start_date , @end_date, @end_date, @end_date, @end_date, @start_date, @end_date;
    END IF;

END //
DELIMITER ;