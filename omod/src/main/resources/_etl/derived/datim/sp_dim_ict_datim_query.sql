DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_ict_datim_query;

CREATE PROCEDURE sp_dim_ict_datim_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN IS_COURSE_AGE_GROUP BOOLEAN
)
BEGIN
    DECLARE age_group_cols VARCHAR(5000);
    DECLARE ict_query VARCHAR(6000);
    DECLARE group_query TEXT;
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

    SET ict_query = ' WITH ICT as (select *
                 from mamba_flat_encounter_ict_offer) ';



    END  //

DELIMITER ;