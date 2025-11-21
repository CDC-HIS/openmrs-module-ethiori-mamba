DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_pmtct_hei_datim_query;

CREATE PROCEDURE sp_dim_pmtct_hei_datim_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN REPORT_TYPE VARCHAR(100)
)
BEGIN

    DECLARE hei_query text;
    DECLARE group_query TEXT;
    DECLARE outcome_condition VARCHAR(150);
    SET session group_concat_max_len = 30000;

    IF REPORT_TYPE = 'RESULT_COLLECTED' THEN
        SET outcome_condition = ' 1=1';
    ELSEIF REPORT_TYPE = 'ART_STARTED' THEN
        SET outcome_condition = ' 1=1';
    ELSE
        SET  outcome_condition = '1=1';
    END IF;


    SET hei_query = 'WITH tmp_hei_test as (select hiv_test.client_id,
                             hiv_test_date,
                             date_of_birth,
                             hiv_test_result,
                             TIMESTAMPDIFF(MONTH, date_of_birth, ?)                                                                              as age_in_months,
                             ROW_NUMBER() over (PARTITION BY hiv_test.client_id ORDER BY dna_pcr_sample_collection_date DESC, encounter_id DESC) as row_num,
                             (SELECT datim_agegroup
                              from mamba_dim_agegroup
                              where TIMESTAMPDIFF(YEAR, date_of_birth, ?) = age)                                                                 as fine_age_group,
                             (SELECT normal_agegroup
                              from mamba_dim_agegroup
                              where TIMESTAMPDIFF(YEAR, date_of_birth, ?) = age)                                                                 as coarse_age_group
                      from mamba_flat_encounter_hei_hiv_test hiv_test
                               join mamba_dim_client client on hiv_test.client_id = client.client_id
                      where test_round = ''Initial test''
                        and dna_pcr_sample_collection_date BETWEEN ? AND ?),
     hei_test as (select * from tmp_hei_test where row_num = 1 and age_in_months <= 12) ';
    IF REPORT_TYPE = 'TOTAL' THEN
        SET group_query = 'SELECT COUNT(*) AS NUMERATOR FROM hei_test';
    ELSEIF REPORT_TYPE = 'DEBUG' THEN
        SET group_query = 'SELECT * FROM hei_test';
    ELSE
    SET group_query = CONCAT('
        SELECT
          hiv_test_result,
          SUM(CASE WHEN age_in_months  <= 2 THEN count ELSE 0 END) AS ''0<=2 Months'',
        SUM(CASE WHEN age_in_months  > 2 THEN count ELSE 0 END) AS ''2-12 Months'',
        SUM(CASE WHEN count is not null THEN count ELSE 0 END) as Subtotal
        FROM (
          SELECT
            hiv_test_result,
           age_in_months,
            COUNT(*) AS count
          FROM hei_test WHERE ', outcome_condition, '
          GROUP BY hiv_test_result, age_in_months
        ) AS subquery
        RIGHT JOIN (SELECT ''Positive'' AS hiv_test_result UNION SELECT ''Negative result'') AS hiv_test_result
        USING (hiv_test_result)
        GROUP BY hiv_test_result
        ');
    END IF;
    SET @sql = CONCAT(hei_query, group_query);
    PREPARE stmt FROM @sql;
    SET @start_date = REPORT_START_DATE;
    SET @end_date = REPORT_END_DATE;
    EXECUTE stmt USING @end_date, @end_date, @end_date, @start_date, @end_date;
    DEALLOCATE PREPARE stmt;

END //

DELIMITER ;