DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_pmtct_eid_datim_query;

CREATE PROCEDURE sp_dim_pmtct_eid_datim_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN REPORT_TYPE VARCHAR(100)
)
BEGIN
    -- DSD: PMTCT_EID - Early Infant Diagnosis Report
    -- Numerator: HIV-exposed infants who had a virologic HIV test by 12 months during reporting period
    -- Disaggregated by: infant age at test (0<=2 months, 2-12 months) and test sequence (First test, Second test or more)

    DECLARE base_query TEXT;
    DECLARE final_query TEXT;

    SET session group_concat_max_len = 30000;

    SET base_query = CONCAT(
            'WITH tmp_hei_test AS ( ',
            '    SELECT ',
            '        hiv_test.client_id, ',
            '        hiv_test_date, ',
            '        date_of_birth, ',
            '        hiv_test_result, ',
            '        test_round, ',
            '        dna_pcr_sample_collection_date, ',
            '        TIMESTAMPDIFF(MONTH, date_of_birth, dna_pcr_sample_collection_date) as age_in_months, ',
            '        ROW_NUMBER() OVER (PARTITION BY hiv_test.client_id ORDER BY dna_pcr_sample_collection_date ASC, encounter_id ASC) as test_sequence, ',
            '        ROW_NUMBER() OVER (PARTITION BY hiv_test.client_id ORDER BY dna_pcr_sample_collection_date DESC, encounter_id DESC) as row_num ',
            '    FROM mamba_flat_encounter_hei_hiv_test hiv_test ',
            '    JOIN mamba_dim_client client ON hiv_test.client_id = client.client_id ',
            '    WHERE dna_pcr_sample_collection_date BETWEEN ''', REPORT_START_DATE, ''' AND ''', REPORT_END_DATE, ''' ',
            '), ',
            'hei_test AS ( ',
            '    SELECT *, ',
            '           CASE WHEN test_sequence = 1 THEN ''First Test'' ELSE ''Second Test or More'' END as test_category, ',
            '           CASE WHEN age_in_months <= 2 THEN ''0<=2 Months'' ELSE ''2-12 Months'' END as age_category ',
            '    FROM tmp_hei_test ',
            '    WHERE age_in_months <= 12 ',
            '      AND row_num = 1 ',
            '), ',
            'categories AS ( ',
            '    SELECT ''First Test'' as test_category, 1 as sort_order ',
            '    UNION ALL ',
            '    SELECT ''Second Test or More'', 2 ',
            ') '
                     );

    IF REPORT_TYPE = 'NUMERATOR' THEN
        SET final_query = CONCAT(base_query,
                                 'SELECT COALESCE(COUNT(*), 0) AS Numerator FROM hei_test'
                          );

    ELSEIF REPORT_TYPE = 'DISAGGREGATED' THEN
        SET final_query = CONCAT(base_query,
                                 'SELECT ',
                                 '    c.test_category, ',
                                 '    COALESCE(SUM(CASE WHEN h.age_category = ''0<=2 Months'' THEN 1 ELSE 0 END), 0) AS `0<=2 Months`, ',
                                 '    COALESCE(SUM(CASE WHEN h.age_category = ''2-12 Months'' THEN 1 ELSE 0 END), 0) AS `2-12 Months` ',
                                 'FROM categories c ',
                                 'LEFT JOIN hei_test h ON c.test_category = h.test_category ',
                                 'GROUP BY c.test_category, c.sort_order ',
                                 'ORDER BY c.sort_order'
                          );

    ELSEIF REPORT_TYPE = 'TOTAL' THEN
        SET final_query = CONCAT(base_query,
                                 'SELECT ',
                                 '    (SELECT COALESCE(COUNT(*), 0) FROM hei_test) AS Numerator, ',
                                 '    COALESCE(SUM(CASE WHEN test_category = ''First Test'' AND age_category = ''0<=2 Months'' THEN 1 ELSE 0 END), 0) AS `First_Test_0_2_Months`, ',
                                 '    COALESCE(SUM(CASE WHEN test_category = ''First Test'' AND age_category = ''2-12 Months'' THEN 1 ELSE 0 END), 0) AS `First_Test_2_12_Months`, ',
                                 '    COALESCE(SUM(CASE WHEN test_category = ''Second Test or More'' AND age_category = ''0<=2 Months'' THEN 1 ELSE 0 END), 0) AS `Second_Test_0_2_Months`, ',
                                 '    COALESCE(SUM(CASE WHEN test_category = ''Second Test or More'' AND age_category = ''2-12 Months'' THEN 1 ELSE 0 END), 0) AS `Second_Test_2_12_Months` ',
                                 'FROM hei_test'
                          );

    ELSE
        -- DEBUG mode
        SET final_query = CONCAT(base_query,
                                 'SELECT client_id AS Client, test_category AS Test_Category, age_category AS Age_Category, age_in_months AS Age_Months, dna_pcr_sample_collection_date FROM hei_test'
                          );
    END IF;

    SET @sql = final_query;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END //

DELIMITER ;