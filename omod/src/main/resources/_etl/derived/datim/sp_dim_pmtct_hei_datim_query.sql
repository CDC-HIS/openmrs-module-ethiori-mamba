DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_pmtct_hei_datim_query;

CREATE PROCEDURE sp_dim_pmtct_hei_datim_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN REPORT_TYPE VARCHAR(100)
)
BEGIN
    -- DSD: PMTCT_HEI - HEI Positivity Report
    -- Numerator: Infants who had a first virologic HIV test (sample collected) by 12 months
    -- Disaggregated by: Result (Positive, Negative) and Age (0<=2 months, 2-12 months)

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
            '        dna_pcr_sample_collection_date, ',
            '        TIMESTAMPDIFF(MONTH, date_of_birth, dna_pcr_sample_collection_date) as age_in_months, ',
            '        ROW_NUMBER() OVER (PARTITION BY hiv_test.client_id ORDER BY dna_pcr_sample_collection_date DESC, encounter_id DESC) as row_num ',
            '    FROM mamba_flat_encounter_hei_hiv_test hiv_test ',
            '    JOIN mamba_dim_client client ON hiv_test.client_id = client.client_id ',
            '    WHERE test_type = ''HIV DNA polymerase chain reaction, dried blood spot (DBS)'' ',
            '      AND (test_round = ''Initial test'' OR hiv_test_result = ''Positive'') ',
            '      AND dna_pcr_sample_collection_date BETWEEN ''', REPORT_START_DATE, ''' AND ''', REPORT_END_DATE, ''' ',
            '), ',
            'hei_test AS ( ',
            '    SELECT *, ',
            '           CASE WHEN hiv_test_result = ''Positive'' THEN ''Positive'' ELSE ''Negative'' END as result_category, ',
            '           CASE WHEN age_in_months <= 2 THEN ''0<=2 Months'' ELSE ''2-12 Months'' END as age_category ',
            '    FROM tmp_hei_test ',
            '    WHERE row_num = 1 ',
            '      AND age_in_months <= 12 ', -- Ensure age filter applies to the calculated sample date age
            '), ',
            'categories AS ( ',
            '    SELECT ''Positive'' as result_category, 1 as sort_order ',
            '    UNION ALL ',
            '    SELECT ''Negative'', 2 ',
            ') '
                     );

    IF REPORT_TYPE = 'NUMERATOR' THEN
        SET final_query = CONCAT(base_query,
                                 'SELECT COALESCE(COUNT(*), 0) AS Numerator FROM hei_test'
                          );

    ELSEIF REPORT_TYPE = 'DISAGGREGATED' THEN
        SET final_query = CONCAT(base_query,
                                 'SELECT ',
                                 '    c.result_category, ',
                                 '    COALESCE(SUM(CASE WHEN h.age_category = ''0<=2 Months'' THEN 1 ELSE 0 END), 0) AS `0<=2 Months`, ',
                                 '    COALESCE(SUM(CASE WHEN h.age_category = ''2-12 Months'' THEN 1 ELSE 0 END), 0) AS `2-12 Months` ',
                                 'FROM categories c ',
                                 'LEFT JOIN hei_test h ON c.result_category = h.result_category ',
                                 'GROUP BY c.result_category, c.sort_order ',
                                 'ORDER BY c.sort_order'
                          );

    ELSEIF REPORT_TYPE = 'TOTAL' THEN
        SET final_query = CONCAT(base_query,
                                 'SELECT ',
                                 '    (SELECT COALESCE(COUNT(*), 0) FROM hei_test) AS Numerator, ',
                                 '    COALESCE(SUM(CASE WHEN result_category = ''Positive'' AND age_category = ''0<=2 Months'' THEN 1 ELSE 0 END), 0) AS `Positive_0_2_Months`, ',
                                 '    COALESCE(SUM(CASE WHEN result_category = ''Positive'' AND age_category = ''2-12 Months'' THEN 1 ELSE 0 END), 0) AS `Positive_2_12_Months`, ',
                                 '    COALESCE(SUM(CASE WHEN result_category = ''Negative'' AND age_category = ''0<=2 Months'' THEN 1 ELSE 0 END), 0) AS `Negative_0_2_Months`, ',
                                 '    COALESCE(SUM(CASE WHEN result_category = ''Negative'' AND age_category = ''2-12 Months'' THEN 1 ELSE 0 END), 0) AS `Negative_2_12_Months` ',
                                 'FROM hei_test'
                          );

    ELSE
        -- DEBUG mode
        SET final_query = CONCAT(base_query,
                                 'SELECT client_id AS Client, result_category AS Result, age_category AS Age_Category, age_in_months AS Age_Months, dna_pcr_sample_collection_date FROM hei_test'
                          );
    END IF;

    SET @sql = final_query;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END //

DELIMITER ;