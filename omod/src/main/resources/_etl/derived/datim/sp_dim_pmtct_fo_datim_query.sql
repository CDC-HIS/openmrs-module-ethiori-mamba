DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_pmtct_fo_datim_query;

CREATE PROCEDURE sp_dim_pmtct_fo_datim_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN REPORT_TYPE VARCHAR(100)
)
BEGIN
    -- DSD: PMTCT_FO - Final Outcome Report
    -- Denominator: HIV-exposed infants born 24 months prior to reporting period and registered in birth cohort
    -- Numerator: HIV-exposed infants with documented outcome by 18 months of age
    -- Disaggregated by: HIV-infected, HIV-uninfected, HIV-final status unknown, Died without status known

    DECLARE base_query TEXT;
    DECLARE final_query TEXT;
    
    -- Build base CTE query
    SET base_query = CONCAT(
        'WITH tmp_birth_cohort AS ( ',
        '    SELECT e.client_id, c.date_of_birth, e.encounter_datetime as enrollment_date, ',
        '           ROW_NUMBER() OVER (PARTITION BY e.client_id ORDER BY e.encounter_datetime DESC, e.encounter_id DESC) as row_num ',
        '    FROM mamba_flat_encounter_hei_enrollment e ',
        '    JOIN mamba_dim_client c ON e.client_id = c.client_id ',
        '    WHERE c.date_of_birth BETWEEN DATE_SUB(''', REPORT_START_DATE, ''', INTERVAL 24 MONTH) ',
        '          AND DATE_SUB(''', REPORT_END_DATE, ''', INTERVAL 24 MONTH) ',
        '), ',
        'birth_cohort AS ( ',
        '    SELECT * FROM tmp_birth_cohort WHERE row_num = 1 ',
        '), ',
        'tmp_final_outcome AS ( ',
        '    SELECT fo.client_id, fo.date_when_final_outcome_was_known, fo.positive, fo.negative_result, ',
        '           fo.still_on_bfexposed, fo.lost_to_followup, fo.died, fo.transferred_out, bc.date_of_birth, ',
        '           TIMESTAMPDIFF(MONTH, bc.date_of_birth, fo.date_when_final_outcome_was_known) as age_at_outcome_months, ',
        '           ROW_NUMBER() OVER (PARTITION BY fo.client_id ORDER BY fo.date_when_final_outcome_was_known DESC, fo.encounter_id DESC) as row_num ',
        '    FROM mamba_flat_encounter_hei_final_outcome fo ',
        '    JOIN birth_cohort bc ON fo.client_id = bc.client_id ',
        '    WHERE fo.date_when_final_outcome_was_known IS NOT NULL ',
        '), ',
        'final_outcome AS ( ',
        '    SELECT * FROM tmp_final_outcome WHERE row_num = 1 AND age_at_outcome_months <= 18 ',
        '), ',
        'outcome_categorized AS ( ',
        '    SELECT fo.*, ',
        '           CASE WHEN fo.positive IS NOT NULL THEN ''HIV-infected'' ',
        '                WHEN fo.negative_result IS NOT NULL THEN ''HIV-uninfected'' ',
        '                WHEN fo.died IS NOT NULL THEN ''Died without status known'' ',
        '                WHEN fo.still_on_bfexposed IS NOT NULL OR fo.lost_to_followup IS NOT NULL OR fo.transferred_out IS NOT NULL THEN ''HIV-final status unknown'' ',
        '                ELSE ''HIV-final status unknown'' ',
        '           END AS outcome_type ',
        '    FROM final_outcome fo ',
        ') '
    );
    
    -- Build final SELECT based on REPORT_TYPE
    IF REPORT_TYPE = 'DENOMINATOR' THEN
        SET final_query = CONCAT(base_query, 
            'SELECT COALESCE(COUNT(*), 0) AS Denominator FROM birth_cohort'
        );
    
    ELSEIF REPORT_TYPE = 'NUMERATOR' THEN
        SET final_query = CONCAT(base_query,
            'SELECT COALESCE(COUNT(*), 0) AS Numerator FROM outcome_categorized'
        );
    
    ELSEIF REPORT_TYPE = 'DISAGGREGATED' THEN
        SET final_query = CONCAT(base_query,
            'SELECT ',
            '    COALESCE(SUM(CASE WHEN outcome_type = ''HIV-infected'' THEN 1 ELSE 0 END), 0) AS `HIV-infected`, ',
            '    COALESCE(SUM(CASE WHEN outcome_type = ''HIV-uninfected'' THEN 1 ELSE 0 END), 0) AS `HIV-uninfected`, ',
            '    COALESCE(SUM(CASE WHEN outcome_type = ''HIV-final status unknown'' THEN 1 ELSE 0 END), 0) AS `HIV-final status unknown`, ',
            '    COALESCE(SUM(CASE WHEN outcome_type = ''Died without status known'' THEN 1 ELSE 0 END), 0) AS `Died without status known` ',
            'FROM outcome_categorized'
        );
    
    ELSEIF REPORT_TYPE = 'TOTAL' THEN
        SET final_query = CONCAT(base_query,
            'SELECT ',
            '    (SELECT COALESCE(COUNT(*), 0) FROM birth_cohort) AS Denominator, ',
            '    (SELECT COALESCE(COUNT(*), 0) FROM outcome_categorized) AS Numerator, ',
            '    COALESCE(SUM(CASE WHEN outcome_type = ''HIV-infected'' THEN 1 ELSE 0 END), 0) AS `HIV-infected`, ',
            '    COALESCE(SUM(CASE WHEN outcome_type = ''HIV-uninfected'' THEN 1 ELSE 0 END), 0) AS `HIV-uninfected`, ',
            '    COALESCE(SUM(CASE WHEN outcome_type = ''HIV-final status unknown'' THEN 1 ELSE 0 END), 0) AS `HIV-final status unknown`, ',
            '    COALESCE(SUM(CASE WHEN outcome_type = ''Died without status known'' THEN 1 ELSE 0 END), 0) AS `Died without status known` ',
            'FROM outcome_categorized'
        );
    
    ELSE
        -- DEBUG mode
        SET final_query = CONCAT(base_query,
            'SELECT client_id AS Client, outcome_type AS Outcome FROM outcome_categorized'
        );
    END IF;
    
    -- Prepare and execute the dynamic query
    SET @sql = final_query;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END //

DELIMITER ;