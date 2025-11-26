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

    -- Calculate birth cohort date range (24 months prior to reporting period)
    DECLARE birth_cohort_start_date DATE;
    DECLARE birth_cohort_end_date DATE;
    
    SET birth_cohort_start_date = DATE_SUB(REPORT_START_DATE, INTERVAL 24 MONTH);
    SET birth_cohort_end_date = DATE_SUB(REPORT_END_DATE, INTERVAL 24 MONTH);

    -- CTE for birth cohort (denominator)
    WITH tmp_birth_cohort AS (
        SELECT 
            e.client_id,
            c.date_of_birth,
            e.encounter_datetime as enrollment_date,
            ROW_NUMBER() OVER (PARTITION BY e.client_id ORDER BY e.encounter_datetime DESC, e.encounter_id DESC) as row_num
        FROM mamba_flat_encounter_hei_enrollment e
        JOIN mamba_dim_client c ON e.client_id = c.client_id
        WHERE c.date_of_birth BETWEEN birth_cohort_start_date AND birth_cohort_end_date
    ),
    birth_cohort AS (
        SELECT * FROM tmp_birth_cohort WHERE row_num = 1
    ),
    
    -- CTE for final outcomes
    tmp_final_outcome AS (
        SELECT 
            fo.client_id,
            fo.date_when_final_outcome_was_known,
            fo.hei_pmtct_final_outcome,
            fo.positive,
            fo.negative_result,
            fo.still_on_bfexposed,
            fo.lost_to_followup,
            fo.died,
            fo.transferred_out,
            bc.date_of_birth,
            TIMESTAMPDIFF(MONTH, bc.date_of_birth, fo.date_when_final_outcome_was_known) as age_at_outcome_months,
            ROW_NUMBER() OVER (PARTITION BY fo.client_id ORDER BY fo.date_when_final_outcome_was_known DESC, fo.encounter_id DESC) as row_num
        FROM mamba_flat_encounter_hei_final_outcome fo
        JOIN birth_cohort bc ON fo.client_id = bc.client_id
        WHERE fo.date_when_final_outcome_was_known IS NOT NULL
    ),
    final_outcome AS (
        SELECT * FROM tmp_final_outcome 
        WHERE row_num = 1 
        AND age_at_outcome_months <= 18
    ),
    
    -- CTE for outcome categorization
    outcome_categorized AS (
        SELECT 
            fo.*,
            CASE 
                WHEN fo.positive IS NOT NULL THEN 'HIV-infected'
                WHEN fo.negative_result IS NOT NULL THEN 'HIV-uninfected'
                WHEN fo.died IS NOT NULL THEN 'Died without status known'
                WHEN fo.still_on_bfexposed IS NOT NULL OR fo.lost_to_followup IS NOT NULL OR fo.transferred_out IS NOT NULL THEN 'HIV-final status unknown'
                ELSE 'HIV-final status unknown'
            END AS outcome_type
        FROM final_outcome fo
    )
    
    -- Generate output based on REPORT_TYPE
    SELECT 
        CASE 
            WHEN REPORT_TYPE = 'DENOMINATOR' THEN 'Denominator'
            WHEN REPORT_TYPE = 'NUMERATOR' THEN 'Numerator'
            WHEN REPORT_TYPE = 'DISAGGREGATED' THEN outcome_type
            WHEN REPORT_TYPE = 'TOTAL' THEN 
                CASE 
                    WHEN outcome_type IS NULL THEN 'Denominator'
                    ELSE outcome_type
                END
            WHEN REPORT_TYPE = 'DEBUG' THEN CONCAT('Client: ', client_id)
            ELSE outcome_type
        END AS Category,
        COUNT(*) AS Count
    FROM (
        -- For DENOMINATOR or TOTAL, include all birth cohort
        SELECT bc.client_id, NULL as outcome_type, bc.date_of_birth, NULL as date_when_final_outcome_was_known, NULL as age_at_outcome_months
        FROM birth_cohort bc
        WHERE REPORT_TYPE IN ('DENOMINATOR', 'TOTAL')
        
        UNION ALL
        
        -- For NUMERATOR, include only those with outcomes
        SELECT oc.client_id, NULL as outcome_type, oc.date_of_birth, oc.date_when_final_outcome_was_known, oc.age_at_outcome_months
        FROM outcome_categorized oc
        WHERE REPORT_TYPE = 'NUMERATOR'
        
        UNION ALL
        
        -- For DISAGGREGATED or default, show breakdown by outcome type
        SELECT oc.client_id, oc.outcome_type, oc.date_of_birth, oc.date_when_final_outcome_was_known, oc.age_at_outcome_months
        FROM outcome_categorized oc
        WHERE REPORT_TYPE IN ('DISAGGREGATED', 'TOTAL')
        
        UNION ALL
        
        -- For DEBUG, show detailed patient-level data
        SELECT oc.client_id, oc.outcome_type, oc.date_of_birth, oc.date_when_final_outcome_was_known, oc.age_at_outcome_months
        FROM outcome_categorized oc
        WHERE REPORT_TYPE = 'DEBUG'
    ) AS combined_results
    GROUP BY 
        CASE 
            WHEN REPORT_TYPE = 'DENOMINATOR' THEN 'Denominator'
            WHEN REPORT_TYPE = 'NUMERATOR' THEN 'Numerator'
            WHEN REPORT_TYPE = 'DISAGGREGATED' THEN outcome_type
            WHEN REPORT_TYPE = 'TOTAL' THEN 
                CASE 
                    WHEN outcome_type IS NULL THEN 'Denominator'
                    ELSE outcome_type
                END
            WHEN REPORT_TYPE = 'DEBUG' THEN CONCAT('Client: ', client_id)
            ELSE outcome_type
        END
    ORDER BY 
        CASE Category
            WHEN 'Denominator' THEN 1
            WHEN 'Numerator' THEN 2
            WHEN 'HIV-infected' THEN 3
            WHEN 'HIV-uninfected' THEN 4
            WHEN 'HIV-final status unknown' THEN 5
            WHEN 'Died without status known' THEN 6
            ELSE 7
        END;

END //

DELIMITER ;