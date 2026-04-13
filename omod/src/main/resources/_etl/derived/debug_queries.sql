-- =========================================================================
-- DEBUG QUERIES FOR V1 vs V2 HYBRID LATE ROW LOOKUP OPTIMIZATION
-- =========================================================================

-- -------------------------------------------------------------------------
-- SETUP: Initialize the Base View (Required for V2)
-- (This should eventually be placed in sp_mamba_data_processing_etl.sql)
-- -------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_mamba_fact_encounter_follow_up AS 
SELECT * 
FROM mamba_flat_encounter_follow_up
    LEFT JOIN mamba_flat_encounter_follow_up_1 USING (encounter_id)
    LEFT JOIN mamba_flat_encounter_follow_up_2 USING (encounter_id)
    LEFT JOIN mamba_flat_encounter_follow_up_3 USING (encounter_id)
    LEFT JOIN mamba_flat_encounter_follow_up_4 USING (encounter_id)
    LEFT JOIN mamba_flat_encounter_follow_up_5 USING (encounter_id)
    LEFT JOIN mamba_flat_encounter_follow_up_6 USING (encounter_id)
    LEFT JOIN mamba_flat_encounter_follow_up_7 USING (encounter_id)
    LEFT JOIN mamba_flat_encounter_follow_up_8 USING (encounter_id)
    LEFT JOIN mamba_flat_encounter_follow_up_9 USING (encounter_id);


-- -------------------------------------------------------------------------
-- TEST SCENARIO 1: End of Year Report (2024-12-31)
-- -------------------------------------------------------------------------

-- Run V1 (Original memory-heavy version)
SET @report_date = '2024-12-31';
CALL sp_fact_line_list_tx_curr_query(@report_date);
-- Check memory/time

-- Run V2 (Hybrid Late Row Lookup version)
CALL sp_fact_line_list_tx_curr_query_v2(@report_date);
-- Check memory/time


-- -------------------------------------------------------------------------
-- TEST SCENARIO 2: Mid-Year Large Batch (2023-06-30)
-- -------------------------------------------------------------------------
SET @report_date = '2023-06-30';
CALL sp_fact_line_list_tx_curr_query(@report_date);

CALL sp_fact_line_list_tx_curr_query_v2(@report_date);


-- -------------------------------------------------------------------------
-- TEST SCENARIO 3: Current Date (Real-time dynamic run)
-- -------------------------------------------------------------------------
SET @report_date = CURDATE();
CALL sp_fact_line_list_tx_curr_query(@report_date);

CALL sp_fact_line_list_tx_curr_query_v2(@report_date);


-- =========================================================================
-- VALIDATION SCRIPT (Checking row counts match exactly between v1 and v2)
-- Note: Replace the SP calls above with temporary tables if doing direct comparison
-- =========================================================================
