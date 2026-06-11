-- Wrapper for the follow-up materialization.
-- Always performs a full rebuild: _create (idempotent CREATE TABLE)
-- followed by _insert (TRUNCATE + 10-way LEFT JOIN INSERT).
-- The table is dropped by sp_data_processing_derived_follow_up
-- before this wrapper is called, so the wrapper is responsible only
-- for the create-then-populate sequence.
DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_followup;

CREATE PROCEDURE sp_fact_hmis_followup()
BEGIN
    CALL sp_fact_hmis_followup_create();
    CALL sp_fact_hmis_followup_insert();
END //

DELIMITER ;
