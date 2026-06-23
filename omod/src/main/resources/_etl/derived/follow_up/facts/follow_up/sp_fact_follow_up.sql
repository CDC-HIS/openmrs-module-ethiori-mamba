-- Wrapper for the follow-up materialization.
-- Always performs a full rebuild: _create (idempotent CREATE TABLE)
-- followed by _insert (TRUNCATE + 10-way LEFT JOIN INSERT).
-- The table is dropped by sp_data_processing_derived_follow_up
-- before this wrapper is called, so the wrapper is responsible only
-- for the create-then-populate sequence.
DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_follow_up;

CREATE PROCEDURE sp_fact_follow_up()
BEGIN
    CALL sp_fact_follow_up_create();
    CALL sp_fact_follow_up_insert();
END //

DELIMITER ;
