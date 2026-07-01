-- Wrapper for the follow-up materialization.
-- Always performs a full rebuild: _create (idempotent CREATE TABLE)
-- followed by _insert (TRUNCATE + 10-way LEFT JOIN INSERT), both against
-- mamba_fact_follow_up_staging. The live mamba_fact_follow_up table is
-- left untouched by this wrapper; sp_data_processing_derived_follow_up
-- swaps staging into place with an atomic RENAME after this call returns.
DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_follow_up;

CREATE PROCEDURE sp_fact_follow_up()
BEGIN
    CALL sp_fact_follow_up_create();
    CALL sp_fact_follow_up_insert();
END //

DELIMITER ;
