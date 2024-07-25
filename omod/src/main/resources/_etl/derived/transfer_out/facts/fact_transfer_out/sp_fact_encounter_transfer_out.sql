-- $BEGIN
CALL sp_fact_encounter_transfer_out_create();
CALL sp_fact_encounter_transfer_out_insert();
CALL sp_fact_encounter_transfer_out_update();
-- $END