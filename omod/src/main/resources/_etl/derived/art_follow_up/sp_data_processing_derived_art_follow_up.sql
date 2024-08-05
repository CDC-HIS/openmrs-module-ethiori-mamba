-- $BEGIN
CALL sp_dim_client_art_follow_up();
CALL sp_fact_encounter_art_follow_up();
CALL sp_fact_encounter_art_first_follow_up();
CALL sp_fact_encounter_art_latest_follow_up();
CALL sp_fact_encounter_tx_new();
-- $END