-- $BEGIN
CALL sp_fact_client_create();

-- Build the per-client "latest event" aggregates as session-scoped temp tables
-- (indexed off mamba_fact_follow_up) so sp_fact_client_insert can join against
-- them instead of re-deriving each one via a shared, unindexed CTE. NULL means
-- "no as-of cutoff" -- use all history, same as the previous inline logic.
CALL sp_tmp_build_latest_followup(NULL);
CALL sp_tmp_build_medical_aggregates(NULL);
CALL sp_tmp_build_vl_eligibility(NULL);
CALL sp_tmp_build_cxca_eligibility(NULL);
CALL sp_tmp_build_family_planning(NULL);
CALL sp_tmp_build_latest_cd4(NULL);
CALL sp_tmp_build_latest_disclosure(NULL);

CALL sp_fact_client_insert();
CALL sp_fact_client_update();
-- $END