-- $BEGIN
CALL sp_fact_client_create();

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