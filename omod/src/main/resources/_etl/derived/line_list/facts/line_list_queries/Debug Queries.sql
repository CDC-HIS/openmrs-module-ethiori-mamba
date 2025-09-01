
show full processlist ;
select count(*) from mamba_z_encounter_obs;

SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'analytics_db';
SELECT COUNT(*) AS 'Stored Procedures' FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'analytics_db';
SELECT COUNT(*) AS 'Stored Procedures' FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA = 'analytics_db';
# ROUTINE_TYPE = 'PROCEDURE' AND


select count(*) from obs;
cat re-test.json | jq '.. | select(type=="object" and .concept) | .concept'

select * from mamba_flat_encounter_ict_general ict_general
                  join mamba_dim_relationship relationship on ict_general.client_id = relationship.person_a