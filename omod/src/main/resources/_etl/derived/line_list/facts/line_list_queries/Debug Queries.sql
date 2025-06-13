
show full processlist ;

SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'analytics_db';
SELECT COUNT(*) AS 'Stored Procedures' FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'analytics_db';
SELECT COUNT(*) AS 'Stored Procedures' FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA = 'analytics_db';
# ROUTINE_TYPE = 'PROCEDURE' AND


select count(*) from obs;
cat re-test.json | jq '.. | select(type=="object" and .concept) | .concept'