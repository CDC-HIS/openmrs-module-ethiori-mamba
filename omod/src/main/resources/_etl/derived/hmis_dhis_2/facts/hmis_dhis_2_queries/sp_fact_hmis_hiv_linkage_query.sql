DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_linkage_query;

CREATE PROCEDURE sp_fact_hmis_hiv_linkage_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

WITH linkage as (select *
                 from mamba_flat_encounter_positive_tracking
                 where date_of_hiv_diagnosis BETWEEN REPORT_START_DATE AND REPORT_END_DATE)
-- Linkage outcome of newly identified Hiv positive individuals in the reporting period
SELECT 'HIV_Linkage_NEW_CT'                                                                   AS S_NO,
       'Linkage outcome of newly identified Hiv positive individuals in the reporting period' as Activity,
       COUNT(*) as Value
FROM linkage
-- Linked to care and treatment
UNION ALL
SELECT 'HIV_Linkage_NEW_CT. 1'        AS S_NO,
       'Linked to care and treatment' as Activity,
       COUNT(*) as Value
FROM linkage
-- Known on ART
UNION ALL
SELECT 'HIV_Linkage_NEW_CT. 2' AS S_NO,
       'Known on ART'          as Activity,
       COUNT(*) as Value
FROM linkage
-- Lost to follow up art
UNION ALL
SELECT 'HIV_Linkage_NEW_CT. 3' AS S_NO,
       'Lost to follow up art' as Activity,
       COUNT(*) as Value
FROM linkage
-- Referred to other facility
UNION ALL
SELECT 'HIV_Linkage_NEW_CT. 4'      AS S_NO,
       'Referred to other facility' as Activity,
       COUNT(*) as Value
FROM linkage
-- Died
UNION ALL
SELECT 'HIV_Linkage_NEW_CT. 5' AS S_NO,
       'Died'                  as Activity,
       COUNT(*) as Value
FROM linkage
-- Others
UNION ALL
SELECT 'HIV_Linkage_NEW_CT. 6' AS S_NO,
       'Others'                as Activity,
       COUNT(*) as Value
FROM linkage;
END //

DELIMITER ;