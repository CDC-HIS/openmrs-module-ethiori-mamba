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
       COUNT(*)                                                                               as Value
FROM linkage
-- Linked to care and treatment
UNION ALL
SELECT 'HIV_Linkage_NEW_CT. 1'        AS S_NO,
       'Linked to care and treatment' as Activity,
       COUNT(*)                       as Value
FROM linkage
where (linked_to_hiv_care = 'Yes' and reason_art_not_started_the_same_day is null and final_outcome is null)
   or final_outcome = 'On antiretroviral therapy'
   or (on_antiretroviral_therapy = 'On antiretroviral therapy' and final_outcome is null)
-- Known on ART
UNION ALL
SELECT 'HIV_Linkage_NEW_CT. 2' AS S_NO,
       'Known on ART'          as Activity,
       COUNT(*)                as Value
FROM linkage
where reason_art_not_started_the_same_day = 'Enrolled in HIV/ART care'
  AND final_outcome is null
-- Lost to follow up art
UNION ALL
SELECT 'HIV_Linkage_NEW_CT. 3' AS S_NO,
       'Lost to follow up art' as Activity,
       COUNT(*)                as Value
FROM linkage
where final_outcome = 'Loss to follow-up (LTFU)'
-- Referred to other facility
UNION ALL
SELECT 'HIV_Linkage_NEW_CT. 4'      AS S_NO,
       'Referred to other facility' as Activity,
       COUNT(*)                     as Value
FROM linkage
where final_outcome = 'Confirmed referral'
   or final_outcome = 'Started ART in other health facility'
   or (reason_art_not_started_the_same_day = 'Referred TX not initiated' and final_outcome is null)
-- Died
UNION ALL
SELECT 'HIV_Linkage_NEW_CT. 5' AS S_NO,
       'Died'                  as Activity,
       COUNT(*)                as Value
FROM linkage
where final_outcome = 'Died'
   or reason_art_not_started_the_same_day = 'Died'
-- Others
UNION ALL
SELECT 'HIV_Linkage_NEW_CT. 6' AS S_NO,
       'Others'                as Activity,
       COUNT(*)                as Value
FROM linkage
where final_outcome = 'Refusal of treatment by patient'
   or final_outcome = 'Other'
   or (reason_art_not_started_the_same_day = 'Refusal of treatment by patient' and final_outcome is null)
   or (reason_art_not_started_the_same_day = 'On adherence preparation' and final_outcome is null)
   or (reason_art_not_started_the_same_day = 'On OI management' and final_outcome is null)
   or (reason_art_not_started_the_same_day = 'Other' and final_outcome is null)
   or (
    (linked_to_hiv_care is null or linked_to_hiv_care = 'No')
  and (on_antiretroviral_therapy is null or on_antiretroviral_therapy = 'No') and final_outcome is null);
END //

DELIMITER ;