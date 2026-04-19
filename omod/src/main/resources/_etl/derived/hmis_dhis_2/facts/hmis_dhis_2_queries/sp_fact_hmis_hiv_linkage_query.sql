DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_linkage_query;

CREATE PROCEDURE sp_fact_hmis_hiv_linkage_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

WITH linkage as (select *
                 from mamba_flat_encounter_positive_tracking
                 where date_enrolled_in_care BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
     linkage_agg AS (
         SELECT COUNT(*) AS total,
                SUM(CASE WHEN (linked_to_hiv_care = 'Yes' AND reason_art_not_started_the_same_day IS NULL AND final_outcome IS NULL)
                           OR final_outcome = 'On antiretroviral therapy'
                           OR (on_antiretroviral_therapy = 'On antiretroviral therapy' AND final_outcome IS NULL)
                          THEN 1 ELSE 0 END)                                                                                     AS linked_to_care,
                SUM(CASE WHEN reason_art_not_started_the_same_day = 'Enrolled in HIV/ART care'
                           AND final_outcome IS NULL THEN 1 ELSE 0 END)                                                          AS known_on_art,
                SUM(CASE WHEN final_outcome = 'Loss to follow-up (LTFU)' THEN 1 ELSE 0 END)                                     AS ltfu,
                SUM(CASE WHEN final_outcome = 'Confirmed referral'
                           OR final_outcome = 'Started ART in other health facility'
                           OR (reason_art_not_started_the_same_day = 'Referred TX not initiated' AND final_outcome IS NULL)
                          THEN 1 ELSE 0 END)                                                                                     AS referred,
                SUM(CASE WHEN final_outcome = 'Died'
                           OR reason_art_not_started_the_same_day = 'Died' THEN 1 ELSE 0 END)                                   AS died,
                SUM(CASE WHEN final_outcome = 'Refusal of treatment by patient'
                           OR final_outcome = 'Other'
                           OR (reason_art_not_started_the_same_day = 'Refusal of treatment by patient' AND final_outcome IS NULL)
                           OR (reason_art_not_started_the_same_day = 'On adherence preparation' AND final_outcome IS NULL)
                           OR (reason_art_not_started_the_same_day = 'On OI management' AND final_outcome IS NULL)
                           OR (reason_art_not_started_the_same_day = 'Other' AND final_outcome IS NULL)
                           OR ((linked_to_hiv_care IS NULL OR linked_to_hiv_care = 'No')
                               AND (on_antiretroviral_therapy IS NULL OR on_antiretroviral_therapy = 'No')
                               AND final_outcome IS NULL)
                          THEN 1 ELSE 0 END)                                                                                     AS others
         FROM linkage
     )

SELECT 'HIV_Linkage_NEW_CT'                                                                   AS S_NO,
       'Linkage outcome of newly identified Hiv positive individuals in the reporting period' as Activity,
       total                                                                                  as Value
FROM linkage_agg
UNION ALL SELECT 'HIV_Linkage_NEW_CT. 1', 'Linked to care and treatment', linked_to_care FROM linkage_agg
UNION ALL SELECT 'HIV_Linkage_NEW_CT. 2', 'Known on ART',                 known_on_art   FROM linkage_agg
UNION ALL SELECT 'HIV_Linkage_NEW_CT. 3', 'Lost to follow up art',        ltfu           FROM linkage_agg
UNION ALL SELECT 'HIV_Linkage_NEW_CT. 4', 'Referred to other facility',   referred       FROM linkage_agg
UNION ALL SELECT 'HIV_Linkage_NEW_CT. 5', 'Died',                         died           FROM linkage_agg
UNION ALL SELECT 'HIV_Linkage_NEW_CT. 6', 'Others',                       others         FROM linkage_agg;
END //

DELIMITER ;
