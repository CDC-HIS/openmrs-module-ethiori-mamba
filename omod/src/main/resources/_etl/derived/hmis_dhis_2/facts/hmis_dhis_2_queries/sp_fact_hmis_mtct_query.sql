DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_mtct_query;

CREATE PROCEDURE sp_fact_hmis_mtct_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
WITH FollowUp as (
    select client_id from mamba_flat_encounter_follow_up
    where encounter_datetime = '2055-09-30'
)

-- Percentage of HIV-positive pregnant women who received ART to reduce the risk of mother-to child-transmission (MTCT) during pregnancy, L&D and PNC
SELECT 'MTCT_ART'                                                                                                         AS S_NO,
       'Percentage of HIV-positive pregnant women who received ART to reduce the risk of mother-to child-transmission (MTCT) during pregnancy, L&D and PNC' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Number of HIV positive women who received ART to reduce the risk of mother to child transmission during ANC for the first time
UNION ALL
SELECT 'MTCT_ART.1.'                                                                                                         AS S_NO,
       'Number of HIV positive women who received ART to reduce the risk of mother to child transmission during ANC for the first time' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Number of HIV positive Pregnant women who received ART to reduce the risk of mother to child transmission during L&D for the first time
UNION ALL
SELECT 'MTCT_ART.2.'                                                                                                         AS S_NO,
       'Number of HIV positive Pregnant women who received ART to reduce the risk of mother to child transmission during L&D for the first time' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Number of HIV positive lactating women who received ART to reduce the risk of mother to child transmission during PNC for the first time
UNION ALL
SELECT 'MTCT_ART.3.'                                                                                                         AS S_NO,
       'Number of HIV positive lactating women who received ART to reduce the risk of mother to child transmission during PNC for the first time' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Number of HIV-positive women who get pregnant while on ART and linked to ANC
UNION ALL
SELECT 'MTCT_ART.4.'                                                                                                         AS S_NO,
       'Number of HIV-positive women who get pregnant while on ART and linked to ANC' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Percentage of  HIV exposed infants who received a virologic HIV test (sample collected) within 12 month
UNION ALL
SELECT 'MTCT_HEI_EID.'                                                                                                         AS S_NO,
       'Percentage of  HIV exposed infants who received a virologic HIV test (sample collected) within 12 month' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Percentage of  HIV exposed infants who received a virologic HIV test (sample collected) within 12 month
UNION ALL
SELECT 'MTCT_HEI_EID.1'                                                                                                         AS S_NO,
       'Number of HIV exposed infants who received a virologic HIV test (sample collected) 0- 2 months of birth' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Number of HIV exposed infants who received a virologic HIV test (sample collected) 2-12 months of birth
UNION ALL
SELECT 'MTCT_HEI_EID.2'                                                                                                         AS S_NO,
       'Number of HIV exposed infants who received a virologic HIV test (sample collected) 2-12 months of birth' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Total Number of infants within 12 month received virological test result
UNION ALL
SELECT 'MTCT_HEI_EID.1'                                                                                                         AS S_NO,
       'Total Number of infants within 12 month received virological test result' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Number of HIV exposed infants who received an HIV test 0- 2 months of birth
UNION ALL
SELECT 'MTCT_HEI_EID.1.1'                                                                                                         AS S_NO,
       'Number of HIV exposed infants who received an HIV test 0- 2 months of birth' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Positive
UNION ALL
SELECT 'MTCT_HEI_EID.1.1. 1'                                                                                                         AS S_NO,
       'Positive' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Negative
UNION ALL
SELECT 'MTCT_HEI_EID.1.1. 2'                                                                                                         AS S_NO,
       'Negative' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Number of HIV exposed infants who received an HIV test 2-12 months of birth
UNION ALL
SELECT 'MTCT_HEI_EID.1.2'                                                                                                         AS S_NO,
       'Number of HIV exposed infants who received an HIV test 2-12 months of birth' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Positive
UNION ALL
SELECT 'MTCT_HEI_EID.1.2. 1'                                                                                                         AS S_NO,
       'Positive' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Negative
UNION ALL
SELECT 'MTCT_HEI_EID.1.2. 2'                                                                                                         AS S_NO,
       'Negative' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Percentage of exposed Infants born to HIV positive women who were started on co-trimoxazole prophylaxis within two months of birth
UNION ALL
SELECT 'MTCT_HEI_COTR'                                                                                                         AS S_NO,
       'Percentage of exposed Infants born to HIV positive women who were started on co-trimoxazole prophylaxis within two months of birth' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Number of infants born to HIV positive women started on co-trimoxazole prophylaxis within two months of birth
UNION ALL
SELECT 'MTCT_HEI_COTR.1.'                                                                                                         AS S_NO,
       'Number of infants born to HIV positive women started on co-trimoxazole prophylaxis within two months of birth' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Percentage of Infants born to HIV-infected women receiving antiretroviral (ARV) prophylaxis for prevention of Women-to-child transmission (PMTCT)
UNION ALL
SELECT 'RMH_PMTCT_IARV'                                                                                                         AS S_NO,
       'Percentage of Infants born to HIV-infected women receiving antiretroviral (ARV) prophylaxis for prevention of Women-to-child transmission (PMTCT)' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Number of HIV exposed infants who received ARV prophylaxis For 12 weeks
UNION ALL
SELECT 'MTCT_HEI_ARV.1.'                                                                                                         AS S_NO,
       'Number of HIV exposed infants who received ARV prophylaxis For 12 weeks' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Number of HIV positive women who gave birth at health institution
UNION ALL
SELECT 'MTCT_HEI_ARV.2.'                                                                                                         AS S_NO,
       'Number of HIV positive women who gave birth at health institution' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Percentage of HIV exposed infants receiving HIV confirmatory (antibody test) test by 18 months
UNION ALL
SELECT 'MTCT_HEI_ABTST'                                                                                                         AS S_NO,
       'Percentage of HIV exposed infants receiving HIV confirmatory (antibody test) test by 18 months' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Number of HIV exposed infants receiving HIV confirmatory (antibody test) by 18 months
UNION ALL
SELECT 'MTCT_HEI_ABTST.1'                                                                                                         AS S_NO,
       'Number of HIV exposed infants receiving HIV confirmatory (antibody test) by 18 months' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Positive
UNION ALL
SELECT 'MTCT_HEI_ABTST.1. 1'                                                                                                         AS S_NO,
       'Positive' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp
-- Negative
UNION ALL
SELECT 'MTCT_HEI_ABTST.1. 2'                                                                                                         AS S_NO,
       'Negative' as Activity,
       COUNT(*)                                                                                                                as Value
FROM FollowUp;
END //

DELIMITER ;