DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_hts_tst_index;

CREATE PROCEDURE sp_fact_hmis_hiv_hts_tst_index(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
WITH FollowUp as (
    select client_id from mamba_flat_encounter_follow_up
    where encounter_datetime = '2055-09-30'
)
-- Number of individuals who were identified and tested using Index testing services and received their result
SELECT 'HIV_HTS_TST_INDEX'                                                                         AS S_NO,
       'Number of individuals who were identified and tested using Index testing services and received their result' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- Number of index cases offered
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1'                                                                         AS S_NO,
       'Number of index cases offered' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- < 1 year, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 1'                                                                         AS S_NO,
       '< 1 year, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- < 1 year, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 2'                                                                         AS S_NO,
       '< 1 Female, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 1 - 4 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 3'                                                                        AS S_NO,
       '1 - 4 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 1 - 4 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 4'                                                                        AS S_NO,
       '1 - 4 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 5 - 9 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 5'                                                                        AS S_NO,
       '5 - 9 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 5 - 9 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 6'                                                                        AS S_NO,
       '5 - 9 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 10 - 14 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 7'                                                                        AS S_NO,
       '10 - 14 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 10 - 14 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 8'                                                                        AS S_NO,
       '10 - 14 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 15 - 19 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 9'                                                                        AS S_NO,
       '15 - 19 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 15 - 19 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 10'                                                                        AS S_NO,
       '15 - 19 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 20 - 24 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 11'                                                                        AS S_NO,
       '20 - 24 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 20 - 24 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 12'                                                                        AS S_NO,
       '20 - 24 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 25 - 29 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 13'                                                                        AS S_NO,
       '25 - 29 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 25 - 29 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 14'                                                                        AS S_NO,
       '25 - 29 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 30 - 34 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 15'                                                                        AS S_NO,
       '30 - 34 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 30 - 34 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 16'                                                                        AS S_NO,
       '30 - 34 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 35 - 39 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 17'                                                                        AS S_NO,
       '35 - 39 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 35 - 39 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 18'                                                                        AS S_NO,
       '35 - 39 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 40 - 44 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 19'                                                                        AS S_NO,
       '40 - 44 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 40 - 44 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 20'                                                                        AS S_NO,
       '40 - 44 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 45 - 49 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 21'                                                                        AS S_NO,
       '45 - 49 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 45 - 49 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 22'                                                                        AS S_NO,
       '45 - 49 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- >= 50 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 23'                                                                        AS S_NO,
       '>= 50 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- >= 50 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.1. 24'                                                                        AS S_NO,
       '>= 50 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- Number of contacts elicited
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.2'                                                                        AS S_NO,
       'Number of contacts elicited' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- < 15 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.2. 1'                                                                        AS S_NO,
       '< 15 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- < 15 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.2. 2'                                                                        AS S_NO,
       '< 15 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- >= 15 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.2. 3'                                                                        AS S_NO,
       '>= 15 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- >= 15 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.2. 4'                                                                        AS S_NO,
       '>= 15 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- Number of contacts tested
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3'                                                                        AS S_NO,
       'Number of contacts tested' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- < 1 year, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.1'                                                                        AS S_NO,
       '< 1 year, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- < 1 year, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.2'                                                                        AS S_NO,
       '< 1 year, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 1 - 4 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.3'                                                                        AS S_NO,
       '1 - 4 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 1 - 4 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.4'                                                                        AS S_NO,
       '1 - 4 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 5 - 9 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.5'                                                                        AS S_NO,
       '5 - 9 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 5 - 9 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.6'                                                                        AS S_NO,
       '5 - 9 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 10 - 14 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.7'                                                                        AS S_NO,
       '10 - 14 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 10 - 14 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.8'                                                                        AS S_NO,
       '10 - 14 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 15 - 19 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.9'                                                                        AS S_NO,
       '15 - 19 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 15 - 19 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.10'                                                                        AS S_NO,
       '15 - 19 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 20 - 24 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.11'                                                                        AS S_NO,
       '20 - 24 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 20 - 24 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.12'                                                                        AS S_NO,
       '20 - 24 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 25 - 29 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.13'                                                                        AS S_NO,
       '25 - 29 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 25 - 29 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.14'                                                                        AS S_NO,
       '25 - 29 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 30 - 34 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.15'                                                                        AS S_NO,
       '30 - 34 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 30 - 34 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.16'                                                                        AS S_NO,
       '30 - 34 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 35 - 39 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.17'                                                                        AS S_NO,
       '35 - 39 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 35 - 39 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.18'                                                                        AS S_NO,
       '35 - 39 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 40 - 44 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.19'                                                                        AS S_NO,
       '40 - 44 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 40 - 44 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.20'                                                                        AS S_NO,
       '40 - 44 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 45 - 49 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.21'                                                                        AS S_NO,
       '45 - 49 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 45 - 49 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.22'                                                                        AS S_NO,
       '45 - 49 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- >= 50 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.23'                                                                        AS S_NO,
       '>= 50 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- >= 50 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.3.24'                                                                        AS S_NO,
       '>= 50 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- Number of contacts by test result (Positive)
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4'                                                                        AS S_NO,
       'Number of contacts by test result (Positive)' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- Number of contacts by test result (Positive)
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1'                                                                        AS S_NO,
       'New positive' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- < 1 year, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.1'                                                                        AS S_NO,
       '< 1 year, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- < 1 year, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.2'                                                                        AS S_NO,
       '< 1 year, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 1 - 4 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.3'                                                                        AS S_NO,
       '1 - 4 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 1 - 4 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.4'                                                                        AS S_NO,
       '1 - 4 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 5 - 9 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.5'                                                                        AS S_NO,
       '5 - 9 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 5 - 9 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.6'                                                                        AS S_NO,
       '5 - 9 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 10 - 14 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.7'                                                                        AS S_NO,
       '10 - 14 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 10 - 14 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.8'                                                                        AS S_NO,
       '10 - 14 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 15 - 19 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.9'                                                                        AS S_NO,
       '15 - 19 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 15 - 19 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.10'                                                                        AS S_NO,
       '15 - 19 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 20 - 24 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.11'                                                                        AS S_NO,
       '20 - 24 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 20 - 24 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.12'                                                                        AS S_NO,
       '20 - 24 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 25 - 29 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.13'                                                                        AS S_NO,
       '25 - 29 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 25 - 29 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.14'                                                                        AS S_NO,
       '25 - 29 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 30 - 34 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.15'                                                                        AS S_NO,
       '30 - 34 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 30 - 34 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.16'                                                                        AS S_NO,
       '30 - 34 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 35 - 39 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.17'                                                                        AS S_NO,
       '35 - 39 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 35 - 39 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.18'                                                                        AS S_NO,
       '35 - 39 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 40 - 44 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.19'                                                                        AS S_NO,
       '40 - 44 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 40 - 44 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.20'                                                                        AS S_NO,
       '40 - 44 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 45 - 49 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.21'                                                                        AS S_NO,
       '45 - 49 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 45 - 49 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.22'                                                                        AS S_NO,
       '45 - 49 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- >= 50 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.23'                                                                        AS S_NO,
       '>= 50 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- >= 50 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.1.24'                                                                        AS S_NO,
       '>= 50 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- Known positive
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2'                                                                        AS S_NO,
       'Known positive' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- < 1 year, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.1'                                                                        AS S_NO,
       '< 1 year, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- < 1 year, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.2'                                                                        AS S_NO,
       '< 1 year, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 1 - 4 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.3'                                                                        AS S_NO,
       '1 - 4 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 1 - 4 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.4'                                                                        AS S_NO,
       '1 - 4 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 5 - 9 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.5'                                                                        AS S_NO,
       '5 - 9 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 5 - 9 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.6'                                                                        AS S_NO,
       '5 - 9 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 10 - 14 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.7'                                                                        AS S_NO,
       '10 - 14 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 10 - 14 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.8'                                                                        AS S_NO,
       '10 - 14 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 15 - 19 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.9'                                                                        AS S_NO,
       '15 - 19 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 15 - 19 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.10'                                                                        AS S_NO,
       '15 - 19 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 20 - 24 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.11'                                                                        AS S_NO,
       '20 - 24 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 20 - 24 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.12'                                                                        AS S_NO,
       '20 - 24 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 25 - 29 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.13'                                                                        AS S_NO,
       '25 - 29 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 25 - 29 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.14'                                                                        AS S_NO,
       '25 - 29 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 30 - 34 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.15'                                                                        AS S_NO,
       '30 - 34 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 30 - 34 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.16'                                                                        AS S_NO,
       '30 - 34 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 35 - 39 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.17'                                                                        AS S_NO,
       '35 - 39 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 35 - 39 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.18'                                                                        AS S_NO,
       '35 - 39 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 40 - 44 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.19'                                                                        AS S_NO,
       '40 - 44 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 40 - 44 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.20'                                                                        AS S_NO,
       '40 - 44 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 45 - 49 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.21'                                                                        AS S_NO,
       '45 - 49 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- 45 - 49 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.22'                                                                        AS S_NO,
       '45 - 49 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- >= 50 years, Male
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.23'                                                                        AS S_NO,
       '>= 50 years, Male' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp
-- >= 50 years, Female
UNION ALL
SELECT 'HIV_HTS_TST_INDEX.4.2.24'                                                                        AS S_NO,
       '>= 50 years, Female' as Activity,
       COUNT(*)                                                                                  AS Value
FROM FollowUp;
END //

DELIMITER ;