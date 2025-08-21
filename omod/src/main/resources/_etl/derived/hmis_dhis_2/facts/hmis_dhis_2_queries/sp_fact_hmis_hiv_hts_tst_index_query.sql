DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_hts_tst_index_query;

CREATE PROCEDURE sp_fact_hmis_hiv_hts_tst_index_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH contact_list as (select contact.client_id,
                             contact.elicited_date,
                             contact.hiv_test_date,
                             contact.hiv_test_result,
                             prior_hiv_test_result,
                             prior_test_date_estimated,
                             date_of_case_closure,
                             client.mrn,
                             client.uan,
                             date_of_birth,
                             client.sex
                      from
                          mamba_flat_encounter_index_contact_followup contact
                              left join mamba_flat_encounter_index_contact_followup_1 contact_1
                                        on contact.encounter_id = contact_1.encounter_id
                             join mamba_dim_client client on contact.client_id = client.client_id),
         offer_list as (select client.client_id,
                               client.mrn,
                               client.uan,
                               date_of_birth,
                               client.sex,
                               offer.offered_date,le
                               offered,
                               offer.yes,
                               offer.no,
                               accepted,
                               accepted_date,
                               ROW_NUMBER() over (PARTITION BY client.client_id ORDER BY offered_date DESC ) as row_num
                        from mamba_flat_encounter_ict_general ict_general
                                 join mamba_flat_encounter_ict_offer offer on ict_general.client_id = offer.client_id
                                 join mamba_dim_client client on ict_general.client_id = client.client_id
                        where offered_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         offer as (select * from offer_list where row_num = 1)
    -- hiv_test_date BETWEEN ? AND ? and hiv_test_result is not null
-- Number of individuals who were identified and tested using Index testing services and received their result
    SELECT 'HIV_HTS_TST_INDEX'                                                                         AS S_NO,
           'Number of individuals who were identified and tested using Index testing services and received their result' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null
-- Number of index cases offered
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1'                                                                         AS S_NO,
           'Number of index cases offered' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where offer.offered_date is not null
-- < 1 year, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 1'                                                                         AS S_NO,
           '< 1 year, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1 and sex = 'Male'
-- < 1 year, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 2'                                                                         AS S_NO,
           '< 1 year, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1 and sex = 'Female'
-- 1 - 4 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 3'                                                                        AS S_NO,
           '1 - 4 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4 and sex = 'Male'
-- 1 - 4 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 4'                                                                        AS S_NO,
           '1 - 4 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4 and sex = 'Female'
-- 5 - 9 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 5'                                                                        AS S_NO,
           '5 - 9 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9 and sex = 'Male'
-- 5 - 9 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 6'                                                                        AS S_NO,
           '5 - 9 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9 and sex = 'Female'
-- 10 - 14 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 7'                                                                        AS S_NO,
           '10 - 14 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14 and sex = 'Male'
-- 10 - 14 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 8'                                                                        AS S_NO,
           '10 - 14 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14 and sex = 'Female'
-- 15 - 19 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 9'                                                                        AS S_NO,
           '15 - 19 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19 and sex = 'Male'
-- 15 - 19 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 10'                                                                        AS S_NO,
           '15 - 19 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19 and sex = 'Female'
-- 20 - 24 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 11'                                                                        AS S_NO,
           '20 - 24 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24 and sex = 'Male'
-- 20 - 24 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 12'                                                                        AS S_NO,
           '20 - 24 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24 and sex = 'Female'
-- 25 - 29 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 13'                                                                        AS S_NO,
           '25 - 29 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29 and sex = 'Male'
-- 25 - 29 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 14'                                                                        AS S_NO,
           '25 - 29 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29 and sex = 'Female'
-- 30 - 34 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 15'                                                                        AS S_NO,
           '30 - 34 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34 and sex = 'Male'
-- 30 - 34 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 16'                                                                        AS S_NO,
           '30 - 34 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34 and sex = 'Female'
-- 35 - 39 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 17'                                                                        AS S_NO,
           '35 - 39 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39 and sex = 'Male'
-- 35 - 39 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 18'                                                                        AS S_NO,
           '35 - 39 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39 and sex = 'Female'
-- 40 - 44 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 19'                                                                        AS S_NO,
           '40 - 44 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44 and sex = 'Male'
-- 40 - 44 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 20'                                                                        AS S_NO,
           '40 - 44 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44 and sex = 'Female'
-- 45 - 49 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 21'                                                                        AS S_NO,
           '45 - 49 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49 and sex = 'Male'
-- 45 - 49 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 22'                                                                        AS S_NO,
           '45 - 49 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49 and sex = 'Female'
-- >= 50 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 23'                                                                        AS S_NO,
           '>= 50 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50 and sex = 'Male'
-- >= 50 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.1. 24'                                                                        AS S_NO,
           '>= 50 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM offer
    where TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50 and sex = 'Female'
-- Number of contacts elicited
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.2'                                                                        AS S_NO,
           'Number of contacts elicited' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
-- < 15 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.2. 1'                                                                        AS S_NO,
           '< 15 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and
          TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15 and sex = 'Male'
-- < 15 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.2. 2'                                                                        AS S_NO,
           '< 15 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and
          TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 15 and sex = 'Female'
-- >= 15 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.2. 3'                                                                        AS S_NO,
           '>= 15 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and
          TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15 and sex = 'Male'
-- >= 15 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.2. 4'                                                                        AS S_NO,
           '>= 15 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and
          TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15 and sex = 'Female'
-- Number of contacts tested
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3'                                                                        AS S_NO,
           'Number of contacts tested' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null
-- < 1 year, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 1'                                                                        AS S_NO,
           '< 1 year, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
          TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) <1 and sex = 'Male'
-- < 1 year, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 2'                                                                        AS S_NO,
           '< 1 year, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) <1 and sex = 'Female'
-- 1 - 4 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 3'                                                                        AS S_NO,
           '1 - 4 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4 and sex = 'Male'
-- 1 - 4 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 4'                                                                        AS S_NO,
           '1 - 4 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4 and sex = 'Female'
-- 5 - 9 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 5'                                                                        AS S_NO,
           '5 - 9 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9 and sex = 'Male'
-- 5 - 9 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 6'                                                                        AS S_NO,
           '5 - 9 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9 and sex = 'Female'
-- 10 - 14 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 7'                                                                        AS S_NO,
           '10 - 14 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14 and sex = 'Male'
-- 10 - 14 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 8'                                                                        AS S_NO,
           '10 - 14 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14 and sex = 'Female'
-- 15 - 19 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 9'                                                                        AS S_NO,
           '15 - 19 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19 and sex = 'Male'
-- 15 - 19 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 10'                                                                        AS S_NO,
           '15 - 19 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19 and sex = 'Female'
-- 20 - 24 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 11'                                                                        AS S_NO,
           '20 - 24 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24 and sex = 'Male'
-- 20 - 24 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 12'                                                                        AS S_NO,
           '20 - 24 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24 and sex = 'Female'
-- 25 - 29 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 13'                                                                        AS S_NO,
           '25 - 29 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29 and sex = 'Male'
-- 25 - 29 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 14'                                                                        AS S_NO,
           '25 - 29 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29 and sex = 'Female'
-- 30 - 34 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 15'                                                                        AS S_NO,
           '30 - 34 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34 and sex = 'Male'
-- 30 - 34 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 16'                                                                        AS S_NO,
           '30 - 34 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34 and sex = 'Female'
-- 35 - 39 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 17'                                                                        AS S_NO,
           '35 - 39 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39 and sex = 'Male'
-- 35 - 39 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 18'                                                                        AS S_NO,
           '35 - 39 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39 and sex = 'Female'
-- 40 - 44 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 19'                                                                        AS S_NO,
           '40 - 44 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44 and sex = 'Male'
-- 40 - 44 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 20'                                                                        AS S_NO,
           '40 - 44 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44 and sex = 'Female'
-- 45 - 49 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 21'                                                                        AS S_NO,
           '45 - 49 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49 and sex = 'Male'
-- 45 - 49 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 22'                                                                        AS S_NO,
           '45 - 49 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49 and sex = 'Female'
-- >= 50 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 23'                                                                        AS S_NO,
           '>= 50 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >=50 and sex = 'Male'
-- >= 50 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX.3. 24'                                                                        AS S_NO,
           '>= 50 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is not null and
        TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >=50 and sex = 'Female'
-- Number of contacts by test result (Positive)
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4'                                                                        AS S_NO,
           'Number of contacts by test result (Positive)' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where (prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
               and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE)
    OR (prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE)
-- Number of contacts by test result (Positive)
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1'                                                                        AS S_NO,
           'New positive' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
-- < 1 year, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 1'                                                                        AS S_NO,
           '< 1 year, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
    and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) <1 and sex = 'Male'
-- < 1 year, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 2'                                                                        AS S_NO,
           '< 1 year, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) <1 and sex = 'Female'
-- 1 - 4 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 3'                                                                        AS S_NO,
           '1 - 4 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4 and sex = 'Male'
-- 1 - 4 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 4'                                                                        AS S_NO,
           '1 - 4 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4 and sex = 'Female'
-- 5 - 9 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 5'                                                                        AS S_NO,
           '5 - 9 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9 and sex = 'Male'
-- 5 - 9 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 6'                                                                        AS S_NO,
           '5 - 9 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9 and sex = 'Female'
-- 10 - 14 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 7'                                                                        AS S_NO,
           '10 - 14 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14 and sex = 'Male'
-- 10 - 14 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 8'                                                                        AS S_NO,
           '10 - 14 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14 and sex = 'Female'
-- 15 - 19 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 9'                                                                        AS S_NO,
           '15 - 19 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19 and sex = 'Male'
-- 15 - 19 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 10'                                                                        AS S_NO,
           '15 - 19 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19 and sex = 'Female'
-- 20 - 24 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 11'                                                                        AS S_NO,
           '20 - 24 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24 and sex = 'Male'
-- 20 - 24 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 12'                                                                        AS S_NO,
           '20 - 24 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24 and sex = 'Female'
-- 25 - 29 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 13'                                                                        AS S_NO,
           '25 - 29 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29 and sex = 'Male'
-- 25 - 29 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 14'                                                                        AS S_NO,
           '25 - 29 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29 and sex = 'Female'
-- 30 - 34 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 15'                                                                        AS S_NO,
           '30 - 34 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34 and sex = 'Male'
-- 30 - 34 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 16'                                                                        AS S_NO,
           '30 - 34 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34 and sex = 'Female'
-- 35 - 39 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 17'                                                                        AS S_NO,
           '35 - 39 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39 and sex = 'Male'
-- 35 - 39 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 18'                                                                        AS S_NO,
           '35 - 39 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39 and sex = 'Female'
-- 40 - 44 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 19'                                                                        AS S_NO,
           '40 - 44 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44 and sex = 'Male'
-- 40 - 44 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 20'                                                                        AS S_NO,
           '40 - 44 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44 and sex = 'Female'
-- 45 - 49 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 21'                                                                        AS S_NO,
           '45 - 49 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49 and sex = 'Male'
-- 45 - 49 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 22'                                                                        AS S_NO,
           '45 - 49 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49 and sex = 'Female'
-- >= 50 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 23'                                                                        AS S_NO,
           '>= 50 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50 and sex = 'Male'
-- >= 50 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.1. 24'                                                                        AS S_NO,
           '>= 50 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result != 'Positive' and hiv_test_result = 'Positive'
      and coalesce(hiv_test_date,date_of_case_closure,elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 50 and sex = 'Female'
-- Known positive
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2'                                                                        AS S_NO,
           'Known positive' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
-- < 1 year, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 1'                                                                        AS S_NO,
           '< 1 year, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
    and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1 and sex = 'Male'
-- < 1 year, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 2'                                                                        AS S_NO,
           '< 1 year, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) < 1 and sex = 'Female'
-- 1 - 4 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 3'                                                                        AS S_NO,
           '1 - 4 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4 and sex = 'Male'
-- 1 - 4 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 4'                                                                        AS S_NO,
           '1 - 4 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 1 AND 4 and sex = 'Female'
-- 5 - 9 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 5'                                                                        AS S_NO,
           '5 - 9 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9 and sex = 'Male'
-- 5 - 9 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 6'                                                                        AS S_NO,
           '5 - 9 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 5 AND 9 and sex = 'Female'
-- 10 - 14 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 7'                                                                        AS S_NO,
           '10 - 14 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14 and sex = 'Male'
-- 10 - 14 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 8'                                                                        AS S_NO,
           '10 - 14 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 10 AND 14 and sex = 'Female'
-- 15 - 19 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 9'                                                                        AS S_NO,
           '15 - 19 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19 and sex = 'Male'
-- 15 - 19 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 10'                                                                        AS S_NO,
           '15 - 19 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19 and sex = 'Female'
-- 20 - 24 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 11'                                                                        AS S_NO,
           '20 - 24 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24 and sex = 'Male'
-- 20 - 24 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 12'                                                                        AS S_NO,
           '20 - 24 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 20 AND 24 and sex = 'Female'
-- 25 - 29 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 13'                                                                        AS S_NO,
           '25 - 29 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29 and sex = 'Male'
-- 25 - 29 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 14'                                                                        AS S_NO,
           '25 - 29 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 25 AND 29 and sex = 'Female'
-- 30 - 34 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 15'                                                                        AS S_NO,
           '30 - 34 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34 and sex = 'Male'
-- 30 - 34 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 16'                                                                        AS S_NO,
           '30 - 34 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 30 AND 34 and sex = 'Female'
-- 35 - 39 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 17'                                                                        AS S_NO,
           '35 - 39 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39 and sex = 'Male'
-- 35 - 39 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 18'                                                                        AS S_NO,
           '35 - 39 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 35 AND 39 and sex = 'Female'
-- 40 - 44 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 19'                                                                        AS S_NO,
           '40 - 44 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44 and sex = 'Male'
-- 40 - 44 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 20'                                                                        AS S_NO,
           '40 - 44 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 40 AND 44 and sex = 'Female'
-- 45 - 49 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 21'                                                                        AS S_NO,
           '45 - 49 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49 and sex = 'Male'
-- 45 - 49 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 22'                                                                        AS S_NO,
           '45 - 49 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 45 AND 49 and sex = 'Female'
-- >= 50 years, Male
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 23'                                                                        AS S_NO,
           '>= 50 years, Male' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >=50 and sex = 'Male'
-- >= 50 years, Female
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4.2. 24'                                                                        AS S_NO,
           '>= 50 years, Female' as Activity,
           COUNT(*)                                                                                  AS Value
    FROM contact_list
    where prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE
      and TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >=50 and sex = 'Female';
END //

DELIMITER ;