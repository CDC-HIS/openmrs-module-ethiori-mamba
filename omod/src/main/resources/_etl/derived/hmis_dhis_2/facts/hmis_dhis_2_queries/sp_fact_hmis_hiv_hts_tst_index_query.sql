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
                                 encounter.encounter_id,
                                 encounter.uuid,
                                 coalesce(contact_birthdate,index_contact.date_of_birth) as birthdate,
                                 CASE
                                     WHEN COALESCE(index_contact.sex, contact_1.respondent_gender) IN
                                          ('F', 'Female gender') THEN 'Female'
                                     WHEN COALESCE(index_contact.sex, contact_1.respondent_gender) IN
                                          ('M', 'Male gender') THEN 'Male'
                                     ELSE COALESCE(index_contact.sex, contact_1.respondent_gender)
                                     END AS sex
                          from mamba_flat_encounter_index_contact_followup contact
                                   left join mamba_flat_encounter_index_contact_followup_1 contact_1
                                             on contact.encounter_id = contact_1.encounter_id
                                   left join mamba_flat_encounter_ict_general g on contact.client_id = g.client_id
                                   left join mamba_dim_client index_client on contact.client_id = index_client.client_id
                                   left join mamba_dim_encounter encounter on contact.encounter_id = encounter.encounter_id
                                   left join mamba_dim_person_attribute attribute on encounter.uuid = attribute.value
                                   left join mamba_dim_client index_contact on attribute.person_id = index_contact.client_id),
         offer_list as (select client.client_id,
                               client.mrn,
                               client.uan,
                               date_of_birth,
                               client.sex,
                               offer.offered_date,
                               number_of_contacts_elicited,
                               offered,
                               offer.yes,
                               offer.no,
                               accepted,
                               accepted_date,
                               number_of_male_contacts_above_the_age_of_15,
                               number_of_male_contacts_below_the_age_of_15,
                               number_of_female_contacts_above_the_age_of_15,
                               number_of_female_contacts_below_the_age_of_15,
                               ROW_NUMBER() over (PARTITION BY client.client_id ORDER BY offered_date DESC ) as row_num
                        from mamba_flat_encounter_ict_general ict_general
                                 join mamba_flat_encounter_ict_offer offer on ict_general.client_id = offer.client_id
                                 join mamba_dim_client client on ict_general.client_id = client.client_id
                        where offered_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         offer as (select * from offer_list where row_num = 1),
         offer_agg AS (
             SELECT COUNT(*)                                                                                          AS total,
                    SUM(CASE WHEN age < 1    AND sex = 'Male'   THEN 1 ELSE 0 END)                                   AS u1_male,
                    SUM(CASE WHEN age < 1    AND sex = 'Female' THEN 1 ELSE 0 END)                                   AS u1_female,
                    SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u4_male,
                    SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u4_female,
                    SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u9_male,
                    SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u9_female,
                    SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u14_male,
                    SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u14_female,
                    SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u19_male,
                    SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u19_female,
                    SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u24_male,
                    SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u24_female,
                    SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u29_male,
                    SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u29_female,
                    SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u34_male,
                    SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u34_female,
                    SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u39_male,
                    SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u39_female,
                    SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u44_male,
                    SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u44_female,
                    SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u49_male,
                    SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u49_female,
                    SUM(CASE WHEN age >= 50 AND sex = 'Male'   THEN 1 ELSE 0 END)                                    AS o50_male,
                    SUM(CASE WHEN age >= 50 AND sex = 'Female' THEN 1 ELSE 0 END)                                    AS o50_female
             FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age FROM offer WHERE offered_date IS NOT NULL) t
         ),
         contact_elicited_agg AS (
             SELECT COUNT(*)                                                                    AS total,
                    SUM(CASE WHEN age < 15  AND sex = 'Male'   THEN 1 ELSE 0 END)              AS u15_male,
                    SUM(CASE WHEN age < 15  AND sex = 'Female' THEN 1 ELSE 0 END)              AS u15_female,
                    SUM(CASE WHEN age >= 15 AND sex = 'Male'   THEN 1 ELSE 0 END)              AS o15_male,
                    SUM(CASE WHEN age >= 15 AND sex = 'Female' THEN 1 ELSE 0 END)              AS o15_female
             FROM (SELECT *, TIMESTAMPDIFF(YEAR, birthdate, REPORT_END_DATE) AS age FROM contact_list
                   WHERE elicited_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE) t
         ),
         contact_tested_agg AS (
             SELECT COUNT(*)                                                                                          AS total,
                    SUM(CASE WHEN age < 1    AND sex = 'Male'   THEN 1 ELSE 0 END)                                   AS u1_male,
                    SUM(CASE WHEN age < 1    AND sex = 'Female' THEN 1 ELSE 0 END)                                   AS u1_female,
                    SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u4_male,
                    SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u4_female,
                    SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u9_male,
                    SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u9_female,
                    SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u14_male,
                    SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u14_female,
                    SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u19_male,
                    SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u19_female,
                    SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u24_male,
                    SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u24_female,
                    SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u29_male,
                    SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u29_female,
                    SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u34_male,
                    SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u34_female,
                    SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u39_male,
                    SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u39_female,
                    SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u44_male,
                    SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u44_female,
                    SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u49_male,
                    SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u49_female,
                    SUM(CASE WHEN age >= 50 AND sex = 'Male'   THEN 1 ELSE 0 END)                                    AS o50_male,
                    SUM(CASE WHEN age >= 50 AND sex = 'Female' THEN 1 ELSE 0 END)                                    AS o50_female
             FROM (SELECT *, TIMESTAMPDIFF(YEAR, birthdate, REPORT_END_DATE) AS age FROM contact_list
                   WHERE hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                     AND hiv_test_result IS NOT NULL) t
         ),
         new_positive_agg AS (
             SELECT COUNT(*)                                                                                          AS total,
                    SUM(CASE WHEN age < 1    AND sex = 'Male'   THEN 1 ELSE 0 END)                                   AS u1_male,
                    SUM(CASE WHEN age < 1    AND sex = 'Female' THEN 1 ELSE 0 END)                                   AS u1_female,
                    SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u4_male,
                    SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u4_female,
                    SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u9_male,
                    SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u9_female,
                    SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u14_male,
                    SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u14_female,
                    SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u19_male,
                    SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u19_female,
                    SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u24_male,
                    SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u24_female,
                    SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u29_male,
                    SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u29_female,
                    SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u34_male,
                    SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u34_female,
                    SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u39_male,
                    SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u39_female,
                    SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u44_male,
                    SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u44_female,
                    SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u49_male,
                    SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u49_female,
                    SUM(CASE WHEN age >= 50 AND sex = 'Male'   THEN 1 ELSE 0 END)                                    AS o50_male,
                    SUM(CASE WHEN age >= 50 AND sex = 'Female' THEN 1 ELSE 0 END)                                    AS o50_female
             FROM (SELECT *, TIMESTAMPDIFF(YEAR, birthdate, REPORT_END_DATE) AS age FROM contact_list
                   WHERE hiv_test_result = 'Positive'
                     AND coalesce(hiv_test_date, date_of_case_closure, elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE) t
         ),
         known_positive_agg AS (
             SELECT COUNT(*)                                                                                          AS total,
                    SUM(CASE WHEN age < 1    AND sex = 'Male'   THEN 1 ELSE 0 END)                                   AS u1_male,
                    SUM(CASE WHEN age < 1    AND sex = 'Female' THEN 1 ELSE 0 END)                                   AS u1_female,
                    SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u4_male,
                    SUM(CASE WHEN age BETWEEN 1  AND 4  AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u4_female,
                    SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u9_male,
                    SUM(CASE WHEN age BETWEEN 5  AND 9  AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u9_female,
                    SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u14_male,
                    SUM(CASE WHEN age BETWEEN 10 AND 14 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u14_female,
                    SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u19_male,
                    SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u19_female,
                    SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u24_male,
                    SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u24_female,
                    SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u29_male,
                    SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u29_female,
                    SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u34_male,
                    SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u34_female,
                    SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u39_male,
                    SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u39_female,
                    SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u44_male,
                    SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u44_female,
                    SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Male'   THEN 1 ELSE 0 END)                        AS u49_male,
                    SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Female' THEN 1 ELSE 0 END)                        AS u49_female,
                    SUM(CASE WHEN age >= 50 AND sex = 'Male'   THEN 1 ELSE 0 END)                                    AS o50_male,
                    SUM(CASE WHEN age >= 50 AND sex = 'Female' THEN 1 ELSE 0 END)                                    AS o50_female
             FROM (SELECT *, TIMESTAMPDIFF(YEAR, birthdate, REPORT_END_DATE) AS age FROM contact_list
                   WHERE prior_hiv_test_result = 'Positive' AND hiv_test_result IS NULL
                     AND elicited_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE) t
         )

    SELECT 'HIV_HTS_TST_INDEX'                                                                                           AS S_NO,
           'Number of individuals who were identified and tested using Index testing services and received their result' as Activity,
           total                                                                                                          AS Value
    FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1',      'Number of index cases offered',  total      FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 1',   '< 1 year, Male',                u1_male    FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 2',   '< 1 year, Female',              u1_female  FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 3',   '1 - 4 years, Male',             u4_male    FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 4',   '1 - 4 years, Female',           u4_female  FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 5',   '5 - 9 years, Male',             u9_male    FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 6',   '5 - 9 years, Female',           u9_female  FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 7',   '10 - 14 years, Male',           u14_male   FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 8',   '10 - 14 years, Female',         u14_female FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 9',   '15 - 19 years, Male',           u19_male   FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 10',  '15 - 19 years, Female',         u19_female FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 11',  '20 - 24 years, Male',           u24_male   FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 12',  '20 - 24 years, Female',         u24_female FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 13',  '25 - 29 years, Male',           u29_male   FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 14',  '25 - 29 years, Female',         u29_female FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 15',  '30 - 34 years, Male',           u34_male   FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 16',  '30 - 34 years, Female',         u34_female FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 17',  '35 - 39 years, Male',           u39_male   FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 18',  '35 - 39 years, Female',         u39_female FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 19',  '40 - 44 years, Male',           u44_male   FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 20',  '40 - 44 years, Female',         u44_female FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 21',  '45 - 49 years, Male',           u49_male   FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 22',  '45 - 49 years, Female',         u49_female FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 23',  '>= 50 years, Male',             o50_male   FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.1. 24',  '>= 50 years, Female',           o50_female FROM offer_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.2',      'Number of contacts elicited',   total      FROM contact_elicited_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.2. 1',   '< 15 years, Male',              u15_male   FROM contact_elicited_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.2. 2',   '< 15 years, Female',            u15_female FROM contact_elicited_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.2. 3',   '>= 15 years, Male',             o15_male   FROM contact_elicited_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.2. 4',   '>= 15 years, Female',           o15_female FROM contact_elicited_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3',      'Number of contacts tested',     total      FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 1',   '< 1 year, Male',                u1_male    FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 2',   '< 1 year, Female',              u1_female  FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 3',   '1 - 4 years, Male',             u4_male    FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 4',   '1 - 4 years, Female',           u4_female  FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 5',   '5 - 9 years, Male',             u9_male    FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 6',   '5 - 9 years, Female',           u9_female  FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 7',   '10 - 14 years, Male',           u14_male   FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 8',   '10 - 14 years, Female',         u14_female FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 9',   '15 - 19 years, Male',           u19_male   FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 10',  '15 - 19 years, Female',         u19_female FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 11',  '20 - 24 years, Male',           u24_male   FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 12',  '20 - 24 years, Female',         u24_female FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 13',  '25 - 29 years, Male',           u29_male   FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 14',  '25 - 29 years, Female',         u29_female FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 15',  '30 - 34 years, Male',           u34_male   FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 16',  '30 - 34 years, Female',         u34_female FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 17',  '35 - 39 years, Male',           u39_male   FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 18',  '35 - 39 years, Female',         u39_female FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 19',  '40 - 44 years, Male',           u44_male   FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 20',  '40 - 44 years, Female',         u44_female FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 21',  '45 - 49 years, Male',           u49_male   FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 22',  '45 - 49 years, Female',         u49_female FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 23',  '>= 50 years, Male',             o50_male   FROM contact_tested_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX.3. 24',  '>= 50 years, Female',           o50_female FROM contact_tested_agg
    UNION ALL
    SELECT 'HIV_HTS_TST_INDEX_4'                          AS S_NO,
           'Number of contacts by test result (Positive)' as Activity,
           COUNT(*)                                       AS Value
    FROM contact_list
    where (hiv_test_result = 'Positive'
        and coalesce(hiv_test_date, date_of_case_closure, elicited_date) BETWEEN REPORT_START_DATE AND REPORT_END_DATE)
       OR (prior_hiv_test_result = 'Positive' and elicited_date between REPORT_START_DATE AND REPORT_END_DATE and hiv_test_result is null)
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1',      'New positive',      total      FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 1',   '< 1 year, Male',   u1_male    FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 2',   '< 1 year, Female', u1_female  FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 3',   '1 - 4 years, Male',     u4_male    FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 4',   '1 - 4 years, Female',   u4_female  FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 5',   '5 - 9 years, Male',     u9_male    FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 6',   '5 - 9 years, Female',   u9_female  FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 7',   '10 - 14 years, Male',   u14_male   FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 8',   '10 - 14 years, Female', u14_female FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 9',   '15 - 19 years, Male',   u19_male   FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 10',  '15 - 19 years, Female', u19_female FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 11',  '20 - 24 years, Male',   u24_male   FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 12',  '20 - 24 years, Female', u24_female FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 13',  '25 - 29 years, Male',   u29_male   FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 14',  '25 - 29 years, Female', u29_female FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 15',  '30 - 34 years, Male',   u34_male   FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 16',  '30 - 34 years, Female', u34_female FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 17',  '35 - 39 years, Male',   u39_male   FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 18',  '35 - 39 years, Female', u39_female FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 19',  '40 - 44 years, Male',   u44_male   FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 20',  '40 - 44 years, Female', u44_female FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 21',  '45 - 49 years, Male',   u49_male   FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 22',  '45 - 49 years, Female', u49_female FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 23',  '>= 50 years, Male',     o50_male   FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.1. 24',  '>= 50 years, Female',   o50_female FROM new_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2',      'Known positive',    total      FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 1',   '< 1 year, Male',   u1_male    FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 2',   '< 1 year, Female', u1_female  FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 3',   '1 - 4 years, Male',     u4_male    FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 4',   '1 - 4 years, Female',   u4_female  FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 5',   '5 - 9 years, Male',     u9_male    FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 6',   '5 - 9 years, Female',   u9_female  FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 7',   '10 - 14 years, Male',   u14_male   FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 8',   '10 - 14 years, Female', u14_female FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 9',   '15 - 19 years, Male',   u19_male   FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 10',  '15 - 19 years, Female', u19_female FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 11',  '20 - 24 years, Male',   u24_male   FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 12',  '20 - 24 years, Female', u24_female FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 13',  '25 - 29 years, Male',   u29_male   FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 14',  '25 - 29 years, Female', u29_female FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 15',  '30 - 34 years, Male',   u34_male   FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 16',  '30 - 34 years, Female', u34_female FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 17',  '35 - 39 years, Male',   u39_male   FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 18',  '35 - 39 years, Female', u39_female FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 19',  '40 - 44 years, Male',   u44_male   FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 20',  '40 - 44 years, Female', u44_female FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 21',  '45 - 49 years, Male',   u49_male   FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 22',  '45 - 49 years, Female', u49_female FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 23',  '>= 50 years, Male',     o50_male   FROM known_positive_agg
    UNION ALL SELECT 'HIV_HTS_TST_INDEX_4.2. 24',  '>= 50 years, Female',   o50_female FROM known_positive_agg;
END //

DELIMITER ;
