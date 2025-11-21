DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_mtct_query;

CREATE PROCEDURE sp_fact_hmis_mtct_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH Enrollment_tmp as (select client_id,
                                   antenatal_care_provider,
                                   ld_client,
                                   post_natal_care,
                                   art_clinic,
                                   location_of_birth,
                                   ROW_NUMBER() over (PARTITION BY client_id ORDER BY date_of_enrollment_or_booking DESC, encounter_id DESC) as row_num
                            from mamba_flat_encounter_pmtct_enrollment
                            where date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         Enrollment as (select * from Enrollment_tmp where row_num = 1),
         tmp_hei_test as (select hiv_test.client_id,
                                 hiv_test_date,
                                 date_of_birth,
                                 hiv_test_result,
                                 TIMESTAMPDIFF(MONTH, date_of_birth, dna_pcr_sample_collection_date)                                                                as age_in_months,
                                 ROW_NUMBER() over (PARTITION BY hiv_test.client_id ORDER BY dna_pcr_sample_collection_date DESC, encounter_id DESC) as row_num
                          from mamba_flat_encounter_hei_hiv_test hiv_test
                                   join mamba_dim_client client on hiv_test.client_id = client.client_id
                          where test_round = 'Initial test'
                            and dna_pcr_sample_collection_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         hei_test as ( select * from tmp_hei_test where row_num=1),
         
         -- New CTE for Confirmatory Test
         tmp_hei_confirmatory as (select hiv_test.client_id,
                                         hiv_test_date,
                                         date_of_birth,
                                         hiv_test_result,
                                         TIMESTAMPDIFF(MONTH, date_of_birth, hiv_test_date) as age_in_months,
                                         ROW_NUMBER() over (PARTITION BY hiv_test.client_id ORDER BY hiv_test_date DESC, encounter_id DESC) as row_num
                                  from mamba_flat_encounter_hei_hiv_test hiv_test
                                           join mamba_dim_client client on hiv_test.client_id = client.client_id
                                  where test_round LIKE '%Confirmatory%'
                                    and hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         hei_confirmatory as (select * from tmp_hei_confirmatory where row_num=1),

         -- New CTE for CPT from Followup
         tmp_hei_cpt as (select f.client_id,
                                f.encounter_datetime as cpt_date,
                                c.date_of_birth,
                                DATEDIFF(f.encounter_datetime, c.date_of_birth) as age_days_at_cpt,
                                ROW_NUMBER() over (PARTITION BY f.client_id ORDER BY f.encounter_datetime ASC) as row_num
                         from mamba_flat_encounter_hei_followup f
                                  join mamba_dim_client c on f.client_id = c.client_id
                         where f.cotrimoxazole_prophylaxis_dose is not null
                           and f.encounter_datetime BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         hei_cpt as (select * from tmp_hei_cpt where row_num=1),

         tmp_hei_enrollment as (
             SELECT
                 e.client_id,
                 c.date_of_birth,
                 e.art_antiretroviral_start_date,
                 e.arv_prophylaxis,
                 ROW_NUMBER() over (PARTITION BY e.client_id ORDER BY e.encounter_datetime DESC) as row_num
             FROM mamba_flat_encounter_hei_enrollment e 
                       JOIN mamba_dim_client c ON e.client_id = c.client_id
             WHERE e.encounter_datetime <= REPORT_END_DATE
         ),
         hei_enrollment as (
             SELECT * FROM tmp_hei_enrollment WHERE row_num = 1
         )

-- Percentage of HIV-positive pregnant women who received ART to reduce the risk of mother-to child-transmission (MTCT) during pregnancy, L&D and PNC
    SELECT 'MTCT_ART'                                                                                                                                           AS S_NO,
           'Percentage of HIV-positive pregnant women who received ART to reduce the risk of mother-to child-transmission (MTCT) during pregnancy, L&D and PNC' as Activity,
           COUNT(*)                                                                                                                                             as Value
    FROM Enrollment
    where row_num = 1
-- Number of HIV positive women who received ART to reduce the risk of mother to child transmission during ANC for the first time
    UNION ALL
    SELECT 'MTCT_ART.1.'                                                                                                                    AS S_NO,
           'Number of HIV positive women who received ART to reduce the risk of mother to child transmission during ANC for the first time' as Activity,
           COUNT(*)                                                                                                                         as Value
    FROM Enrollment
    where antenatal_care_provider is not null
-- Number of HIV positive Pregnant women who received ART to reduce the risk of mother to child transmission during L&D for the first time
    UNION ALL
    SELECT 'MTCT_ART.2.'                                                                                                                             AS S_NO,
           'Number of HIV positive Pregnant women who received ART to reduce the risk of mother to child transmission during L&D for the first time' as Activity,
           COUNT(*)                                                                                                                                  as Value
    FROM Enrollment
    where ld_client is not null
-- Number of HIV positive lactating women who received ART to reduce the risk of mother to child transmission during PNC for the first time
    UNION ALL
    SELECT 'MTCT_ART.3.'                                                                                                                              AS S_NO,
           'Number of HIV positive lactating women who received ART to reduce the risk of mother to child transmission during PNC for the first time' as Activity,
           COUNT(*)                                                                                                                                   as Value
    FROM Enrollment
    where post_natal_care is not null
-- Number of HIV-positive women who get pregnant while on ART and linked to ANC
    UNION ALL
    SELECT 'MTCT_ART.4.'                                                                  AS S_NO,
           'Number of HIV-positive women who get pregnant while on ART and linked to ANC' as Activity,
           COUNT(*)                                                                       as Value
    FROM Enrollment
    where art_clinic is not null
-- Percentage of  HIV exposed infants who received a virologic HIV test (sample collected) within 12 month
    UNION ALL
    SELECT 'MTCT_HEI_EID.'                                                                                           AS S_NO,
           'Percentage of  HIV exposed infants who received a virologic HIV test (sample collected) within 12 month' as Activity,
           COUNT(*)                                                                                                  as Value
    FROM hei_test
-- Percentage of  HIV exposed infants who received a virologic HIV test (sample collected) within 12 month
    UNION ALL
    SELECT 'MTCT_HEI_EID.1'                                                                                          AS S_NO,
           'Number of HIV exposed infants who received a virologic HIV test (sample collected) 0- 2 months of birth' as Activity,
           COUNT(*)                                                                                                  as Value
    FROM hei_test
    where age_in_months BETWEEN 0 AND 2
-- Number of HIV exposed infants who received a virologic HIV test (sample collected) 2-12 months of birth
    UNION ALL
    SELECT 'MTCT_HEI_EID.2'                                                                                          AS S_NO,
           'Number of HIV exposed infants who received a virologic HIV test (sample collected) 2-12 months of birth' as Activity,
           COUNT(*)                                                                                                  as Value
    FROM hei_test
    where age_in_months BETWEEN 2 AND 12
-- Total Number of infants within 12 month received virological test result
    UNION ALL
    SELECT 'MTCT_HEI_EID.3'                                                           AS S_NO,
           'Total Number of infants within 12 month received virological test result' as Activity,
           COUNT(*)                                                                   as Value
    FROM hei_test
    where hiv_test_result is not null
-- Number of HIV exposed infants who received an HIV test 0- 2 months of birth
    UNION ALL
    SELECT 'MTCT_HEI_EID.1.1'                                                            AS S_NO,
           'Number of HIV exposed infants who received an HIV test 0- 2 months of birth' as Activity,
           COUNT(*)                                                                      as Value
    FROM hei_test
    where hiv_test_result is not null
      and age_in_months BETWEEN 0 AND 2
-- Positive
    UNION ALL
    SELECT 'MTCT_HEI_EID.1.1. 1' AS S_NO,
           'Positive'            as Activity,
           COUNT(*)              as Value
    FROM hei_test
    where hiv_test_result is not null
      and age_in_months BETWEEN 0 AND 2
      and hiv_test_result = 'Positive'
-- Negative
    UNION ALL
    SELECT 'MTCT_HEI_EID.1.1. 2' AS S_NO,
           'Negative'            as Activity,
           COUNT(*)              as Value
    FROM hei_test
    where hiv_test_result is not null
      and age_in_months BETWEEN 0 AND 2
      and hiv_test_result = 'Negative'
-- Number of HIV exposed infants who received an HIV test 2-12 months of birth
    UNION ALL
    SELECT 'MTCT_HEI_EID.1.2'                                                            AS S_NO,
           'Number of HIV exposed infants who received an HIV test 2-12 months of birth' as Activity,
           COUNT(*)                                                                      as Value
    FROM hei_test
    where hiv_test_result is not null
      and age_in_months BETWEEN 2 AND 12
-- Positive
    UNION ALL
    SELECT 'MTCT_HEI_EID.1.2. 1' AS S_NO,
           'Positive'            as Activity,
           COUNT(*)              as Value
    FROM hei_test
    where hiv_test_result is not null
      and age_in_months BETWEEN 2 AND 12
      and hiv_test_result = 'Positive'
-- Negative
    UNION ALL
    SELECT 'MTCT_HEI_EID.1.2. 2' AS S_NO,
           'Negative'            as Activity,
           COUNT(*)              as Value
    FROM hei_test
    where hiv_test_result is not null
      and age_in_months BETWEEN 2 AND 12
      and hiv_test_result = 'Negative'
-- Percentage of exposed Infants born to HIV positive women who were started on co-trimoxazole prophylaxis within two months of birth
    UNION ALL
    SELECT 'MTCT_HEI_COTR'                                                                                                                      AS S_NO,
           'Percentage of exposed Infants born to HIV positive women who were started on co-trimoxazole prophylaxis within two months of birth' as Activity,
           COUNT(*)                                                                                                                             as Value
    FROM hei_cpt
    where age_days_at_cpt <= 60
-- Number of infants born to HIV positive women started on co-trimoxazole prophylaxis within two months of birth
    UNION ALL
    SELECT 'MTCT_HEI_COTR.1.'                                                                                              AS S_NO,
           'Number of infants born to HIV positive women started on co-trimoxazole prophylaxis within two months of birth' as Activity,
           COUNT(*)                                                                                                        as Value
    FROM hei_cpt
    where age_days_at_cpt <= 60
-- Percentage of Infants born to HIV-infected women receiving antiretroviral (ARV) prophylaxis for prevention of Women-to-child transmission (PMTCT)
    UNION ALL
    SELECT 'RMH_PMTCT_IARV'                                                                                                                                    AS S_NO,
           'Percentage of Infants born to HIV-infected women receiving antiretroviral (ARV) prophylaxis for prevention of Women-to-child transmission (PMTCT)' as Activity,
           COUNT(*)                                                                                                                                            as Value
    FROM hei_enrollment
    where arv_prophylaxis is not null
-- Number of HIV exposed infants who received ARV prophylaxis For 12 weeks
    UNION ALL
    SELECT 'MTCT_HEI_ARV.1.'                                                         AS S_NO,
           'Number of HIV exposed infants who received ARV prophylaxis For 12 weeks' as Activity,
           COUNT(*)                                                                  as Value
    FROM hei_enrollment
    where arv_prophylaxis is not null
-- Number of HIV positive women who gave birth at health institution
    UNION ALL
    SELECT 'MTCT_HEI_ARV.2.'                                                   AS S_NO,
           'Number of HIV positive women who gave birth at health institution' as Activity,
           COUNT(*)                                                            as Value
    FROM Enrollment
    where location_of_birth is not null
-- Percentage of HIV exposed infants receiving HIV confirmatory (antibody test) test by 18 months
    UNION ALL
    SELECT 'MTCT_HEI_ABTST'                                                                                 AS S_NO,
           'Percentage of HIV exposed infants receiving HIV confirmatory (antibody test) test by 18 months' as Activity,
           COUNT(*)                                                                                         as Value
    FROM hei_confirmatory
    where age_in_months <= 18
-- Number of HIV exposed infants receiving HIV confirmatory (antibody test) by 18 months
    UNION ALL
    SELECT 'MTCT_HEI_ABTST.1'                                                                      AS S_NO,
           'Number of HIV exposed infants receiving HIV confirmatory (antibody test) by 18 months' as Activity,
           COUNT(*)                                                                                as Value
    FROM hei_confirmatory
    where age_in_months <= 18
-- Positive
    UNION ALL
    SELECT 'MTCT_HEI_ABTST.1. 1' AS S_NO,
           'Positive'            as Activity,
           COUNT(*)              as Value
    FROM hei_confirmatory
    where age_in_months <= 18
      and hiv_test_result = 'Positive'
-- Negative
    UNION ALL
    SELECT 'MTCT_HEI_ABTST.1. 2' AS S_NO,
           'Negative'            as Activity,
           COUNT(*)              as Value
    FROM hei_confirmatory
    where age_in_months <= 18
      and hiv_test_result = 'Negative';
END //

DELIMITER ;