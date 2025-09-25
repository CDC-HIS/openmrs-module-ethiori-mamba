DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_mtct_query;

CREATE PROCEDURE sp_fact_hmis_mtct_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    #Known positive
    WITH Enrollment_tmp as (select client_id,
                                   antenatal_care_provider,
                                   ld_client,
                                   post_natal_care,
                                   art_clinic,
                                   ROW_NUMBER() over (PARTITION BY client_id ORDER BY date_of_enrollment_or_booking DESC, encounter_id DESC) as row_num
                            from mamba_flat_encounter_pmtct_enrollment
                            where date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         Enrollment as (select * from Enrollment_tmp where row_num = 1),
         hei_test as (select hiv_test.client_id,
                             hiv_test_date,
                             date_of_birth,
                             hiv_test_result,
                             TIMESTAMPDIFF(MONTH, date_of_birth, REPORT_END_DATE) as age_in_months
                      from mamba_flat_encounter_hei_dna_pcr_test hiv_test
                               left join mamba_flat_encounter_hei_dna_pcr_test_1 hiv_test_1
                                         on hiv_test.encounter_id = hiv_test_1.encounter_id
                               join mamba_dim_client client on hiv_test.client_id = client.client_id
                      where test_round = 'Initial test'
                        and specimen_collection_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
        hei_follow_up as (
            select client_id

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
    SELECT 'MTCT_HEI_EID.1'                                                           AS S_NO,
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
    where hiv_test_result is not null and age_in_months BETWEEN 0 AND 2
-- Positive
    UNION ALL
    SELECT 'MTCT_HEI_EID.1.1. 1' AS S_NO,
           'Positive'            as Activity,
           COUNT(*)              as Value
    FROM hei_test
    where hiv_test_result is not null and age_in_months BETWEEN 0 AND 2 and hiv_test_result = 'Positive'
-- Negative
    UNION ALL
    SELECT 'MTCT_HEI_EID.1.1. 2' AS S_NO,
           'Negative'            as Activity,
           COUNT(*)              as Value
    FROM hei_test
    where hiv_test_result is not null and age_in_months BETWEEN 0 AND 2 and hiv_test_result = 'Negative'
-- Number of HIV exposed infants who received an HIV test 2-12 months of birth
    UNION ALL
    SELECT 'MTCT_HEI_EID.1.2'                                                            AS S_NO,
           'Number of HIV exposed infants who received an HIV test 2-12 months of birth' as Activity,
           COUNT(*)                                                                      as Value
    FROM hei_test
    where hiv_test_result is not null and age_in_months BETWEEN 2 AND 12
-- Positive
    UNION ALL
    SELECT 'MTCT_HEI_EID.1.2. 1' AS S_NO,
           'Positive'            as Activity,
           COUNT(*)              as Value
    FROM hei_test
    where hiv_test_result is not null and age_in_months BETWEEN 2 AND 12 and hiv_test_result = 'Positive'
-- Negative
    UNION ALL
    SELECT 'MTCT_HEI_EID.1.2. 2' AS S_NO,
           'Negative'            as Activity,
           COUNT(*)              as Value
    FROM hei_test
    where hiv_test_result is not null and age_in_months BETWEEN 2 AND 12 and hiv_test_result = 'Negative'
-- Percentage of exposed Infants born to HIV positive women who were started on co-trimoxazole prophylaxis within two months of birth
    UNION ALL
    SELECT 'MTCT_HEI_COTR'                                                                                                                      AS S_NO,
           'Percentage of exposed Infants born to HIV positive women who were started on co-trimoxazole prophylaxis within two months of birth' as Activity,
           COUNT(*)                                                                                                                             as Value
    FROM Enrollment
-- Number of infants born to HIV positive women started on co-trimoxazole prophylaxis within two months of birth
    UNION ALL
    SELECT 'MTCT_HEI_COTR.1.'                                                                                              AS S_NO,
           'Number of infants born to HIV positive women started on co-trimoxazole prophylaxis within two months of birth' as Activity,
           COUNT(*)                                                                                                        as Value
    FROM Enrollment
-- Percentage of Infants born to HIV-infected women receiving antiretroviral (ARV) prophylaxis for prevention of Women-to-child transmission (PMTCT)
    UNION ALL
    SELECT 'RMH_PMTCT_IARV'                                                                                                                                    AS S_NO,
           'Percentage of Infants born to HIV-infected women receiving antiretroviral (ARV) prophylaxis for prevention of Women-to-child transmission (PMTCT)' as Activity,
           COUNT(*)                                                                                                                                            as Value
    FROM Enrollment
-- Number of HIV exposed infants who received ARV prophylaxis For 12 weeks
    UNION ALL
    SELECT 'MTCT_HEI_ARV.1.'                                                         AS S_NO,
           'Number of HIV exposed infants who received ARV prophylaxis For 12 weeks' as Activity,
           COUNT(*)                                                                  as Value
    FROM Enrollment
-- Number of HIV positive women who gave birth at health institution
    UNION ALL
    SELECT 'MTCT_HEI_ARV.2.'                                                   AS S_NO,
           'Number of HIV positive women who gave birth at health institution' as Activity,
           COUNT(*)                                                            as Value
    FROM Enrollment
-- Percentage of HIV exposed infants receiving HIV confirmatory (antibody test) test by 18 months
    UNION ALL
    SELECT 'MTCT_HEI_ABTST'                                                                                 AS S_NO,
           'Percentage of HIV exposed infants receiving HIV confirmatory (antibody test) test by 18 months' as Activity,
           COUNT(*)                                                                                         as Value
    FROM Enrollment
-- Number of HIV exposed infants receiving HIV confirmatory (antibody test) by 18 months
    UNION ALL
    SELECT 'MTCT_HEI_ABTST.1'                                                                      AS S_NO,
           'Number of HIV exposed infants receiving HIV confirmatory (antibody test) by 18 months' as Activity,
           COUNT(*)                                                                                as Value
    FROM Enrollment
-- Positive
    UNION ALL
    SELECT 'MTCT_HEI_ABTST.1. 1' AS S_NO,
           'Positive'            as Activity,
           COUNT(*)              as Value
    FROM Enrollment
-- Negative
    UNION ALL
    SELECT 'MTCT_HEI_ABTST.1. 2' AS S_NO,
           'Negative'            as Activity,
           COUNT(*)              as Value
    FROM Enrollment;
END //

DELIMITER ;