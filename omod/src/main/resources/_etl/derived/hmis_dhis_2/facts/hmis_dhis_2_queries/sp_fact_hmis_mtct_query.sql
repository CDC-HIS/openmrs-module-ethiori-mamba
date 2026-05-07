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
         delivery_tmp as (select client_id,
                                   antenatal_care_provider,
                                   ld_client,
                                   post_natal_care,
                                   art_clinic,
                                   location_of_birth,
                                   ROW_NUMBER() over (PARTITION BY client_id ORDER BY date_of_delivery DESC, encounter_id DESC) as row_num
                            from mamba_flat_encounter_pmtct_enrollment
                            where date_of_delivery BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         Delivery as (select * from delivery_tmp where row_num = 1),
         tmp_hei_test as (select hiv_test.client_id,
                                 hiv_test_date,
                                 date_of_birth,
                                 hiv_test_result,
                                 TIMESTAMPDIFF(MONTH, date_of_birth, dna_pcr_sample_collection_date)                                                 as age_in_months,
                                 ROW_NUMBER() over (PARTITION BY hiv_test.client_id ORDER BY dna_pcr_sample_collection_date DESC, encounter_id DESC) as row_num
                          from mamba_flat_encounter_hei_hiv_test hiv_test
                                   join mamba_dim_client client on hiv_test.client_id = client.client_id
                          where test_type = 'HIV DNA polymerase chain reaction, dried blood spot (DBS)' and test_round = 'Initial test'
                            and dna_pcr_sample_collection_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         hei_test as (select * from tmp_hei_test where row_num = 1),
         tmp_hei_confirmatory as (select hiv_test.client_id,
                                         hiv_test_date,
                                         date_of_birth,
                                         hiv_test_result,
                                         TIMESTAMPDIFF(MONTH, date_of_birth, followup_date_followup_1)                                                 as age_in_months,
                                         ROW_NUMBER() over (PARTITION BY hiv_test.client_id ORDER BY followup_date_followup_1 DESC, encounter_id DESC) as row_num
                                  from mamba_flat_encounter_hei_hiv_test hiv_test
                                           join mamba_dim_client client on hiv_test.client_id = client.client_id
                                  where test_type = 'Rapid test for HIV'
                                    and followup_date_followup_1 BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         hei_confirmatory as (select * from tmp_hei_confirmatory where row_num = 1),
         tmp_hei_cpt as (select f.client_id,
                                f.followup_date_followup                                                             as cpt_date,
                                c.date_of_birth,
                                arv_prophylaxis,
                                DATEDIFF(f.followup_date_followup, c.date_of_birth)                                  as age_days_at_cpt,
                                ROW_NUMBER() over (PARTITION BY f.client_id ORDER BY f.followup_date_followup, f.encounter_id ) as row_num
                         from mamba_flat_encounter_hei_followup f
                             left join mamba_flat_encounter_hei_followup_1 f1 on f.encounter_id = f1.encounter_id
                                  join mamba_dim_client c on f.client_id = c.client_id
                         where
                              f1.cotrimoxazole_prophylaxis_dose is not null                 and
                             f.followup_date_followup BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         hei_cpt as (select * from tmp_hei_cpt where row_num = 1),
         tmp_hei_enrollment as (SELECT e.client_id,
                                       c.date_of_birth,
                                       e.art_antiretroviral_start_date,
                                       e.arv_prophylaxis,
                                       ROW_NUMBER() over (PARTITION BY e.client_id ORDER BY e.date_enrolled_in_care DESC) as row_num
                                FROM mamba_flat_encounter_hei_enrollment e
                                         JOIN mamba_dim_client c ON e.client_id = c.client_id
                                WHERE e.date_enrolled_in_care BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         hei_enrollment as (SELECT * FROM tmp_hei_enrollment WHERE row_num = 1),
         enrollment_agg AS (
             SELECT COUNT(*)                                                   AS total,
                    SUM(CASE WHEN antenatal_care_provider IS NOT NULL THEN 1 ELSE 0 END) AS anc_count,
                    SUM(CASE WHEN ld_client IS NOT NULL THEN 1 ELSE 0 END)               AS ld_count,
                    SUM(CASE WHEN post_natal_care IS NOT NULL THEN 1 ELSE 0 END)         AS pnc_count,
                    SUM(CASE WHEN art_clinic IS NOT NULL THEN 1 ELSE 0 END)              AS art_count
             FROM Enrollment
             WHERE row_num = 1
         ),
         hei_test_agg AS (
             SELECT SUM(CASE WHEN age_in_months BETWEEN 0 AND 2 THEN 1 ELSE 0 END)                                                  AS in_0_2,
                    SUM(CASE WHEN age_in_months > 2 AND age_in_months <= 12 THEN 1 ELSE 0 END)                                       AS in_2_12,
                    SUM(CASE WHEN hiv_test_result IS NOT NULL THEN 1 ELSE 0 END)                                                     AS total_with_result,
                    SUM(CASE WHEN hiv_test_result IS NOT NULL AND age_in_months BETWEEN 0 AND 2 THEN 1 ELSE 0 END)                   AS in_0_2_tested,
                    SUM(CASE WHEN hiv_test_result = 'Positive' AND age_in_months BETWEEN 0 AND 2 THEN 1 ELSE 0 END)                 AS in_0_2_positive,
                    SUM(CASE WHEN hiv_test_result = 'Negative' AND age_in_months BETWEEN 0 AND 2 THEN 1 ELSE 0 END)                 AS in_0_2_negative,
                    SUM(CASE WHEN hiv_test_result IS NOT NULL AND age_in_months > 2 AND age_in_months <= 12 THEN 1 ELSE 0 END)       AS in_2_12_tested,
                    SUM(CASE WHEN hiv_test_result = 'Positive' AND age_in_months > 2 AND age_in_months <= 12 THEN 1 ELSE 0 END)     AS in_2_12_positive,
                    SUM(CASE WHEN hiv_test_result = 'Negative' AND age_in_months > 2 AND age_in_months <= 12 THEN 1 ELSE 0 END)     AS in_2_12_negative
             FROM hei_test
         ),
         hei_confirmatory_agg AS (
             SELECT COUNT(*)                                                                    AS total,
                    SUM(CASE WHEN hiv_test_result = 'Positive' THEN 1 ELSE 0 END)              AS positive,
                    SUM(CASE WHEN hiv_test_result = 'Negative' THEN 1 ELSE 0 END)              AS negative
             FROM hei_confirmatory
         )

    SELECT 'MTCT_ART'                                                                                                                                           AS S_NO,
           'Percentage of HIV-positive pregnant women who received ART to reduce the risk of mother-to child-transmission (MTCT) during pregnancy, L&D and PNC' as Activity,
           total                                                                                                                                                 as Value
    FROM enrollment_agg
    UNION ALL SELECT 'MTCT_ART.1.', 'Number of HIV positive women who received ART to reduce the risk of mother to child transmission during ANC for the first time',        COALESCE(anc_count,0) FROM enrollment_agg
    UNION ALL SELECT 'MTCT_ART.2.', 'Number of HIV positive Pregnant women who received ART to reduce the risk of mother to child transmission during L&D for the first time', COALESCE(ld_count,0)  FROM enrollment_agg
    UNION ALL SELECT 'MTCT_ART.3.', 'Number of HIV positive lactating women who received ART to reduce the risk of mother to child transmission during PNC for the first time', COALESCE(pnc_count,0) FROM enrollment_agg
    UNION ALL SELECT 'MTCT_ART.4.', 'Number of HIV-positive women who get pregnant while on ART and linked to ANC',                                                             COALESCE(art_count,0) FROM enrollment_agg
    UNION ALL SELECT 'MTCT_HEI_EID.',   'Percentage of  HIV exposed infants who received a virologic HIV test (sample collected) within 12 month',              COALESCE(in_0_2,0)        FROM hei_test_agg
    UNION ALL SELECT 'MTCT_HEI_EID.1',  'Number of HIV exposed infants who received a virologic HIV test (sample collected) 0- 2 months of birth',             COALESCE(in_0_2,0)        FROM hei_test_agg
    UNION ALL SELECT 'MTCT_HEI_EID.2',  'Number of HIV exposed infants who received a virologic HIV test (sample collected) 2-12 months of birth',             COALESCE(in_2_12,0)       FROM hei_test_agg
    UNION ALL SELECT 'MTCT_HEI_EID.3',  'Total Number of infants within 12 month received virological test result',                                             COALESCE(total_with_result,0) FROM hei_test_agg
    UNION ALL SELECT 'MTCT_HEI_EID.1.1',   'Number of HIV exposed infants who received an HIV test 0- 2 months of birth',                                      COALESCE(in_0_2_tested,0)    FROM hei_test_agg
    UNION ALL SELECT 'MTCT_HEI_EID.1.1. 1', 'Positive',                                                                                                        COALESCE(in_0_2_positive,0)  FROM hei_test_agg
    UNION ALL SELECT 'MTCT_HEI_EID.1.1. 2', 'Negative',                                                                                                        COALESCE(in_0_2_negative,0)  FROM hei_test_agg
    UNION ALL SELECT 'MTCT_HEI_EID.1.2',   'Number of HIV exposed infants who received an HIV test 2-12 months of birth',                                      COALESCE(in_2_12_tested,0)   FROM hei_test_agg
    UNION ALL SELECT 'MTCT_HEI_EID.1.2. 1', 'Positive',                                                                                                        COALESCE(in_2_12_positive,0) FROM hei_test_agg
    UNION ALL SELECT 'MTCT_HEI_EID.1.2. 2', 'Negative',                                                                                                        COALESCE(in_2_12_negative,0) FROM hei_test_agg
    UNION ALL
    SELECT 'MTCT_HEI_COTR'                                                                                                                      AS S_NO,
           'Percentage of exposed Infants born to HIV positive women who were started on co-trimoxazole prophylaxis within two months of birth' as Activity,
           COUNT(*)                                                                                                                             as Value
    FROM hei_cpt
    where age_days_at_cpt <= 60
    UNION ALL
    SELECT 'MTCT_HEI_COTR.1.'                                                                                              AS S_NO,
           'Number of infants born to HIV positive women started on co-trimoxazole prophylaxis within two months of birth' as Activity,
           COUNT(*)                                                                                                        as Value
    FROM hei_cpt
    where age_days_at_cpt <= 60
    UNION ALL
    SELECT 'RMH_PMTCT_IARV'                                                                                                                                    AS S_NO,
           'Percentage of Infants born to HIV-infected women receiving antiretroviral (ARV) prophylaxis for prevention of Women-to-child transmission (PMTCT)' as Activity,
           COUNT(*)                                                                                                                                            as Value
    FROM hei_enrollment
    where arv_prophylaxis is not null
    UNION ALL
    SELECT 'MTCT_HEI_ARV.1.'                                                         AS S_NO,
           'Number of HIV exposed infants who received ARV prophylaxis For 12 weeks' as Activity,
           COUNT(*)                                                                  as Value
    FROM hei_enrollment
    where arv_prophylaxis is not null
    UNION ALL
    SELECT 'MTCT_HEI_ARV.2.'                                                   AS S_NO,
           'Number of HIV positive women who gave birth at health institution' as Activity,
           COUNT(*)                                                            as Value
    FROM Delivery
    where location_of_birth != 'Home Delivery'
    UNION ALL SELECT 'MTCT_HEI_ABTST',    'Percentage of HIV exposed infants receiving HIV confirmatory (antibody test) test by 18 months', COALESCE(total,0)    FROM hei_confirmatory_agg
    UNION ALL SELECT 'MTCT_HEI_ABTST.1',  'Number of HIV exposed infants receiving HIV confirmatory (antibody test) by 18 months',          COALESCE(total,0)    FROM hei_confirmatory_agg
    UNION ALL SELECT 'MTCT_HEI_ABTST.1. 1', 'Positive',                                                                                    COALESCE(positive,0) FROM hei_confirmatory_agg
    UNION ALL SELECT 'MTCT_HEI_ABTST.1. 2', 'Negative',                                                                                    COALESCE(negative,0) FROM hei_confirmatory_agg;
END //

DELIMITER ;
