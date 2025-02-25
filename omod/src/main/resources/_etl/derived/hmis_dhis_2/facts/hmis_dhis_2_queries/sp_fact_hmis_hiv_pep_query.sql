DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_prep_query;

CREATE PROCEDURE  sp_fact_hmis_hiv_pep_query(
    IN REPORT_END_DATE DATE
)
BEGIN
WITH PostExposure AS (select client_id,
                             encounter_id,
                             reporting_date,
                             exposure_type,
                             eligible,
                             postexposure_prophylaxis_regimen
                      FROM mamba_flat_encounter_exposed_person_information
),
     tmp_latest_follow_up as (select client_id,
                                     encounter_id,
                                     reporting_date,
                                     exposure_type,
                                     eligible,
                                     postexposure_prophylaxis_regimen,
                                     ROW_NUMBER() over (PARTITION BY client_id ORDER BY reporting_date DESC, encounter_id DESC) as row_num
                              from PostExposure

                              where reporting_date <= REPORT_END_DATE),
     tx_new as (select *
                from tmp_latest_follow_up
                WHERE
                    row_num = 1
     )

-- Number of persons provided with post-exposure prophylaxis (PEP) for risk of HIV infection by exposure type
SELECT 'HIV_PEP'                                                                       AS S_NO,
       'Number of persons provided with post-exposure prophylaxis (PEP) for risk of HIV infection by exposure type' as Activity,
       COUNT(*)                                                                                as Value
FROM tx_new

UNION ALL
SELECT 'HIV_PEP1'                                                                       AS S_NO,
       'Occupational' as Activity,
       COUNT(*)                                                                                as Value
FROM tx_new
where exposure_type='Occupational'
UNION ALL
SELECT 'HIV_PEP2'                                                                       AS S_NO,
       'Sexual violence' as Activity,
       COUNT(*)                                                                                as Value
FROM tx_new
where exposure_type='Sexual violence'
UNION ALL
SELECT 'HIV_PEP3'                                                                       AS S_NO,
       'Other Non occupational' as Activity,
       COUNT(*)                                                                                as Value
FROM tx_new
where exposure_type='Other';
END //

DELIMITER ;