DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_pep_query;

CREATE PROCEDURE  sp_fact_hmis_hiv_pep_query(IN REPORT_START_DATE DATE,
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

                              where reporting_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
     tx_new as (select *
                from tmp_latest_follow_up
                WHERE
                    row_num = 1
     ),
     pep_agg AS (
         SELECT
             COUNT(*) AS total,
             SUM(CASE WHEN exposure_type = 'Occupational'                                THEN 1 ELSE 0 END) AS occupational,
             SUM(CASE WHEN exposure_type IN ('Sexual violence','Sexual assault')          THEN 1 ELSE 0 END) AS sexual_violence,
             SUM(CASE WHEN exposure_type IN ('Other','Non-occupational')                  THEN 1 ELSE 0 END) AS other_non_occ
         FROM tx_new
     )
SELECT 'HIV_PEP'    AS S_NO, 'Number of persons provided with post-exposure prophylaxis (PEP) for risk of HIV infection by exposure type' AS Activity, total          FROM pep_agg
UNION ALL SELECT 'HIV_PEP. 1', 'Occupational',          occupational     FROM pep_agg
UNION ALL SELECT 'HIV_PEP. 2', 'Sexual violence',       sexual_violence  FROM pep_agg
UNION ALL SELECT 'HIV_PEP. 3', 'Other Non occupational', other_non_occ  FROM pep_agg;
END //

DELIMITER ;