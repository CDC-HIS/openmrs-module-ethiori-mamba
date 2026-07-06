-- KPI-03 (PMTCT, HIV-exposed infant half) for the facility KPI dashboard.

DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_pmtct_hei_status_dashboard_query;

CREATE PROCEDURE sp_dim_pmtct_hei_status_dashboard_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
    WITH Enrollment AS (SELECT client_id
                         FROM mamba_flat_encounter_hei_enrollment
                         WHERE date_enrolled_in_care BETWEEN REPORT_START_DATE AND REPORT_END_DATE),

         FinalOutcome AS (SELECT client_id,
                                  ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY encounter_datetime DESC) AS rn
                           FROM mamba_flat_encounter_hei_final_outcome
                           WHERE encounter_datetime <= REPORT_END_DATE)

    SELECT COUNT(*)                    AS all_hei,
           SUM(fo.client_id IS NULL)   AS currently_active
    FROM Enrollment e
             LEFT JOIN FinalOutcome fo ON e.client_id = fo.client_id AND fo.rn = 1;
END //

DELIMITER ;
