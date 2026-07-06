-- KPI-03 (PMTCT, maternal half) for the facility KPI dashboard.

DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_pmtct_maternal_dashboard_query;

CREATE PROCEDURE sp_dim_pmtct_maternal_dashboard_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
    WITH Enrollment AS (SELECT client_id,
                               encounter_id,
                               date_of_enrollment_or_booking,
                               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY date_of_enrollment_or_booking, encounter_id) AS row_num
                        FROM mamba_flat_encounter_pmtct_enrollment
                        WHERE date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE),

         Discharge AS (SELECT *,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY discharge_date, encounter_id) AS row_num
                       FROM mamba_flat_encounter_pmtct_discharge
                       WHERE discharge_date > REPORT_START_DATE),

         Episode_Window AS (SELECT e.client_id,
                                    e.date_of_enrollment_or_booking AS start_date,
                                    d.discharge_date
                             FROM Enrollment e
                                      LEFT JOIN Discharge d
                                                ON e.client_id = d.client_id
                                                    AND e.row_num = d.row_num
                                                    AND d.discharge_date > e.date_of_enrollment_or_booking)

    SELECT COUNT(*)                     AS all_enrolled,
           SUM(discharge_date IS NULL)  AS currently_active
    FROM Episode_Window;
END //

DELIMITER ;
