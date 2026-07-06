-- KPI-05 (PEP) for the facility KPI dashboard.

DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_pep_dashboard_query;

CREATE PROCEDURE sp_dim_pep_dashboard_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
    WITH post_information AS (SELECT client_id, exposure_type, reporting_date
                               FROM mamba_flat_encounter_exposed_person_information),

         ever_latest AS (SELECT client_id, exposure_type
                          FROM (SELECT client_id,
                                       exposure_type,
                                       ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY reporting_date DESC) AS rn
                                FROM post_information) ranked
                          WHERE rn = 1),

         current_latest AS (SELECT client_id, exposure_type
                             FROM (SELECT client_id,
                                          exposure_type,
                                          ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY reporting_date DESC) AS rn
                                   FROM post_information
                                   WHERE reporting_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE) ranked
                             WHERE rn = 1),

         ever_counts AS (SELECT exposure_type, COUNT(*) AS ever_enrolled
                          FROM ever_latest
                          GROUP BY exposure_type),

         current_counts AS (SELECT exposure_type, COUNT(*) AS current_this_month
                             FROM current_latest
                             GROUP BY exposure_type),

         -- FULL OUTER JOIN emulation: every exposure_type from either side, missing counts as 0.
         combined AS (SELECT e.exposure_type                    AS exposure_type,
                              e.ever_enrolled                    AS ever_enrolled,
                              COALESCE(c.current_this_month, 0)  AS current_this_month
                       FROM ever_counts e
                                LEFT JOIN current_counts c ON e.exposure_type = c.exposure_type
                       UNION
                       SELECT c.exposure_type                    AS exposure_type,
                              COALESCE(e.ever_enrolled, 0)        AS ever_enrolled,
                              c.current_this_month                AS current_this_month
                       FROM current_counts c
                                LEFT JOIN ever_counts e ON e.exposure_type = c.exposure_type)

    SELECT exposure_type,
           SUM(ever_enrolled)        AS ever_enrolled,
           SUM(current_this_month)   AS current_this_month
    FROM combined
    GROUP BY exposure_type WITH ROLLUP;
END //

DELIMITER ;
