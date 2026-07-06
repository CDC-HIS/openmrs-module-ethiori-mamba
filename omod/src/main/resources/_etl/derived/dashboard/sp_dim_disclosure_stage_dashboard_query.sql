-- KPI-12 (Disclosure Stage, Pediatric) for the facility KPI dashboard.
--
-- TODO(backend-review): "pediatric TX_Curr" here is `tx_curr_end_date >= REPORT_END_DATE AND
--   age < 15`, matching the canonical definition in sp_dim_tx_curr_datim_query.sql (which
--   requires treatment_end_date >= REPORT_END_DATE).

DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_disclosure_stage_dashboard_query;

CREATE PROCEDURE sp_dim_disclosure_stage_dashboard_query(
    IN REPORT_END_DATE DATE
)
BEGIN
    WITH RankedDisclosure AS (SELECT client_id,
                                      CASE stages_of_disclosure
                                          WHEN 'full_disclosure_stage_3' THEN 3
                                          WHEN 'full_disclosure_stage_2' THEN 2
                                          WHEN 'full_disclosure_stage_1' THEN 1
                                          WHEN 'no_disclosure' THEN 0
                                          ELSE NULL
                                          END AS stage_rank
                               FROM mamba_fact_follow_up
                               WHERE stages_of_disclosure IS NOT NULL
                                 AND follow_up_date_followup_ <= COALESCE(REPORT_END_DATE, CURDATE())),

         HighestPerClient AS (SELECT client_id, MAX(stage_rank) AS highest_stage
                               FROM RankedDisclosure
                               WHERE stage_rank IS NOT NULL
                               GROUP BY client_id),

         PediatricTxCurr AS (SELECT client_id
                              FROM mamba_fact_client_staging
                              WHERE tx_curr_end_date >= COALESCE(REPORT_END_DATE, CURDATE())
                                AND TIMESTAMPDIFF(YEAR, birthdate, COALESCE(REPORT_END_DATE, CURDATE())) < 15)

    SELECT h.highest_stage                                                                                  AS disclosure_stage_rank,
           CASE h.highest_stage
               WHEN 3 THEN 'DS3'
               WHEN 2 THEN 'DS2'
               WHEN 1 THEN 'DS1'
               WHEN 0 THEN 'DS0'
               END                                                                                           AS disclosure_stage_label,
           COUNT(*)                                                                                          AS client_count
    FROM PediatricTxCurr p
             JOIN HighestPerClient h ON p.client_id = h.client_id
    GROUP BY h.highest_stage
    ORDER BY h.highest_stage DESC;
END //

DELIMITER ;
