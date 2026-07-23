-- KPI-06 (ART Retention) for the facility KPI dashboard.
-- TODO(backend-review): unlike the source SP, this does NOT exclude currently-"Transferred out"
--   clients from the base cohort -- they're intentionally kept so they can be counted in
--   to_count and subtracted via net_cohort_denominator, per the "original + TI - TO" definition.
DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_art_retention_dashboard_query;

CREATE PROCEDURE sp_dim_art_retention_dashboard_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
    WITH FollowUp AS (SELECT follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_date_followup_      AS follow_up_date,
                             art_antiretroviral_start_date AS art_start_date,
                             CASE follow_up_status
                                 WHEN 'Alive' THEN 'Alive on ART'
                                 WHEN 'Restart medication' THEN 'Restart'
                                 WHEN 'Transferred out' THEN 'TO'
                                 WHEN 'Stop all' THEN 'Stop'
                                 WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
                                 WHEN 'Ran away' THEN 'Drop'
                                 END                       AS follow_up_status
                      FROM mamba_fact_follow_up follow_up),

         latest_follow_up AS (SELECT client_id, follow_up_status
                               FROM (SELECT client_id,
                                            follow_up_status,
                                            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                     FROM FollowUp
                                     WHERE follow_up_status IS NOT NULL
                                       AND art_start_date IS NOT NULL
                                       AND follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())
                                     ) ranked
                               WHERE row_num = 1),

         milestones AS (SELECT 6 AS milestone_months
                         UNION ALL
                         SELECT 12
                         UNION ALL
                         SELECT 24
                         UNION ALL
                         SELECT 36),

         cohort_window AS (SELECT m.milestone_months,
                                   fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                           fn_gregorian_to_ethiopian_calendar(REPORT_START_DATE, 'Y-M-D'),
                                           INTERVAL -m.milestone_months MONTH))            AS window_start,
                                   fn_ethiopian_to_gregorian_calendar(DATE_ADD(
                                           fn_gregorian_to_ethiopian_calendar(REPORT_END_DATE, 'Y-M-D'),
                                           INTERVAL -m.milestone_months MONTH))            AS window_end
                            FROM milestones m),

         tmp_cohort AS (SELECT cw.milestone_months,
                                f.client_id,
                                ROW_NUMBER() OVER (PARTITION BY cw.milestone_months, f.client_id
                                    ORDER BY f.follow_up_date DESC, f.encounter_id DESC)  AS row_num
                         FROM FollowUp f
                                  JOIN cohort_window cw
                                       ON f.art_start_date BETWEEN cw.window_start AND cw.window_end
                         WHERE f.follow_up_status IS NOT NULL
                           AND f.art_start_date IS NOT NULL
                           AND f.follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())),

         cohort AS (SELECT milestone_months, client_id FROM tmp_cohort WHERE row_num = 1)

    SELECT c.milestone_months                                                                 AS milestone_months,
           COUNT(*)                                                                            AS original_cohort,
           SUM(fn_get_ti_status(c.client_id, REPORT_START_DATE, REPORT_END_DATE) = 'TI')       AS ti_count,
           SUM(lfu.follow_up_status = 'TO')                                                    AS to_count,
           COUNT(*)
               + SUM(fn_get_ti_status(c.client_id, REPORT_START_DATE, REPORT_END_DATE) = 'TI')
               - SUM(lfu.follow_up_status = 'TO')                                              AS net_cohort_denominator,
           SUM(lfu.follow_up_status IN ('Alive on ART', 'Restart'))                            AS retained_numerator,
           ROUND(
                   SUM(lfu.follow_up_status IN ('Alive on ART', 'Restart')) /
                   NULLIF(COUNT(*)
                       + SUM(fn_get_ti_status(c.client_id, REPORT_START_DATE, REPORT_END_DATE) = 'TI')
                       - SUM(lfu.follow_up_status = 'TO'), 0) * 100, 1)                        AS retention_pct
    FROM cohort c
             JOIN latest_follow_up lfu ON c.client_id = lfu.client_id
    GROUP BY c.milestone_months
    ORDER BY c.milestone_months;
END //

DELIMITER ;
