DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_tx_new_datim_kp_query;

CREATE PROCEDURE sp_dim_tx_new_datim_kp_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)

    SELECT 'Population Type' AS `Population Type`, 'Total' AS `Total`
    UNION ALL
    SELECT 'PWID', 0
    UNION ALL
    SELECT 'MSM', 0
    UNION ALL
    SELECT 'Transgender People', 0
    UNION ALL
    SELECT 'FSW', 0
    UNION ALL
    SELECT 'People in prison and other closed settings', 0
END //

DELIMITER ;