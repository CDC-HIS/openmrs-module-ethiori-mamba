DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_tx_ml_kp_datim_query;

CREATE PROCEDURE sp_dim_tx_ml_kp_datim_query()
BEGIN

    SELECT 'Died' as `Outcome`, 0 AS  `PWID`,0 AS `MSM`, 0 AS `Transgender People`, 0 AS `FSW`, 0 AS `People in prison and other closed settings`, 0 AS `Sub-totals`
    UNION ALL
    SELECT 'On treatment for <3 months when experienced IIT', 0,0,0,0,0,0
    UNION ALL
    SELECT 'On treatment for 3-5 months when experienced IIT', 0,0,0,0,0,0
    UNION ALL
    SELECT 'On treatment for 6+ months when experienced IIT', 0,0,0,0,0,0;

END //

DELIMITER ;