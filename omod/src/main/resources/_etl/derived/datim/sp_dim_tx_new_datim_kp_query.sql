DELIMITER //

DROP PROCEDURE IF EXISTS sp_dim_tx_new_datim_kp_query;

CREATE PROCEDURE sp_dim_tx_new_datim_kp_query()
BEGIN
SELECT 'PWID' AS `Population Type`, 0 AS `Total`
UNION ALL
SELECT 'People in prison and other closed settings', 0;

END //

DELIMITER ;