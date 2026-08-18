DROP FUNCTION IF EXISTS fn_add_ethiopian_months;

DELIMITER //
CREATE FUNCTION fn_add_ethiopian_months(
    eth_date    VARCHAR(10),
    n           INT,
    skip_pagume TINYINT(1)
)
    RETURNS VARCHAR(10)
    DETERMINISTIC
BEGIN
    DECLARE y INT; DECLARE m INT; DECLARE d INT;
    DECLARE m0 INT;
    DECLARE wheel INT;
    DECLARE pagume_len INT;

    SET y = CAST(SUBSTRING_INDEX(eth_date, '-', 1) AS SIGNED);
    SET m = CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(eth_date, '-', 2), '-', -1) AS SIGNED);
    SET d = CAST(SUBSTRING_INDEX(eth_date, '-', -1) AS SIGNED);

    IF skip_pagume = 1 THEN
        IF m = 13 THEN
            SET m = 12;
            SET d = 30;
        END IF;
        SET wheel = 12;
    ELSE
        SET wheel = 13;
    END IF;

    SET m0 = (m - 1) + n;
    SET y  = y + FLOOR(m0 / wheel);
    SET m  = MOD(MOD(m0, wheel) + wheel, wheel) + 1;

    IF skip_pagume = 0 AND m = 13 THEN
        SET pagume_len = IF(MOD(y, 4) = 3, 6, 5);
        SET d = LEAST(d, pagume_len);
    END IF;

    RETURN CONCAT(y, '-', LPAD(m, 2, '0'), '-', LPAD(d, 2, '0'));
END //

DELIMITER ;