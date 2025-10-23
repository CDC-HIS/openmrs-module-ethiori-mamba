DROP FUNCTION IF EXISTS fn_ethiopian_to_gregorian_calendar;

DELIMITER //
CREATE FUNCTION fn_ethiopian_to_gregorian_calendar(ethiopian_date VARCHAR(10))
    RETURNS VARCHAR(10)
    READS SQL DATA
    DETERMINISTIC
BEGIN
    DECLARE ey INT;
    DECLARE em INT;
    DECLARE ed INT;
    DECLARE jd BIGINT;
    DECLARE doy INT;
    DECLARE leap_days INT;
    DECLARE is_leap TINYINT;
    DECLARE l INT;
    DECLARE n INT;
    DECLARE i INT;
    DECLARE j INT;
    DECLARE gy INT;
    DECLARE gm INT;
    DECLARE gd INT;
    DECLARE result VARCHAR(10);

    -- Validate input format
    IF ethiopian_date NOT REGEXP '^[0-9]{4}-[0-1][0-9]-[0-3][0-9]$' THEN
        RETURN NULL;  -- Invalid format
    END IF;

    -- Parse input 'YYYY-MM-DD'
    SET ey = CAST(SUBSTRING(ethiopian_date, 1, 4) AS UNSIGNED);
    SET em = CAST(SUBSTRING(ethiopian_date, 6, 2) AS UNSIGNED);
    SET ed = CAST(SUBSTRING(ethiopian_date, 9, 2) AS UNSIGNED);

    -- Check for Ethiopian leap year (ey % 4 = 3)
    SET is_leap = (ey % 4 = 3);

    -- Validate Ethiopian date
    IF ey < 1 OR ey > 9999 OR
       em < 1 OR em > 13 OR
       ed < 1 OR
       (em <= 12 AND ed > 30) OR
       (em = 13 AND ed > (5 + is_leap)) THEN
        RETURN NULL;  -- Invalid Ethiopian date
    END IF;

    -- Calculate day of year (doy)
    IF em < 13 THEN
        SET doy = 30 * (em - 1) + ed;
    ELSE
        SET doy = 360 + ed;
    END IF;

    -- Calculate leap days adjustment
    SET leap_days = ey DIV 4 - (IF(is_leap, 1, 0));

    -- Calculate Julian Day Number (JDN)
    SET jd = 365 * (ey - 1) + leap_days + doy + 1724220;

    -- Convert JDN to Gregorian date
    SET l = jd + 68569;
    SET n = (4 * l) DIV 146097;
    SET l = l - ((146097 * n) + 3) DIV 4;
    SET i = (4000 * (l + 1)) DIV 1461001;
    SET l = l - (1461 * i) DIV 4 + 31;
    SET j = (80 * l) DIV 2447;
    SET gd = l - (2447 * j) DIV 80;
    SET gm = j + 2 - 12 * (j DIV 11);
    SET gy = 100 * (n - 49) + i + (j DIV 11);

    -- Validate Gregorian date
    IF gy < 1 OR gy > 9999 OR gm < 1 OR gm > 12 OR gd < 1 OR gd > 31 THEN
        RETURN NULL;  -- Invalid Gregorian date
    END IF;

    -- Format output as YYYY-MM-DD
    SET result = CONCAT(
            LPAD(gy, 4, '0'),
            '-',
            LPAD(gm, 2, '0'),
            '-',
            LPAD(gd, 2, '0')
                 );

    RETURN result;
END //

DELIMITER ;