DROP FUNCTION IF EXISTS fn_gregorian_to_ethiopian_calendar;

DELIMITER //
CREATE FUNCTION fn_gregorian_to_ethiopian_calendar(_date DATE, _format VARCHAR(100))
    RETURNS VARCHAR(100)  -- Increased size to handle longer formats if needed
    DETERMINISTIC
BEGIN
    DECLARE s INT;
    DECLARE t INT;
    DECLARE n INT;
    DECLARE jdn INT;
    DECLARE era INT DEFAULT 1723856;
    DECLARE r INT;
    DECLARE eth_year INT;
    DECLARE eth_month INT;
    DECLARE eth_day INT;

    DECLARE greg_year INT DEFAULT YEAR(_date);
    DECLARE greg_month INT DEFAULT MONTH(_date);
    DECLARE greg_day INT DEFAULT DAY(_date);

    -- Calculate leap day indicator
    SET s = (greg_year DIV 4) - ((greg_year - 1) DIV 4)
                - (greg_year DIV 100) + ((greg_year - 1) DIV 100)
                + (greg_year DIV 400) - ((greg_year - 1) DIV 400);

    -- Month adjustment
    SET t = (14 - greg_month) DIV 12;

    -- Day of year offset
    SET n = 31 * t * (greg_month - 1)
                + (1 - t) * (59 + s + 30 * (greg_month - 3) + ((3 * greg_month - 7) DIV 5))
                + greg_day - 1;

    -- Julian Day Number
    SET jdn = 1721426
                  + 365 * (greg_year - 1)
                  + ((greg_year - 1) DIV 4)
                  - ((greg_year - 1) DIV 100)
        + ((greg_year - 1) DIV 400)
        + n;

    -- Ethiopian conversion
    SET r = (jdn - era) MOD 1461;
    SET n = r MOD 365 + 365 * (r DIV 1460);
    SET eth_year = 4 * ((jdn - era) DIV 1461) + (r DIV 365) - (r DIV 1460);
    SET eth_month = (n DIV 30) + 1;
    SET eth_day = (n MOD 30) + 1;

    -- Format the output
    SET _format = REPLACE(_format, 'Y', CAST(eth_year AS CHAR));
    SET _format = REPLACE(_format, 'M', LPAD(CAST(eth_month AS CHAR), 2, '0'));
    SET _format = REPLACE(_format, 'D', LPAD(CAST(eth_day AS CHAR), 2, '0'));

    RETURN _format;
END //
DELIMITER ;