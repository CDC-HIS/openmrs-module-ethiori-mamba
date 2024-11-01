DROP FUNCTION IF EXISTS fn_gregorian_to_ethiopian_calendar;

DELIMITER //
CREATE FUNCTION fn_gregorian_to_ethiopian_calendar(_date DATE, _format VARCHAR(100))
    RETURNS VARCHAR(10)
    DETERMINISTIC
BEGIN
    DECLARE s FLOAT;
    DECLARE t FLOAT;
    DECLARE n FLOAT;
    DECLARE jdn FLOAT;
    DECLARE era FLOAT;
    DECLARE r FLOAT;
    DECLARE _year FLOAT;
    DECLARE _month FLOAT;
    DECLARE _day  FLOAT;
    SET _year = CAST(SUBSTRING_INDEX(DATE(_date), '-', 1) AS UNSIGNED);
    SET _month = CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(DATE(_date), '-', 2), '-', -1) AS UNSIGNED);
    SET _day = CAST(SUBSTRING_INDEX(DATE(_date), '-', -1) AS UNSIGNED);
    SET s = FLOOR(_year / 4)
                - FLOOR((_year - 1) / 4)
                - FLOOR(_year / 100)
                + FLOOR((_year - 1) / 100)
                + FLOOR(_year / 400)
        - FLOOR((_year - 1) / 400);
    SET t = FLOOR((14 - _month) / 12);
    SET n = 31 * t * (_month - 1)
        + (1 - t) * (59 + s + 30 * (_month - 3) + FLOOR((3 * _month - 7) / 5))
        + _day - 1;
    SET jdn = 1721426
                  + 365 * (_year - 1)
                  + FLOOR((_year - 1) / 4)
                  - FLOOR((_year - 1) / 100)
        + FLOOR((_year - 1) / 400)
        + n;
    SET era = 1723856;
    SET r = MOD((jdn - era), 1461);
    SET n = MOD(r, 365) + 365 * FLOOR(r / 1460);
    SET _year = 4 * FLOOR((jdn - era) / 1461) + FLOOR(r / 365) - FLOOR(r / 1460);
    SET _month = FLOOR(n / 30) + 1;
    SET _day = MOD(n, 30) + 1;
    SET _format = REPLACE(_format, 'Y', _year);
    SET _format = REPLACE(_format, 'M', _month);
    SET _format = REPLACE(_format, 'D', _day);
    RETURN _format;
END //

DELIMITER ;