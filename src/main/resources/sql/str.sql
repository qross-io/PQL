DEBUG ON;

-- PRINT $x(2, $p(2, 4));
-- CALL $x(2, $p(2, 4));

DEBUG ON;

OPEN CSV FILE '''@QROSS_HOME/data/calendar.csv''' AS TABLE 'calendar' (
    solar_year INT,
    solar_month INT,
    solar_day INT,
    lunar_day TEXT,
    solar_term TEXT,
    festival TEXT,
    week_number INT,
    workday INT
);
GET # SELECT * FROM :calendar;
--PUT # INSERT INTO qross_calendar1 (solar_year, solar_month, solar_day, lunar_day, solar_term, festival, week_number, workday) VALUES (#solar_year, #solar_month, #solar_day, '#lunar_day', '#solar_term', '#festival', #week_number, #workday);

--GET # SELECT solar_year,solar_month,solar_day,lunar_day,solar_term,festival_name,week_number,workday FROM qross_calendar;
--SAVE AS CSV FILE 'calendar.csv' WITHOUT HEADERS;

--OPEN CSV FILE 'wnl.csv' AS TABLE 'calendar' (
--    solar_year INT,
--    solar_month INT,
--    solar_day INT,
--    week_name TEXT,
--    lunar_year INT,
--    lunar_month TEXT,
--    lunar_day TEXT,
--    ganzhi TEXT,
--    zodiac TEXT,
--    leap_month TEXT,
--    month_ganzhi TEXT,
--    day_ganzhi TEXT,
--    solar_term TEXT,
--    term_time TEXT,
--    solar_festival TEXT,
--    lunar_festival TEXT,
--    special_festival TEXT,
--    shu_fu TEXT
--);

--GET # SELECT * FROM :calendar LIMIT 1, -1;
--PUT # INSERT INTO qross_calendar (solar_year, solar_month, solar_day, lunar_year, lunar_month, lunar_day, solar_term, solar_festival, lunar_festival, week_name)
--    VALUES (#solar_year, #solar_month, #solar_day, #lunar_year, &lunar_month, &lunar_day, &solar_term, &solar_festival, &lunar_festival, &week_name);



RETURN $x(4);

FUNCTION $x($a, $b DEFAULT 3)
    BEGIN
        RETURN $a * $b;
    END;

FUNCTION $p($a, $b)
    BEGIN
        RETURN $a + $b;
    END;