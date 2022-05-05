DEBUG ON;

INSERT INTO qross_calendar (solar_year, solar_month, solar_day, lunar_day, solar_term, festival, week_number, workday) VALUES (1970,1,1,'廿四','','元旦',4,-1),(1970,1,2,'廿五','','',5,-1),(1970,1,3,'廿六','','',6,-1);

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
PUT # INSERT INTO qross_calendar (solar_year, solar_month, solar_day, lunar_day, solar_term, festival, week_number, workday) VALUES (#solar_year, #solar_month, #solar_day, '#lunar_day', '#solar_term', '#festival', #week_number, #workday);

-- REQUEST JSON API '''http://192.168.3.67:7700/keeper/logs?token=@KEEPER_HTTP_TOKEN&hour=20210725/11&cursor=0''';
-- PARSE "/" AS OBJECT;

--OPEN CSV FILE 'c:/Space/therbligs.csv' AS TABLE 'therbligs' WITH FIRST ROW HEADERS;
--GET # SELECT * FROM :therbligs;
--SAVE TO DEFAULT;
--PUT # INSERT INTO therbligs (action, elapsed, degree) VALUES (?, ?, ?);

--FOR $file OF (FILE LIST "C:\\io.Qross\\MyDrivers 2\\新建文件夹\\385\\1") LOOP
--    SET $index := $file.name TAKE AFTER '(' TAKE BEFORE ')';
--    IF $index <= 7019 THEN
--        FILE DELETE $file.path;
--    ELSE
--        EXIT;
--    END IF;
--END LOOP;


--PUT # INSERT INTO td (status) VALUES ('#item');


-- SELECT id AS project_id, parent_project_id FROM qross_projects WHERE top_project_id=77 AND id > 296 ORDER BY parent_project_id ASC, id ASC -> TO TREE (project_id, parent_project_id, 77, children);

--SET $int := @TEST_FUNCTION_3(1, 2, $e := 5, $d := 7, $c := 6);
--PRINT $int;

