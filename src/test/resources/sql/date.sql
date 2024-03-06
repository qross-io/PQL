DEBUG ON;

--OPEN REDIS qross;
--REDIS KEYS *;

--GET # SELECT id, project_id, title, job_type, owner FROM qross_jobs;
--SAVE AS NEW EXCEL 'f:/jobs.xlsx';
--PUT # INSERT INTO sheet2 (A, B, C) VALUES (#id, #project_id, &title);

--OPEN CSV FILE 'f:/jobs.csv' AS TABLE jobs WITH FIRST ROW HEADERS;
--SELECT id, project_id, owner  FROM :jobs SEEK 0 LIMIT 5;
--PRINT @POINTER;

SET $C := 'HOURLY 0/5';
SET $T := @NOW;
VAR $RECENT := [];
SET $M := $C TICK BY $T;
FOR $I IN 1 TO 10 LOOP
    IF $M IS NOT NULL THEN
        LET $RECENT ADD ${ $M FORMAT 'yyyy-MM-dd HH:mm:ss' };
        SET $T := $M PLUS 1 SECOND;
        SET $M :=  $C TICK BY $T;
    END IF;
END LOOP;
PRINT $RECENT;

EXIT CODE 0;

SET $command_type, $command_logic := SELECT command_type, command_logic FROM qross_commands_templates WHERE id=8;
    IF $command_logic IS NOT NULL AND $command_logic IS NOT EMPTY THEN
        IF $command_type == 'pql' THEN
            GET # INVOKE cn.qross.pql.PQL.recognizeParametersIn(String $command_logic);
        ELSE
            GET # INVOKE cn.qross.pql.PQL.recognizeParametersInEmbedded(String $command_logic);
        END IF;
    END IF;

FOR $param IN @BUFFER LOOP
    PRINT $param;
END LOOP;

EXIT CODE 0;

IF ${ '(?i)^SELECT' FIND FIRST IN 'select * from qross_jobs' } === 'Select' THEN
    PRINT 'HELLO';
END IF;

EXIT CODE 0;

SELECT * FROM qross_projects -> COLLECT (id, project_name, parent_project_id) AS 'combined';

EXIT CODE 0;

IF $template_id != '0' AND NOT EXISTS (SELECT id FROM qross_commands_templates_parameters WHERE template_id=#{template_id}) THEN
    SET $command_logic := SELECT command_logic FROM qross_commands_templates WHERE id=#{template_id};
    IF $command_logic IS NOT NULL AND $command_logic IS NOT EMPTY THEN
        GET # INVOKE cn.qross.pql.PQL.recognizeParametersIn(String $command_logic);
        PUT # INSERT INTO qross_commands_templates_parameters (template_id, parameter_name, creator) VALUES (#{template_id}, &item, @userid);
    END IF;
END IF;
SELECT * FROM qross_commands_templates_parameters WHERE template_id=#{template_id};

EXIT CODE 0;

SET $x := 'PRINT #{hello};\nSELECT * FROM $a!;\n';
VAR $y := INVOKE cn.qross.pql.PQL.recognizeParametersIn($x);
PRINT $y;

-- FILE DELETE 'c:/Space/test.log';

/*
SET $table := "qross_jobs";
SET $id := 1;

VAR $row :=
    CASE WHEN EXISTS (SELECT id, title FROM $table! WHERE id=$id) THEN
        (SELECT id, title FROM qross_jobs LIMIT 0 -> INSERT { "id": 222, "title": "HELLO WORLD" })
    ELSE
        (SELECT id, title FROM qross_jobs LIMIT 1)
    END -> FIRST ROW;

--FOR $job OF (SELECT id, title FROM qross_jobs LIMIT 1 -> INSERT { "id": 222, "title": "HELLO WORLD" }) LOOP
--    PRINT $job.id;
--    PRINT $job.title;
--END LOOP;

OUTPUT # IF true THEN {
    "count": $row.id,
    "message": $row.title,
    "if": 1,
    "then": 2
} ELSE {
    "id": $row.id,
    "message": $row.title,
    "case": 3,
    "when": 4
} END;

REQUEST JSON API '''http://@KEEPER_HTTP_ADDRESS:@KEEPER_HTTP_PORT/global/set?name=QUIT_ON_NEXT_BEAT&value=yes''' METHOD 'PUT';
PARSE '/' AS VALUE;
*/

-- REDIS HGET site qross -> CONCAT " -> master -> keeper";
--SAVE TO REDIS qross;
--GET # SELECT status FROM td WHERE id IN (62, 63);
--PASS # REDIS HGET site #status -> TO TABLE (status);
--PUT # INSERT INTO td (status, info) VALUES ('pass', '#status');
-- GET # REDIS HGETALL site -> TO TABLE (site,url);
-- PUT # REDIS HSET site2 &site &url;


-- REQUEST JSON API '''http://localhost:7700/task/instant?creator=1&info={"jobId":4,"dag":"","params":"","commands":"","delay":0,"ignoreDepends":"yes"}'''
--    METHOD 'PUT';

--OPEN QROSS;
--SET $record_time := '#{record_time}';
--SET $record_day := '#{record_time}' FORMAT 'yyyyMMdd';
--SET $record_time := '#{record_time}' FORMAT 'HHmmss';
--SET $file := @QROSS_HOME + 'tasks/' + $record_day + '/#{job_id}/#{task_id}_' + $record_time + '.log';
--IF FILE EXISTS $file THEN
--    OPEN JSON FILE $file AS TABLE 'logs';
--
--    GET # SELECT logTime, logType, logText FROM :logs WHERE logType != 'INFO';
--    SHOW 20;
--
--    FOR $time, $type, $text IN (SELECT logTime, logType, logText FROM :logs WHERE logType != 'INFO') LOOP
--        PRINT $time;
--        PRINT $type;
--        PRINT $text;
--    END LOOP;
--END IF;

-- REQUEST JSON API  '''https://api-bigdata-staging.zichan360.com/phone/singleCallByTtsOfkeeper?calledShowNumber=01086483133&calledNumber=18631675795&ttsCode=TTS_169899687&deptNo=0007&token=7f0e43a86e15a20bdff07a160e0b9cc4&cId=9999&name=SingleCallByVoice&jobId=652&jobTitle=${ "aaa" URL ENCODE }''';

-- OUTPUT # DIR CAPACITY 'c:/cn.qross/Coding';


    -- $depend.dependency_value -> GET 'jobId';
	-- UPDATE qross_jobs_dependencies SET dependency_label=${ $value TAKE AFTER ':' TAKE BEFORE ',' TRIM '"' AND '"' }, dependency_content=${ $value TAKE AFTER '${' TAKE BEFORE '}' TRIM }, dependency_option='success' WHERE id=$id;


-- GET # SELECT id, title, cron_exp FROM qross_jobs LIMIT 10;
-- SAVE AS NEW EXCEL 'c:/space/万笔适宜.xlsx';
-- PUT # INSERT INTO sheet1 (A, B, C) VALUES (#id, '#title', '#cron_exp');

--OPEN TXT FILE 'f:/18.log' AS TABLE 'logs' (datetime TEXT, log_type TEXT, log_info TEXT) DELIMITED BY ' \[|\]';
--OUTPUT {
--    "logs": ${{ SELECT * FROM :logs SEEK -1 LIMIT 10 }},
--    "cursor": @POINTER
--}
