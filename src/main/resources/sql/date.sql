DEBUG ON;

--OPEN QROSS;

--GET # SELECT id, project_id, title, job_type, owner FROM qross_jobs;
--SAVE AS NEW EXCEL 'f:/jobs.xlsx';
--PUT # INSERT INTO sheet2 (A, B, C) VALUES (#id, #project_id, &title);

--OPEN CSV FILE 'f:/jobs.csv' AS TABLE jobs WITH FIRST ROW HEADERS;
--SELECT id, project_id, owner  FROM :jobs SEEK 0 LIMIT 5;
--PRINT @POINTER;

SET $applies := SELECT event_applies FROM qross_keeper_custom_events WHERE id=#{id};
IF $applies != '#{applies}' THEN
    FOR $apply IN ${ $applies SPLIT ',' } LOOP
        IF NOT ('#{applies}' CONTAINS $apply) THEN
            DELETE FROM qross_jobs_events WHERE event_name='''onTask${ $apply INIT CAP }''' AND event_function='CUSTOM_#{id}';
        END IF;
    END LOOP;
END IF;

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

-- OUTPUT # DIR CAPACITY 'c:/io.Qross/Coding';


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
