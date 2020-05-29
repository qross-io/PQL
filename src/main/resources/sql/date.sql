DEBUG ON;

--OPEN QROSS;

--GET # SELECT id, project_id, title, job_type, owner FROM qross_jobs;
--SAVE AS NEW EXCEL 'f:/jobs.xlsx';
--PUT # INSERT INTO sheet2 (A, B, C) VALUES (#id, #project_id, &title);

--OPEN CSV FILE 'f:/jobs.csv' AS TABLE jobs WITH FIRST ROW HEADERS;
--SELECT id, project_id, owner  FROM :jobs SEEK 0 LIMIT 5;
--PRINT @POINTER;

--OPEN TXT FILE 'f:/18.log' AS TABLE 'logs' (datetime DATETIME, log_type TEXT, log_info TEXT) DELIMITED BY ' \[|\]';
--OUTPUT {
--    "logs": ${{ SELECT * FROM :logs SEEK #{position} WHERE log_type='INFO' AND datetime<'2020-05-15 18:10:00' LIMIT 10 }},
--    "cursor": @POINTER
--}

OPEN TXT FILE 'f:/18.log' AS TABLE 'logs' (datetime DATETIME, log_type TEXT, log_info TEXT) DELIMITED BY '\[|\]';
OUTPUT {
    "logs": ${{ SELECT * FROM :logs SEEK #{position} WHERE datetime<'2020-05-15 18:10:00' LIMIT 100 }},
    "cursor": @POINTER
}