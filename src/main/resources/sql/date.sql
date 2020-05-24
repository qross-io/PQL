DEBUG ON;

OPEN QROSS;

--GET # SELECT id, project_id, title, job_type, owner FROM qross_jobs;
--SAVE AS NEW CSV FILE 'c:/io.qross/jobs.csv' with headers;

OPEN CSV FILE 'c:/io.Qross/jobs.csv' AS jobs WITH FIRST ROW HEADERS;
SELECT id, project_id, owner  FROM :jobs SEEK 0 LIMIT 5;
PRINT @POINTER;