DEBUG ON;

--OPEN QROSS;

--SAVE AS EXCEL STREAM FILE "hello.xlsx" TEMPLATE "template.xlsx";

--VAR $table := SELECT id, title, next_tick FROM qross_jobs LIMIT 1 -> SELECT id AS name;

--PRINT $table;

OUTPUT # IF @SECURITY_AUTHENTICATION_MODE STARTS WITH 'ldap' THEN 'focus' ELSE 'normal' END;



