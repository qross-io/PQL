DEBUG ON;

OPEN QROSS;

GET # SELECT title, id FROM qross_jobs LIMIT 10;
PUT # REPLACE INTO qross_notes (title, owner);