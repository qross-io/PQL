DEBUG ON;

OPEN QROSS;

GET # SELECT id, title FROM qross_jobs LIMIT 10;
SAVE AS NEW CSV FILE '123.csv' WITH HEADERS ('id', 'title');


