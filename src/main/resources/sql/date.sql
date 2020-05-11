DEBUG ON;

OPEN QROSS;

GET # select IFNULL(switch_time, '') AS switch_time, IFNULL(unchecked_exceptional_tasks, '') AS unchecked_exceptional_tasks from qross_jobs limit 10;

SAVE AS NEW EXCEL 'a123.xlsx';
PUT # INSERT INTO SHEET sheet1 (A, B) VALUES (&switch_time, &unchecked_exceptional_tasks);

