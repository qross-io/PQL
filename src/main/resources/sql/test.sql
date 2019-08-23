
--OPEN "mysql.test":
--    GET # SELECT * FROM tc where id=27;
--SAVE AS default:
--    PUT # insert into td (status, info) values (&status, &info);

DEBUG ON;

OPEN "mysql.qross";
SELECT id FROM test.wz LIMIT 0 -> INSERT IF NOT EXISTS 2;


--SET $a := @NOW SET "DAY=1" SET "DAY-1" FORMAT "yyyy-MM-dd";
--set $a := @now PLUS DAYS $b * - 2;
--TEMP hello # SELECT * FROM test.wz;
--
--OPEN TEMP:
--    SELECT * FROM hello;


--IF true THEN
--    PRINT INFO 'CORRECT;
--END IF;