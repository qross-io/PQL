PRINT "hello world";


--OPEN "mysql.test":
--    GET # SELECT * FROM tc where id=27;
--SAVE AS default:
--    PUT # insert into td (status, info) values (&status, &info);

DEBUG ON;

--SET $d := 1569814678642 TO DATETIME;
--SET $e := "2019-12-31 11:22:29";

PRINT @NOW;

--SET $a := @NOW SET "DAY=1" SET "DAY-1" FORMAT "yyyy-MM-dd";

-- 循环部分,正式上线后注释掉-----------------------------------------------------------------------------------------------

SET $a := '2019-10-10';
SET $b := '2019-10-11';

PRINT $a PLUS 1 YEAR;

IF $a <= $b THEN
    PRINT "<=";
END IF;

FUNCTION $test($a, $b DEFAULT 0)
    BEGIN
        PRINT $a;
        PRINT $b;
        PRINT $c;
    END;

SET $a := "abc";
SET $b := "b";
SET $c := "d";

PRINT INFO "FUNCTION CALL 1";
CALL $test($a);
PRINT INFO "FUNCTION CALL 2";
CALL $test($b := 1, $a := "world");
PRINT INFO "FUNCTION CALL 3";
CALL $test("Tom", $c := "hello");

--PRINT $e;


--SLEEP TO NEXT MINUTE;
--SLEEP TO NEXT SECOND;
--SLEEP 2 SECONDS;
--SLEEP 3 MILLIS;

-- OPEN "mysql.qross";
--SEND MAIL "test" CONTENT "hello world" TO "wuzheng";
--  GET #  SELECT id, task_time FROM qross_tasks WHERE id>1000 LIMIT 20;
--  SAVE AS NEW JSON FILE "c:/space/tasks.json";

-- 循环部分,正式上线后注释掉-----------------------------------------------------------------------------------------------

--set $a := @now PLUS DAYS $b * - 2;
--TEMP hello # SELECT * FROM test.wz;
--
--OPEN TEMP:
--    SELECT * FROM hello;


--IF true THEN
--    PRINT INFO 'CORRECT;
--END IF;


-- 'day_start_datetime' = basecommand.dayStart(date)#获取传入时间参数+
-- js_end_dat1=day_start_datetime[0:4]+day_start_datetime[5:7]+day_start_datetime[8:10]#结束日期不带'-'

--    js_start_time_rz=js_end_dat+' 00:00:00'#一天开始时间+


--    target_table='data_analy_agent_rate'
