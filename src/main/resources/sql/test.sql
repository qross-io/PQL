
--OPEN "mysql.test":
--    GET # SELECT * FROM tc where id=27;
--SAVE AS default:
--    PUT # insert into td (status, info) values (&status, &info);

DEBUG ON;



--SET $d := 1569814678642 TO DATETIME;
--SET $e := "2019-12-31 11:22:29";

-- 循环部分,正式上线后注释掉-----------------------------------------------------------------------------------------------

--SET $a := @NOW SET "DAY=1" SET "DAY-1" FORMAT "yyyy-MM-dd";

-- 循环部分,正式上线后注释掉-----------------------------------------------------------------------------------------------

OPEN QROSS;

var $s := "abc123def";

-- PRINT $s TAKE BEFORE /\d+/;
-- PRINT $s TAKE AFTER /\d+/;
-- PRINT $s TAKE BEFORE LAST /\d/;
-- PRINT $s TAKE AFTER LAST /\d/;
-- PRINT $s TAKE BETWEEN /\d/ AND /\d/;
-- PRINT $s REPLACE ALL /\d/ TO "Z";
-- PRINT $s MATCHES /\d/;
-- PRINT $s WHOLE MATCHES /^[a-z0-9]+$/i;
-- PRINT /z/ TEST $s;
-- RINT /z/ FIND FIRST IN $s;

-- print /abc/i TEST 'ABc';


        print "dfavrABCDEsaBCfdcdf" TAKE AFTER LAST  'ABC';
        print "dfavrAbCDEsaBCfdcdf" TAKE AFTER LAST  /abc/i;
        print "dfavrAbCDEsaBCfdcdf" TAKE AFTER LAST  /a/i;
        print '--------------------TAKE BEFORE LAST---------------------------------';

        print "dfavrABCDEsaBCfdcdf" TAKE BEFORE LAST  'ABC';
        print "dfavrAbCDEsaBCfdcdf" TAKE BEFORE LAST  /abc/i;
        print "dfavrAbCDEsaBCfdcdf" TAKE BEFORE LAST  /a/i;

--PRINT $e;


--SLEEP TO NEXT MINUTE;
--SLEEP TO NEXT SECOND;
--SLEEP 2 SECONDS;
--SLEEP 3 MILLISECONDS;

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

-- day_start_datetime = basecommand.dayStart(date)#获取传入时间参数+
-- js_end_dat1=day_start_datetime[0:4]+day_start_datetime[5:7]+day_start_datetime[8:10]#结束日期不带'-'
--    js_end_dat=js_end_dat1[0:4]+'-'+js_end_dat1[4:6]+'-'+js_end_dat1[6:8]#结束日期带'-'
--    js_start_time_rz=js_end_dat+' 00:00:00'#一天开始时间
--    js_end_time_rz=js_end_dat+' 23:59:59'#一天结束时间

--    stat_mon=day_start_datetime[0:4]+day_start_datetime[5:7]#获取统计月份
--    d=datetime(int(day_start_datetime[0:4]),int(day_start_datetime[5:7]),int(day_start_datetime[8:10]))
--    d1 = d + timedelta(days=-37)#当天时间向前推37天
--    js_start_dat1=d1.strftime('%Y%m%d')#传入计算开始日期
--    js_start_dat=js_start_dat1[0:4]+'-'+js_start_dat1[4:6]+'-'+js_start_dat1[6:8]#传入开始日期带'-'
--    target_table='data_analy_agent_rate'