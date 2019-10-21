
--OPEN "mysql.test":
--    GET # SELECT * FROM tc where id=27;
--SAVE AS default:
--    PUT # insert into td (status, info) values (&status, &info);

DEBUG ON;

--sum(task_logs_space_usage) as abc

SELECT sum(task_logs_space_usage) as abc FROM qross_space_monitor where task_logs_space_usage > 0;


--VAR $TO_CLEAR := SELECT B.job_id, A.keep_x_task_records FROM qross_jobs A
--                                        INNER JOIN (SELECT job_id, COUNT(0) AS task_amount FROM qross_tasks GROUP BY job_id) B ON A.id=B.job_id
--                                            WHERE A.keep_x_task_records>0 AND B.task_amount>A.keep_x_task_records;
--
--FOR $job_id, $keep_tasks IN $TO_CLEAR LOOP
--    SET $task_id := SELECT id AS task_id FROM qross_tasks WHERE job_id=$job_id ORDER BY id DESC LIMIT $keep_tasks,1;
--    PRINT $task_id;
--    VAR $count := 0;
--    FOR $id, $create_time IN (SELECT id, create_time FROM qross_tasks WHERE job_id=$job_id AND id<=$task_id) LOOP
--        PRINT $id;
--        DELETE FILE @QROSS_HOME + "tasks/" + $job_id + "/" + ${ $create_time REPLACE "-" TO "" SUBSTRING 1 TO 9 } + "/" + $id + ".log";
--        SET $count := $count + 1;
--    END LOOP;

--    DELETE FROM qross_tasks_logs WHERE job_id=$job_id AND task_id<=$task_id;
--    DELETE FROM qross_tasks_dependencies WHERE job_id=$job_id AND task_id<=$task_id;
--    DELETE FROM qross_tasks_dags WHERE job_id=$job_id AND task_id<=$task_id;
--    DELETE FROM qross_tasks_events WHERE job_id=$job_id AND task_id<=$task_id;
--    DELETE FROM qross_tasks_records WHERE job_id=$job_id AND task_id<=$task_id;
--    SET $rows := DELETE FROM qross_tasks WHERE job_id=$job_id AND id<=$task_id;
--    INSERT INTO qross_jobs_clean_records (job_id, amount) VALUES ($job_id, $rows);

--    PRINT DEBUG $rows + ' tasks of job $job_id has been deleted.';
--END LOOP;



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