
--OPEN "mysql.test":
--    GET # SELECT * FROM tc where id=27;
--SAVE AS default:
--    PUT # insert into td (status, info) values (&status, &info);

DEBUG ON;

OPEN QROSS;

SET $C := "0";
SET $T := @NOW;
SET $RECENT := [];
SET $M := $C TICK BY $T;
WHILE $M IS NOT NULL LOOP
    LET $RECENT ADD $M;
    LET $T PLUS 1 SECOND;
    SET $M :=  $C TICK BY $T;
END LOOP;

OUTPUT $RECENT;

--OPEN QROSS;
--    GET # SELECT * FROM qross_users;
--SAVE AS NEW EXCEL "f:/abc.xlsx";  -- NEW 可选, 是否在文件存在时先删除
--    PREP # INSERT INTO SHEET sheet1 (A, B) VALUES ('Initial', 'LastLoginTime'); --SHEET必须有
--    PUT # INSERT INTO SHEET sheet1 (A, B) VALUES ('#initial', &last_login_time);

--VAR $a := SELECT * FROM td;
--IF $a IS NOT EMPTY THEN
--    OUTPUT $a TO HTML TABLE;
--END IF;



--SEND MAIL/EMAIL "hello world"
--    (SET) CONTENT ""
--    FROM DEFAULT TEMPLATE #
--    FROM TEMPLATE "/templates/example.html"
--    WITH DEFAULT SIGNATURE
--    WITH SIGNATURE "/templates/signature.html"
--    PLACE "hello" AT "${placeholder}"
--    PLACE DATA "a=1&b=2&c=d"
--    ATTACH "file.excel"
--    TO "wuzheng"
--    CC ""
--    BCC "";

    --FROM TEMPLATE abc.html
    --FROM DEFAULT TEMPLATE



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

