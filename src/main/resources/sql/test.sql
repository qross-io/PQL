
--OPEN "mysql.test":
--    GET # SELECT * FROM tc where id=27;
--SAVE AS default:
--    PUT # insert into td (status, info) values (&status, &info);

DEBUG ON;

OPEN QROSS;

IF '#{parent}' == '0' THEN
    OUTPUT {
        "projects": ${{
           SELECT A.order_index, A.id, A.project_name, A.parent_project_id, A.description, IFNULL(B.amount, 0) AS amount
              FROM (
                        SELECT 4 AS order_index, id, project_name, parent_project_id, description FROM qross_projects WHERE parent_project_id=0
                            UNION ALL
                        SELECT 3 AS order_index, -1 AS id, 'Unclassified' AS project_name, 0 AS parent_project_id, 'Unclassified Jobs' AS description FROM dual) A
                LEFT JOIN (SELECT project_id, COUNT(0) AS amount FROM qross_jobs GROUP BY project_id) B ON A.id=B.project_id
                            UNION ALL
                        SELECT 1 AS order_index, -3 AS id, 'All' AS project_name, 0 AS parent_project_id,'All Jobs' AS description, COUNT(0) AS amount FROM qross_jobs
                            UNION ALL
                    SELECT 2 AS order_index, -2 AS id, 'Exceptional' AS project_name, 0 AS parent_project_id, 'Exceptional Jobs' AS description, count(id) AS amount FROM qross_jobs WHERE unchecked_exceptional_tasks<>''
                  ORDER BY order_index ASC, project_name ASC
          }},
          "jobs": []
      };
ELSE
    SET $where := '';
    IF #{parent} > 0 THEN
        SET $where := "WHERE project_id=#{parent}";
    ELSIF #{parent} == -2 THEN
        SET $where := "WHERE unchecked_exceptional_tasks<>''";
    ELSIF #{parent} == -3 THEN
        SET $where := "WHERE 1=1";
    ELSIF #{parent} == -1 THEN
        SET $where := "WHERE project_id=0";
    END IF;

    OUTPUT {
        "projects": ${{
            SELECT A.id, A.project_name, A.parent_project_id, IFNULL(B.amount, 0) AS amount, A.description FROM qross_projects A
             LEFT JOIN (SELECT project_id, COUNT(0) AS amount FROM qross_jobs GROUP BY project_id) B ON A.id=B.project_id
             WHERE parent_project_id=#{parent} ORDER BY project_name ASC }},
        "jobs": ${{ SELECT id, title, description FROM qross_jobs $where! }}
    };
END IF;

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

