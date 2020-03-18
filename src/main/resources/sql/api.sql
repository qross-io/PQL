DEBUG ON;

OPEN QROSS;

FOR $taskTime IN '#{list}' SPLIT ',' LOOP
    INSERT INTO qross_tasks (job_id, task_time, record_time, creator, create_mode, start_mode) VALUES (#{jobId}, $taskTime, @NOW, '#user', 'instant', 'manual_start');
END LOOP;