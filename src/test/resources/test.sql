DEBUG ON;

SET $NULL, $NULL2 := SELECT finish_time, CAST(finish_time AS CHAR) AS finish_time_2 FROM qross_tasks WHERE id=4041;

PRINT ${ $NULL IF NULL 'ABC'};
PRINT ${ $NULL2 IF NULL 'DEF' };


-- SELECT id AS project_id, parent_project_id FROM qross_projects WHERE top_project_id=77 AND id > 296 ORDER BY parent_project_id ASC, id ASC -> TO TREE (project_id, parent_project_id, 77, children);

--SET $int := @TEST_FUNCTION_3(1, 2, $e := 5, $d := 7, $c := 6);
--PRINT $int;