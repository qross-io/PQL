DEBUG ON;

-- REQUEST JSON API '''http://192.168.3.67:7700/keeper/logs?token=@KEEPER_HTTP_TOKEN&hour=20210725/11&cursor=0''';
-- PARSE "/" AS OBJECT;

PRINT ${ @NOW GET FULL WEEK NAME };


--PUT # INSERT INTO td (status) VALUES ('#item');


-- SELECT id AS project_id, parent_project_id FROM qross_projects WHERE top_project_id=77 AND id > 296 ORDER BY parent_project_id ASC, id ASC -> TO TREE (project_id, parent_project_id, 77, children);

--SET $int := @TEST_FUNCTION_3(1, 2, $e := 5, $d := 7, $c := 6);
--PRINT $int;

