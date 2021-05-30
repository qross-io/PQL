IF $module_name != '' THEN
    SET $top := SELECT id FROM qross_projects WHERE project_name=$module_name -> IF EMPTY 0;
    IF $parent == '0' THEN
        SET $parent := $top;
    END IF;
    IF NOT EXISTS (SELECT id FROM qross_projects WHERE top_project_id=$top AND parent_project_id=$parent AND project_name=$project_name) THEN
        INSERT INTO qross_projects (top_project_id, parent_project_id, project_name) VALUES ($top, $parent, $project_name);
    ELSE
        OUTPUT 0;
    END IF;
ELSE
    IF $parent == '0' THEN
        IF NOT EXISTS (SELECT id FROM qross_projects WHERE top_project_id=0 AND parent_project_id=0 AND project_name=$project_name) THEN
            INSERT INTO qross_projects (top_project_id, parent_project_id, project_name) VALUES (0, 0, $project_name);
        ELSE
            OUTPUT 0;
        END IF;
    ELSE
        SET $top := SELECT top_project_id FROM qross_projects WHERE id=#{parent} -> IF EMPTY 0;
        IF NOT EXISTS (SELECT id FROM qross_projects WHERE top_project_id=$top AND parent_project_id=#{parent} AND project_name=$project_name) THEN
            INSERT INTO qross_projects (top_project_id, parent_project_id, project_name) VALUES ($top, #{parent}, $project_name);
        ELSE
            OUTPUT 0;
        END IF;
    END IF;
END IF;