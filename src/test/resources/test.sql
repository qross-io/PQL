DEBUG ON;



CACHE 'cache_catches' # SELECT gear_id, species_id, other_species_id, SUM(amount) AS amount, SUM(IF(other_species_id=0, amount, amount*2)) AS catches FROM fish_catches
                            WHERE journey_id=3 AND species_id>0 GROUP BY gear_id, species_id, other_species_id;
CACHE 'cache_gears' # SELECT id AS gear_id, rod_id, thread_number, hook_type, hook_number, hook_thread_number, hook_thread_length,
                            bobber_leads, adjust_1, adjust_2, bait_1, bait_2, bait_3, lure_1, lure_2, catches FROM fish_gears WHERE journey_id=3;

OPEN CACHE;
    GET # SELECT gear_id, SUM(catches) AS catches FROM cache_catches GROUP BY gear_id;
SAVE TO CACHE;
    PUT # UPDATE cache_gears SET catches=#catches WHERE gear_id=#gear_id;
SAVE TO DEFAULT;

    -- 其他装备，子线长度和子线线号单独没有意义
    GET # SELECT gear_id, thread_number, hook_type, hook_number, hook_thread_number, hook_thread_length,
                    bobber_leads, adjust_1, adjust_2, bait_1, bait_2, bait_3, lure_1, lure_2, catches FROM cache_gears;
    TRANS # [{ "gear_id": #gear_id, "catalog_name": "thread-number", "label_name": "#thread_number", "item_name": "#hook_thread_number", "catches": #catches  },
             { "gear_id": #gear_id, "catalog_name": "hook-type", "label_name": "#hook_type", "item_name": "#hook_number", "catches": #catches },
             { "gear_id": #gear_id, "catalog_name": "bobber", "label_name": "#bobber_leads", "item_name": "", "catches": #catches },
             { "gear_id": #gear_id, "catalog_name": "adjust", "label_name": "#adjust_1", "item_name": "#bobber_leads", "catches": #catches },
             { "gear_id": #gear_id, "catalog_name": "adjust", "label_name": "#adjust_2", "item_name": "#bobber_leads", "catches": #catches },
             { "gear_id": #gear_id, "catalog_name": "bait", "label_name": "#bait_1", "item_name": "", "catches": #catches },
             { "gear_id": #gear_id, "catalog_name": "bait", "label_name": "#bait_2", "item_name": "", "catches": #catches },
             { "gear_id": #gear_id, "catalog_name": "bait", "label_name": "#bait_3", "item_name": "", "catches": #catches },
             { "gear_id": #gear_id, "catalog_name": "lure", "label_name": "#lure_1", "item_name": "", "catches": #catches },
             { "gear_id": #gear_id, "catalog_name": "lure", "label_name": "#lure_2", "item_name": "", "catches": #catches }];
    SHIFT # DELETE "label_name = ''" -> SELECT gear_id, catalog_name, label_name, item_name, SUM(catches) AS catches;
    PUT # INSERT INTO fish_gears_labels (angler_id, `year`, catalog_name, label_name, used, catches) VALUES (#{angler}, #{year}, &catalog_name, &label_name, 1, #catches)
                    ON DUPLICATE KEY UPDATE used=used, catches=catches;

    SAVE AS CACHE TABLE 'cache_labels';

    GET # SELECT B.catalog_name, B.label_name, B.item_name,  A.species_id, SUM(A.catches) AS catches FROM  (
               SELECT gear_id, species_id, amount AS catches FROM cache_catches
                    UNION ALL
               SELECT gear_id, other_species_id AS species_id, amount AS catches FROM cache_catches WHERE other_species_id>0) A
            INNER JOIN (
               SELECT gear_id, 'rod' AS catalog_name, CAST(rod_id AS TEXT) AS label_name, '' AS item_name FROM cache_gears
                    UNION
               SELECT gear_id, catalog_name, label_name, item_name FROM cache_labels WHERE catalog_name IN ('thread-number', 'hook-type', 'adjust', 'lure')
       ) B ON A.gear_id=B.gear_id GROUP BY 1,2,3,4;
    PUT # INSERT INTO fish_gears_species (angler_id, catalog_name, label_name, item_name, species_id, catches)
                VALUES (#{angler}, &catalog_name, &label_name, &item_name, #species_id, #catches) ON DUPLICATE KEY UPDATE catches=catches+#catches;

EXIT CODE 0;

open default;
    get # select species_name from fish_species;

save to mysql.fish;
    put # insert into fish_species (species_name) values (&species_name);

EXIT CODE 0;

OPEN DEFAULT;
    GET # SELECT title_name, catalog_name, label_name FROM fish_dictionary ORDER BY 1, 2, 3;
SAVE TO mysql.fish;
    PUT # INSERT INTO fish_dictionary (title_name, catalog_name, label_name) VALUES (&title_name, &catalog_name, &label_name);

EXIT CODE 0;

GET # SELECT * FROM fish_gears;

TRANS # [
        {"catalog_name": "gear", "title_name": "thread-number", "item_name": "#thread_number", "catches": #catches},
        {"catalog_name": "gear", "title_name": "hook-type", "item_name": &hook_type, "catches": #catches},
        {"catalog_name": "gear", "title_name": "hook-thread-length", "item_name": &hook_thread_length, "catches": #catches}
    ];
--SAVE AS CACHE TABLE 'cache_dictionary' PRIMARY KEY 'id';
--OPEN CACHE;
--SELECT catalog_name, title_name,  SUM(catches) as sc, COUNT(distinct item_name) AS cc, COUNT() AS cn FROM cache_dictionary GROUP BY catalog_name, title_name;
SHIFT # DELETE "item_name=''" SELECT COUNT(item_name) AS cc, SUM(catches) as sc, MIN(catches) AS ic, MAX(catches) AS ac, AVG(catches) AS vc;

--, COUNT(catalog_name+title_name), SUM(catches)


EXIT CODE 0;

VAR $s := '{"id":1}' FIND '/' AS ROW;
PRINT $s.id;


-- @REPLACE($f, @PROPERTIES('pql.debug'), @PROPERTIES('hello.world'))

--INSERT INTO qross_calendar (solar_year, solar_month, solar_day, lunar_day, solar_term, festival, week_number, workday) VALUES (1970,1,1,'廿四','','元旦',4,-1),(1970,1,2,'廿五','','',5,-1),(1970,1,3,'廿六','','',6,-1);
--
--OPEN CSV FILE '''d:/Space/ProvincesAndCities.csv''' AS TABLE 'regions' WITH FIRST ROW HEADERS;
--GET # SELECT * FROM :regions;
--PUT # INSERT INTO fish_regions (code, parent_code, region_name, short_name, spell, full_name, region_level, tel_code, post_code, lng, lat) VALUES
--        (#area_code, #parent_id, '#name', '#short_name', '#pin_yin', '#full_name', #city_lv, '#tel_code', '#post_code', #lng, #lat);


-- REQUEST JSON API '''http://192.168.3.67:7700/keeper/logs?token=@KEEPER_HTTP_TOKEN&hour=20210725/11&cursor=0''';
-- PARSE "/" AS OBJECT;

--OPEN CSV FILE 'c:/Space/therbligs.csv' AS TABLE 'therbligs' WITH FIRST ROW HEADERS;
--GET # SELECT * FROM :therbligs;
--SAVE TO DEFAULT;
--PUT # INSERT INTO therbligs (action, elapsed, degree) VALUES (?, ?, ?);

--FOR $file OF (FILE LIST "C:\\cn.qross\\MyDrivers 2\\新建文件夹\\385\\1") LOOP
--    SET $index := $file.name TAKE AFTER '(' TAKE BEFORE ')';
--    IF $index <= 7019 THEN
--        FILE DELETE $file.path;
--    ELSE
--        EXIT;
--    END IF;
--END LOOP;


--PUT # INSERT INTO td (status) VALUES ('#item');


-- SELECT id AS project_id, parent_project_id FROM qross_projects WHERE top_project_id=77 AND id > 296 ORDER BY parent_project_id ASC, id ASC -> TO TREE (project_id, parent_project_id, 77, children);

--SET $int := @TEST_FUNCTION_3(1, 2, $e := 5, $d := 7, $c := 6);
--PRINT $int;

