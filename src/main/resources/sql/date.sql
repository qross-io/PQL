DEBUG ON;

--OPEN mysql.local;
--
--CACHE test1 # select id, status AS `中文`, info AS `100` from tc;
--
--OPEN CACHE;
--
--GET # select [100], [中文] from test1;
--SAVE AS 'mysql.local';
--PUT # insert into td (info, status) VALUES ('#[中文]', &[100]);

OPEN QROSS;

SET $create_time := '2020-04-06 12:00:00';

IF $create_time EARLIER @Now > 24 HOURS THEN
    PRINT 'HELLO';
END IF;