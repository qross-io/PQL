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

--OPEN mysql.adb;
DELETE FROM tc WHERE id<10;
DELETE FROM td WHERE id<10;