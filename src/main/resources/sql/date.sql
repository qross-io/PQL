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

PRINT "123456" DROP 1;

VAR $id := SELECT id, username FROM qross_users -> FIRST COLUMN -> JOIN '","' QUOTE '"';
PRINT $id;

REQUEST JSON API 'http://bigdata-api-dev.zichan360.com/encryption-service/entryption/entryStr'
DATA  { "list" : [$id!] } METHOD 'POST';
PARSE '/data' AS TABLE;
