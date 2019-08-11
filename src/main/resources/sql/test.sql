
OPEN "mysql.test":
    GET # SELECT * FROM tc where id=27;
SAVE AS default:
    PUT # insert into td (status, info) values (&status, &info);