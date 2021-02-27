# PQL 一种简单优雅的数据处理语言
PQL是一种跨数据源的过程化查询语言（Procedural Query Language），是一种运行在JVM上的中间件语言。PQL的门槛极低，会SQL即可编写数据处理程序。PQL旨在提供一种最简单的方式对数据处理过程的各种查询语句进行封装，让开发过程更专注于业务逻辑。PQL中集成了大量附加功能，所有相关功能都可用一条语句实现，简单高效。    
PQL不仅可应用于数据开发（特别是多数据源场景下数据流转非常方便），还可应用于后端开发和模板引擎等场景，可极大简化代码，更简单的处理和呈现数据。PQL清晰的代码格式易于规范开发流程，方便统一管理。  

## PQL概览

PQL看起来很像存储过程。每条语句使用分号`;`结尾，更多语法规则见[PQL基本语法](http://www.qross.cn/pql/basic)
1. PQL的 `Hello World`：
```sql
    PRINT 'Hello World!';
```
2. PQL支持[连接任意JDBC数据源](http://www.qross.cn/pql/properties)并顺序[执行SQL语句](http://www.qross.cn/pql/sql)，例如:
```sql
    OPEN mysql.qross;
    INSERT INTO scores (name, score) VALUES ('Tom', 89);
    UPDATE students SET age=18 WHERE id=1;
    SELECT * FROM scores ORDER BY score LIMIT 10;
```
3. [跨数据源数据流转](http://www.qross.cn/pql/save)非常轻松：
```sql
    OPEN hive.cluster1;
        GET # SELECT name, COUNT(0) AS amount FROM table1 GROUP BY name;
    SAVE TO mysql.result;
        PUT # INSERT INTO table2 (name, amount) VALUES (&name, #amount);
```
4. 提供[中间数据库](http://www.qross.cn/pql/cache)缓存处理过程中的数据：
```sql
    OPEN mysql.db1;
        CACHE 'table1' # SELECT id, name FROM table1;
    OPEN mysql.db2;
        CACHE 'table2' # SELECT id, score FROM table2;
    OPEN CACHE;
        SELECT A.id, A.name, B.score FROM table1 A INNER JOIN table2 B ON A.id=B.id;       
```
5. 可无障碍[使用JSON数据](http://www.qross.cn/pql/json)：
```sql
    OUTPUT {
        "data": ${{ SELECT * FROM table1 }},
        "count": @COUNT_OF_LAST_SELECT
    };
```
6. 支持各种形式的[变量](http://www.qross.cn/pql/variable)和[表达式](http://www.qross.cn/pql/sharp)嵌入以对数据进行再加工：
```sql
SET $name := SELECT name FROM table1 WHERE id=#{id};
SELECT * FROM table2 WHERE name=$name AND create_time>=${ @NOW MINUS 1 DAY }
     -> INSERT IF EMPTY (name, score) VALUES ('N/A', 0);　
```
7. 支持条件控制语句[IF](http://www.qross.cn/pql/if)和[CASE](http://www.qross.cn/pql/case)：
```sql
    IF $i > 1 THEN
        PRINT 'greater than 1.';
    ELSIF $i < 1 THEN
        PRINT 'less then 1.';
    ELSE
        PRINT 'equals 0.';
    END IF;
```
8. 支持[FOR](http://www.qross.cn/pql/for)和[WHILE](http://www.qross.cn/pql/while)循环：
```sql
    FOR $id, $name IN (SELECT id, name FROM table3) LOOP
        PRINT $id;
        PRINT $name;
        SLEEP 1 SECOND;
    END LOOP;
```
9. 支持[请求数据接口](http://www.qross.cn/pql/request)和[发送邮件](http://www.qross.cn/pql/send)：
```sql
REQUEST JSON API 'http://www.domain.com/api?id=1'
    PARSE '/data' AS TABLE;

SEND MAIL "test mail"
    CONTENT "hello world"
    TO "user@domain.com";
```
10. 支持[Excel](http://www.qross.cn/pql/excel)、[CSV](http://www.qross.cn/pql/csv)、[TXT](http://www.qross.cn/pql/txt)等文件的读写操作：
```sql
OPEN mysql.db1;
    GET # SELECT * FROM table1;
SAVE AS NEW EXCEL "example.xlsx"  USE TEMPLATE  "template.xlsx";
    PREP # INSERT INTO sheet1 (A, B, C) VALUES ('姓名', '年龄', '分数');
    PUT # INSERT INTO sheet1 ROW 2 (A, B, C) VALUES (id, ‘#name’, &title);
``` 

PQL的最大的特点就是“简单”，可以在你的任何Java或Scala项目中使用，也可以保存为独立的脚本文件运行。更多更强大的功能请参阅各语句对应的文档。

## PQL 应用

### 统一接口 OneApi
**OneApi** 是PQL的一种应用，可以通过PQL编辑Web服务的接口，以帮助后端工程师减少大量的重复代码编写工作，快速实现业务逻辑。请参阅[OneApi文档](http://www.qross.cn/oneapi/overview)获取更多信息。

### 模板引擎 Voyager
**Voyager** 是PQL的另一个应用，和OneApi一样，也是应用于Web开发。其同类是FreeMarker或Thymeleaf，不过更简单直接，不需要记各种标记语法，直接在网页HTML代码中写SQL语句进行查询并输出数据。请参阅[Voyager文档](http://www.qross.cn/master/overview)获取更多信息。

### 任务调度工具 Keeper
**Keeper** 是一个轻量级但功能强大的任务调度工具，支持运行PQL任务，也支持通过PQL扩展Keeper，如事件、依赖等。请参阅[Keeper文档](http://www.qross.cn/keeper/overview)获取更多信息。

### 数据管理平台 Master
**Master** 是一个综合各种数据开发相关功能的管理平台，可以在上面编写PQL并直接运行、部署调度任务、管理服务接口等。请参阅[Master文档](http://www.qross.cn/master/overview)获取更多信息。

## PQL 文档目录

* PQL快速入门
    + [使用PQL](http://www.qross.cn/pql/use-pql)
    + [数据源配置](http://www.qross.cn/pql/properties)
    + [PQL源码和示例项目](http://www.qross.cn/pql/example)
    + [版本和更新](http://www.qross.cn/pql/version)

* PQL基础
    + [PQL基本语法](http://www.qross.cn/pql/basic)
    + [打开和切换数据源 OPEN](http://www.qross.cn/pql/open)
    + [PQL中的SQL语句](http://www.qross.cn/pql/sql)    
    + [PQL数据类型](http://www.qross.cn/pql/datatype)
    + [使用注释](http://www.qross.cn/pql/comment)
    + [用户变量](http://www.qross.cn/pql/variable)
    + [全局变量](http://www.qross.cn/pql/global)
    + [向PQL过程传递参数](http://www.qross.cn/pql/params)
    + [访问集合类型元素](http://www.qross.cn/pql/collection)

* 数据流转
    + [PQL中极致简单的数据流转操作](http://www.qross.cn/pql/dataflow)
    + [跨数据源数据流转 SAVE](http://www.qross.cn/pql/save)
    + [将数据保存在缓冲区 GET](http://www.qross.cn/pql/get)    
    + [将缓冲区的数据保存到数据库 PUT](http://www.qross.cn/pql/put)
    + [在目标数据源执行非查询操作 PREP](http://www.qross.cn/pql/prep)
    + [使用缓冲区数据再查询 PASS](http://www.qross.cn/pql/pass)
    + [中间临时内存数据库 CACHE](http://www.qross.cn/pql/cache)
    + [中间临时文件数据库 TEMP](http://www.qross.cn/pql/temp)
    + [大数据量下的分页查询 PAGE](http://www.qross.cn/pql/page)
    + [大数据量下的分块查询和更新 BLOCK](http://www.qross.cn/pql/block)
    + [大数据量下的数据加工 PROCESS](http://www.qross.cn/pql/process)
    + [大数据量下的批量更新 BATCH](http://www.qross.cn/pql/batch)

* 输出文件
    + [将数据保存到Excel文件](http://www.qross.cn/pql/excel)
    + [将数据另存为CSV文件](http://www.qross.cn/pql/csv)
    + [将数据另存为文本文件](http://www.qross.cn/pql/txt)
    + [将数据另存为Json数据文件](http://www.qross.cn/pql/json-file)

* PQL中的语句
    + [变量声明 SET](http://www.qross.cn/pql/set)
    + [变量声明 VAR](http://www.qross.cn/pql/var)
    + [在控制台输出信息 PRINT](http://www.qross.cn/pql/print)
    + [返回结果 OUTPUT](http://www.qross.cn/pql/output)
    + [集合类型数据编辑 LET](http://www.qross.cn/pql/let)
    + [简单输出 ECHO](http://www.qross.cn/pql/echo)
    + [启用调试 DEBUG](http://www.qross.cn/pql/debug)
    + [执行字符串形式的语句 EXEC](http://www.qross.cn/pql/exec)
    + [休息一下 SLEEP](http://www.qross.cn/pql/sleep)
    + [退出程序 EXIT CODE](http://www.qross.cn/pql/exit-code)  

* 分支和循环
    + [条件控制 IF](http://www.qross.cn/pql/if)
    + [条件控制 CASE](http://www.qross.cn/pql/case)
    + [条件表达式](http://www.qross.cn/pql/condition)
    + [循环遍历 FOR](http://www.qross.cn/pql/for)
    + [条件循环 WHILE](http://www.qross.cn/pql/while)
    + [退出循环 EXIT](http://www.qross.cn/pql/exit)
    + [继续下一次循环 CONTINUE](http://www.qross.cn/pql/continue)

* 更优雅的数据操作
    + [Sharp表达式](http://www.qross.cn/pql/sharp)
    + [字符串操作 TEXT](http://www.qross.cn/pql/sharp-text)
    + [数字操作 INTEGER/DECIMAL](http://www.qross.cn/pql/sharp-numeric)
    + [日期时间操作 DATETIME](http://www.qross.cn/pql/sharp-datetime)
    + [正则表达式操作 REGEX](http://www.qross.cn/pql/sharp-regex)
    + [数组操作 ARRAY](http://www.qross.cn/pql/sharp-array)
    + [数据行操作 ROW](http://www.qross.cn/pql/sharp-row)
    + [数据表操作 TABLE](http://www.qross.cn/pql/sharp-table)
    + [Sharp表达式操作 - Json字符串](http://www.qross.cn/pql/sharp-json)
    + [数据判断](http://www.qross.cn/pql/sharp-if)    

* PQL高级特性
    + [嵌入式查询表达式](http://www.qross.cn/pql/query)
    + [PQL中的Json](http://www.qross.cn/pql/json)
    + [富字符串](http://www.qross.cn/pql/rich)
    + [PQL中有返回值的语句](http://www.qross.cn/pql/evaluate)

* 自定义函数
    + [函数声明 FUNCTION](http://www.qross.cn/pql/function)
    + [中断数据并返回值 RETURN](http://www.qross.cn/pql/return)
    + [函数调用 CALL](http://www.qross.cn/pql/call)    

* 扩展操作
    + [请求Json数据接口 REQUEST](http://www.qross.cn/pql/request)
    + [解析Json接口返回的数据 PARSE](http://www.qross.cn/pql/parse)
    + [访问Redis](http://www.qross.cn/pql/redis)
    + [发送邮件 SEND](http://www.qross.cn/pql/send)
    + [文件操作 FILE](http://www.qross.cn/pql/file)
    + [文件夹操作 DIR](http://www.qross.cn/pql/dir)

* 其他语言相关
    + [在PQL中调用Java方法 INVOKE](http://www.qross.cn/pql/invoke)
    + [在PQL中运行Shell命令 RUN](http://www.qross.cn/pql/run)
    + [PQL中的Javascript](http://www.qross.cn/pql/javascript)    

* 附录
    + [完整的嵌入规则表](http://www.qross.cn/pql/place)
    + [特殊字符表](http://www.qross.cn/pql/characters)
    + [io.qross.pql.PQL 类](http://www.qross.cn/pql/class)
    + [PQL全局设置](http://www.qross.cn/pql/setup)

## PQL 技术支持

PQL语言免费使用，有任何问题均可联系作者或留言。PQL的更新频率为每周一次。

**官方网站 [www.qross.cn](http://www.qross.cn)**  
**作者邮箱 [wu@qross.io](mailto:wu@qross.io)**