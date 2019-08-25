
# API IN ONE

## 概述

在大多数情况下，接口只是用来访问数据库更新或获取数据。
API IN ONE 提供一种最快捷的方式，不需要写Java代码，直接通过SQL实现接口逻辑。

### 主要特点如下：
1. 支持变量
2. 支持IF条件语句
3. 支持FOR IN, FOR TO, FOR SELECT和WHILE循环语句
4. 支持函数
5. 支持不同类型的返回值
6. 支持js短句运算

### 未实现功能如下：
1. 跨数据源访问
2. 非数据库业务逻辑，如权限
3. 自定义函数

## 语句规则

语句和语句之间必须用分号 ; 隔开，最后一条语句可以没有分号。
必须使用分号的地方如字符串常量中，使用 ~u003b 代替

## 参数

占位符格式如下 #{name}
其中大括号不能省略
接受从接口地址中传递来的数据，仅替换，不做任何加工

## 字符串常量
使用双引号包含，如 "hello world"
转义符使用 \"，如 "hello \"Tom\""
目的是与SQL中的单引号进行区分
在嵌入查询语句中时，双引号会自动去除

## 变量

变量格式如下 ${name} 或 $name
表示用SET语句声明的变量或通过PISQL的set方法设置的全局变量
变量只有两种类型：字符串类型和数字
为与数据语句区分，字符串常量用双引号包围
为变量赋不同类型的值，变量的类型也会跟着改变

## SET 语句

SET语句用于给变量赋值或更新变量的值
SET语句支持以下几种类型：

1. 字符串常量, 如 SET $s := "";
2. 数字常量，如 SET $n := 10;
3. 函数，如 SET $s := $REPLACE($name, "hello", "world");
4. 变量，如 SET $s := $t;
5. 查询语句，如 SET $name, $age := SELECT name, age FROM table1 WHERE id=#{id};
    SELECT 语句中也支持参数、变量、函数和表达式
6. 更新语句，如 SET $affected := DELETE FROM table WHERE age<18;
    返回受影响的行数，同时支持参数、变量、函数和表达式
7. javascript表达式，如 SET $s := 2 * 3 - 1;
    同时支持传递参数、变量和函数。一般用于数学计算或字符串连接。


## IF 语句

格式
IF condition THEN
    statement...
ELSIF condition THEN
    statement...
ELSE
    statement...
END IF;

ELSIF 可以有多个，也可以写成 ELSE IF，中间必须有一个且只有一个空格
语句块中的每条语句必须使用分号隔开，最后一条也需要分号
condition 格式见“条件语句”

## FOR-IN 语句

格式
FOR vars IN collection DELIMITED BY delimiter
    LOOP
        statement...
    END LOOP;

1. collection只能是字符串，一般为某种分隔符分隔的字符串，也可以是函数或表达式等
2. DELIMITED BY语句可以省略，默认分隔符为逗号
3. vars支持多个变量，即再对每一项进行字符串分隔，如
    FOR $name&$value IN "a=1&b=2&c=3" DELIMITED BY "&"

## FOR-TO 语句

格式
FOR $i IN start TO end
    LOOP
        statement...
    END LOOP;

1. start和end一般为数字，可以是函数或表达式
2. 只支持单变量
3. 步长为1，如果需要改变步长，在循环中使用IF或嵌套循环

## FOR-SELECT 语句

格式
FOR $a, $b IN SELECT a, b FROM table1
    LOOP
        statement...
    END LOOP;

即遍历一个SELECT查询语句的结果，支持多个变量赋值
SELECT语句中支持嵌入参数、变量和函数或表达式

## WHILE 语句

格式
WHILE condition
    LOOP
        statement...
    END LOOP;

condition规则见条件语句

## 条件语句

条件表达式用于IF语句、ELSIF语句和WHILE语句中，支持20多种运算符，列表如下：
等于 =, ==
不等于 !=, <>
开始于 ^=
非开始于 =^
结束于 $=
非结束于 =$
包含于 *=
不包含于 =*
正则表达式匹配 #=
正则表达式不匹配 =#
存在 EXISTS ()  括号内写SELECT语句
不存在 NOT EXISTS ()  括号内写SELECT语句
在列表中 IN ()
不在列表中 NOT IN ()
大于 >
小于 <
大于等于 >=
小于等于 <=
为NULL IS NULL
不为NULL IS NOT NULL
为空值 IS EMPTY
不为空值 IS NOT EMPTY

与 AND
或 OR
非 NOT 可能会引起问题，不建议使用

条件表达式中也支持小括号
条件表达式左右的值支持参数、变量、常量、函数和表达式

## 表达式

在查询语句内使用时，为方便识别，需要以 ${{ }} 包围。
表达式是内部的一套计算逻辑，支持传递参数、变量、常量、函数，最后会作为js表达式进行解析计算最终结果

## 函数

1. 函数格式为  $FUNC(a1, a2) 或者 ${FUNC(a1, a2)}
2. 在语句中时必须以 $ 符号开头
3. 遵照SQL即数据库的规则或使用习惯，同一个功能的函数有多个别名
4. 函数参数支持参数、变量、常量、函数和js表达式
5. 在字符串插件或查询语句内使用时，需要使用 ${ } 包围

## 解析顺序

### 整体顺序
1. \#{name} 地址参数
1. ${{ }} 表达式
2. ${VAR} 或 $VAR 变量
3. ${FUNC()} 或 $FUNC() 函数

### 表达式内部顺序
1. ${VAR} 或 $VAR 变量
2. ${FUNC()} 或 $FUNC() 函数
3. js表达式

### 函数内部顺序
1. 内层嵌套函数
2. 外层函数

## 缓存机制

为了提高查询速度，减少与数据库的交互，在某些情况下，可以为单独每一个API设置缓存，缓存时间为1小时。
每次对单个API的访问都会重置缓存时间。
可以在接口管理页面中进行强制刷新或清空缓存。