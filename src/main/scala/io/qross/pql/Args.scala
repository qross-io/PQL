package io.qross.pql

//使用?表示可选，使用+表示一个或多个
object Args {
    val None = 0 //无参数
    val One = 1 //一个参数, 参数名字中无空格, 用方括号 [..] 包围
    val Multi = 2 //多参, 用圆括号 (..)包围
    val More = 3  //one or more, Multi的多个重复, 必须是最后一个参数，格式如 (..), (..), 比如insert语句的串接模式
    val Map = 4 //两种表现形式，一种与URL地址参数相同 a=1&b=2&c=3 中间不能包含空格，另一种为json对象格式 { "a": 1, "b": 2, "c": 3}
    val Char = 5 //字符或字符串, 必须输入字符串, 使用单引号 '..' 包围。是One参数的子集
    val Select = 6 //一个参数或多个参数, 参数之间使用逗号隔开, 可包含单词，主要是SELECT语句中的select和limit, 依靠下一个phrase的名字识别，用星号*表示，或星号*..*包围
    val Set = 7 //一整套参数, 主要是update中的set, 依靠下一个phrase的名字识别，使用与号 &..& 包围
    val Condition = 8 //条件, 主要是where, 依靠下一个phrase的名字识别，使用竖线 |..| 包围
    val Limit = 9 // LIMIT格式，表示为 m,n

    //val OrderBy&GroupBy = 9  // 有点像SELECT
}