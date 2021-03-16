package io.qross.pql

import io.qross.core.{DataCell, DataHub, DataRow, DataType}
import io.qross.exception.SQLParseException
import io.qross.ext.TypeExt._

import scala.collection.mutable
import scala.util.Random

//全局函数

object GlobalFunction {

    //系统全局函数
    val SYSTEM: mutable.HashMap[String, GlobalFunction] = new mutable.HashMap[String, GlobalFunction]()
    //用户全局函数
    val USER: mutable.HashMap[String, GlobalFunction] = new mutable.HashMap[String, GlobalFunction]()

    //所有函数名
    val NAMES: mutable.HashSet[String] = new mutable.HashSet[String]()
    //所有无参函数名
    val WITHOUT_ARGUMENTS: mutable.HashSet[String] = new mutable.HashSet[String]()

    NAMES ++= GlobalFunctionDeclaration.getClass.getDeclaredMethods.map(m => m.getName)

    //执行有参预定义函数
    def call(functionName: String, args: List[DataCell]): DataCell = {
        classOf[GlobalFunctionDeclaration]
            .getDeclaredMethod(functionName, Class.forName("scala.collection.immutable.List"))
            .invoke(null, args).asInstanceOf[DataCell]
    }

    //执行无参函数
    def call(functionName: String): DataCell = {
        if (USER.contains(functionName)) {
            USER(functionName).call()
        }
        else if (SYSTEM.contains(functionName)) {
            SYSTEM(functionName).call()
        }
        else {
            GlobalFunction.call(functionName, List[DataCell]())
        }
    }

    def put(functionName: String, functionArgs: String, functionStatement: String): Unit = {
        USER += functionName -> new GlobalFunction(functionName, functionArgs, functionStatement)
    }
}

//执行自定义有参函数
class GlobalFunction(val functionName: String, val functionArgs: String, val functionStatement: String) {

    private val arguments = new DataRow()

    if (functionArgs != "") {
        functionArgs
            .split(",")
            .map(_.trim().takeAfter("$"))
            .foreach(variable => {
                """(?i)\sDEFAULT\s""".r.findFirstIn(variable) match {
                    case Some(default) => arguments.set(variable.takeBefore(default).trim(), variable.takeAfter(default).trim().eval())
                    case None => arguments.set(variable, DataCell.NULL)
                }
            })
    }

    //执行自定义有参函数
    def call(args: String): DataCell = {
        // 初始化并赋默认值
        val pql = new PQL(this.functionStatement, DataHub.DEFAULT).set(arguments)

        val chars = new mutable.ListBuffer[String]()
        var i = 0
        args.pickChars(chars)
            .split(",")
            .map(_.trim().takeAfter("$"))
            .foreach(arg => {
                if (arg.contains(":=")) {
                    //即使未赋值的变量也能有值
                    pql.set(arg.takeBefore(":=").trim(), arg.takeAfter(":=").trim().restoreChars(chars).eval())
                }
                else {
                    arguments.getFieldName(i) match {
                        case Some(name) => pql.set(name, arg.restoreChars(chars).eval())
                        case None =>
                    }
                }
                i += 1
            })

        pql.call()
    }

    //执行自定义无参函数
    def call(): DataCell = {
        new PQL(this.functionStatement, DataHub.DEFAULT).call()
    }

    //执行系统全局函数
//    def invoke(args: List[DataCell]): DataCell = {
//        //Class.forName("io.qross.pql.GlobalFunctionDeclaration")
//        classOf[GlobalFunctionDeclaration]
//            .getDeclaredMethod(functionName, Class.forName("scala.collection.immutable.List"))
//            .invoke(null, args).asInstanceOf[DataCell]
//    }
}

object GlobalFunctionDeclaration {
    /*
    Set(
    "SUBSTR", "TRIM", "SPLIT", "LEN",
    "IFNULL", "NVL",
    "REGEX_LIKE", "REGEX_INSTR", "REGEX_SUBSTR", "REGEX_REPLACE") */

    //CHARINDEX(stringToFind, stringToSearch)
    def CHARINDEX(args: List[DataCell]): DataCell = {
        if (args.size >= 2) {
            DataCell(args(1).asText.indexOf(args.head.asText) + 1, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @CHARINDEX, expect 2, actual ${args.size}")
        }
    }

    def CONCAT(args: List[DataCell]): DataCell = {
        DataCell(args.map(s => s.asText).mkString(""), DataType.TEXT)
    }

    def CONCAT_WS = ???

    //def ELT(n, str1, str2, str3) = ???

    //def FIND_IN_SET(str, [])

    //def HEX(N_OR_S) = ???

    //def INSERT(str,pos,len,newstr)

    def INSTR(args: List[DataCell]): DataCell = {
        if (args.size >= 2) {
            DataCell(args.head.asText.indexOf(args(1).asText) + 1, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @INSTR, expect 2, actual ${args.size}")
        }
    }

    def LCASE(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asText.toLowerCase(), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @LCASE, expect 1, actual 0")
        }
    }

    def LEFT(args: List[DataCell]): DataCell = {
        if (args.size == 2) {
            DataCell(args.head.asText.take(args(1).asInteger.toInt), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @LEFT, expect 2, actual ${args.size}")
        }
    }

    def LEN(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asText.length, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @LEN, expect 1, actual 0")
        }
    }

    def LENGTH(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asText.length, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @LENGTH, expect 1, actual 0")
        }
    }

    //LOAD_FILE 加载文件

    //LOCATE(substr,str) , LOCATE(substr,str,pos)
    //POSITION(substr IN str)是 LOCATE(substr,str)同义词。

    def LOWER(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asText.toLowerCase(), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @LOWER, expect 1, actual 0")
        }
    }

    //LPAD(str, len, padstr)
    def LPAD(args: List[DataCell]): DataCell = {
        if (args.size >= 3) {
            DataCell(args.head.asText.pad(args(1).asInteger.toInt, args(2).asText(" ")), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @LPAD, expect 3, actual " + args.size)
        }
    }

    def LTRIM(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asText.replaceFirst("^\\s+", ""), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @LTRIM, expect 1, actual 0")
        }
    }

    //MID(str,pos,len) 是 SUBSTRING(str,pos,len)的同义词。

    def POSITION(args: List[DataCell]): DataCell = {
        if (args.size >= 2) {
            DataCell(args(1).asText.indexOf(args.head.asText) + 1, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @POSITION, expect 2, actual ${args.size}")
        }
    }

    def REPEAT(args: List[DataCell]): DataCell = {
        if (args.size >= 2) {
            DataCell(List.fill(args(1).asInteger.toInt)(args.head).mkString(""), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @REPEAT, expect 2, actual ${args.size}")
        }
    }

    //REPLACE(stringToReplace, oldString, newString)
    def REPLACE(args: List[DataCell]): DataCell = {
        if (args.size == 3) {
            DataCell(args.head.asText.replace(args(1).asText, args(2).asText), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @REPLACE, expect 3, actual ${args.size}")
        }
    }

    def REVERSE(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asText.reverse, DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @REVERSE, expect 1, actual 0")
        }
    }

    //REPLACE(stringToReplace, oldString, newString)
    def RIGHT(args: List[DataCell]): DataCell = {
        if (args.size == 2) {
            DataCell(args.head.asText.takeRight(args(1).asInteger.toInt), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @RIGHT, expect 2, actual ${args.size}")
        }
    }

    //RPAD(str, length, padstr)
    def RPAD(args: List[DataCell]): DataCell = {
        if (args.size >= 3) {
            DataCell(args.head.asText.padRight(args(1).asInteger.toInt, args(2).asText(" ")), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @LPAD, expect 3, actual " + args.size)
        }
    }

    def RTRIM(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asText.replaceFirst("\\s+$", ""), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @RTRIM, expect 1, actual 0")
        }
    }

    //SPACE(N)

    //SUBSTRING(str,pos) , SUBSTRING(str FROM pos) SUBSTRING(str,pos,len) , SUBSTRING(str FROM pos FOR len)
    //SUBSTR()是 SUBSTRING()的同义词。
    //带有len参数的格式从字符串str返回一个长度同len字符相同的子字符串，起始于位置 pos。
    //使用 FROM的格式为标准 SQL 语法。也可能对pos使用一个负值。
    //假若这样，则子字符串的位置起始于字符串结尾的pos 字符，而不是字符串的开头位置。

    def TRIM(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asText.trim(), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @TRIM, expect 1, actual 0")
        }
    }

    def UCASE(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asText.toUpperCase(), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @UCASE, expect 1, actual 0")
        }
    }

    def UPPER(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asText.toUpperCase(), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @UPPER, expect 1, actual 0")
        }
    }

    def FLOOR(args: List[DataCell]): DataCell = {
        if (args.size == 1) {
            DataCell(args.head.asDecimal.floor(0), DataType.INTEGER)
        }
        else if (args.size > 1) {
            DataCell(args.head.asDecimal.floor(args.last.asInteger(0).toInt), DataType.DECIMAL)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @FLOOR, expect 1, actual 0")
        }
    }

    //取 m 到 n随机数
    def RANDOM(args: List[DataCell]): DataCell = {
        if (args.size == 1) {
            DataCell(Random.nextInt(args.head.asInteger(10).toInt), DataType.INTEGER)
        }
        else if (args.size > 1) {
            val seed = args.head.asInteger(0).toInt
            DataCell(seed + Random.nextInt(args.last.asInteger(10).toInt - seed), DataType.INTEGER)
        }
        else {
            DataCell(Random.nextInt(10), DataType.INTEGER)
        }
    }

    def ROUND(args: List[DataCell]): DataCell = {
        if (args.size == 1) {
            DataCell(args.head.asDecimal.round(0), DataType.INTEGER)
        }
        else if (args.size > 1) {
            DataCell(args.head.asDecimal.round(args.last.asInteger(0).toInt), DataType.DECIMAL)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @ROUND, expect 1, actual 0")
        }
    }
}

class GlobalFunctionDeclaration {

}