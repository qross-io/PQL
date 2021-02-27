package io.qross.pql

import io.qross.core.{DataCell, DataType}
import io.qross.exception.SQLParseException
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

import scala.util.Random

//全局函数

object GlobalFunction {
    /*
        Set(
        "SUBSTR", "TRIM", "SPLIT", "LEN",
        "IFNULL", "NVL",
        "REGEX_LIKE", "REGEX_INSTR", "REGEX_SUBSTR", "REGEX_REPLACE") */

    def CHAR_LENGTH = ???

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

    //REPLACE(stringToReplace, oldString, newString)
    def LEFT(args: List[DataCell]): DataCell = {
        if (args.size == 2) {
            DataCell(args.head.asText.take(args(1).asInteger.toInt), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @LEFT, expect 2, actual ${args.size}")
        }
    }

    //def LCASE = ???

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

    //LPAD(str,len,padstr)

    //LTRIM(str)

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

    //RTRIM(str)

    //SPACE(N)

    //SUBSTRING(str,pos) , SUBSTRING(str FROM pos) SUBSTRING(str,pos,len) , SUBSTRING(str FROM pos FOR len)
    //SUBSTR()是 SUBSTRING()的同义词。
    //带有len参数的格式从字符串str返回一个长度同len字符相同的子字符串，起始于位置 pos。
    //使用 FROM的格式为标准 SQL 语法。也可能对pos使用一个负值。
    //假若这样，则子字符串的位置起始于字符串结尾的pos 字符，而不是字符串的开头位置。

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

class GlobalFunction(functionName: String) {
    def call(args: List[DataCell]): DataCell = {
        //Class.forName("io.qross.pql.GlobalFunction")
        classOf[GlobalFunction]
            .getDeclaredMethod(functionName, Class.forName("scala.collection.immutable.List"))
            .invoke(null, args).asInstanceOf[DataCell]
    }
}

object GlobalFunctionNames {
    private val NAMES: Set[String] = GlobalFunction.getClass.getDeclaredMethods.map(m => m.getName).filter(n => "^[A-Z]".r.test(n)).toSet

    def contains(name: String): Boolean = {
        NAMES.contains(name)
    }
}