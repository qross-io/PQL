package io.qross.pql

import java.time.DayOfWeek

import io.qross.core.{DataCell, DataHub, DataRow, DataType}
import io.qross.exception.SQLParseException
import io.qross.ext.TypeExt._
import io.qross.jdbc.DataSource
import io.qross.time.DateTime
import io.qross.time.TimeSpan._
import scala.collection.JavaConverters._
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

    //更新一个函数
    def renew(functionName: String, owner: Int = 0): Unit = {
        val row = DataSource.QROSS.queryDataRow("SELECT function_args, function_statement FROM qross_functions WHERE function_name=? AND owner=?", functionName, owner)
        put(functionName, row.getString("function_args"), row.getString("function_statement"), owner)
    }

    def remove(functionName: String, owner: Int = 0): Unit = {
        if (owner == 0) {
            SYSTEM.remove(functionName)
        }
        else {
            USER.remove(functionName)
        }
    }

    def put(functionName: String, functionArgs: String, functionStatement: String): Unit = {
        put(functionName, functionArgs, functionStatement, 0)
    }

    def put(functionName: String, functionArgs: String, functionStatement: String, owner: java.lang.Integer): Unit = {
        if (owner == 0) {
            SYSTEM += functionName -> new GlobalFunction(functionName, functionArgs, functionStatement)
        }
        else {
            USER += functionName -> new GlobalFunction(functionName, functionArgs, functionStatement)
        }
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

    //----------- 数字 -------------------------

    def ABS(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            args.head.asDecimal.abs.dataCell
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @ABS, expect 1, actual 0")
        }
    }

    def ACOS(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            Math.acos(args.head.asDecimal(0)).dataCell
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @ACOS, expect 1, actual 0")
        }
    }

    def ASIN(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            Math.asin(args.head.asDecimal(0)).dataCell
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @ASIN, expect 1, actual 0")
        }
    }

    def ATAN(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            Math.atan(args.head.asDecimal(0)).dataCell
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @ATAN, expect 1, actual 0")
        }
    }

    def CEIL(args: List[DataCell]): DataCell = {
        if (args.size == 1) {
            args.head.asDecimal.ceil(0).dataCell
        }
        else if (args.size > 1) {
            args.head.asDecimal.ceil(args.last.asInteger(0).toInt).dataCell
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @CEIL, expect 1 or 2, actual 0")
        }
    }

    def COS(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            Math.cos(args.head.asDecimal(0)).dataCell
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @COS, expect 1, actual 0")
        }
    }

    def COT(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            (1 / Math.tan(args.head.asDecimal(1))).dataCell
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @COT, expect 1, actual 0")
        }
    }

    def EXP(args: List[DataCell]): DataCell = {
        if (args.size > 1) {
            DataCell(Math.exp(args.head.asInteger(1).toInt), DataType.DECIMAL)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @EXP, expect 2, actual " + args.size)
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
            throw new SQLParseException(s"Incorrect arguments at @FLOOR, expect 1 or 2, actual 0")
        }
    }

    def LOG(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(Math.log(args.head.asDecimal(0)), DataType.DECIMAL)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @LOG, expect 1, actual 0")
        }
    }

    def LOG10(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            Math.log10(args.head.asDecimal(0)).dataCell
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @LOG, expect 1, actual 0")
        }
    }

    def MIN(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.size == 1) {
                if (args.head.isJavaList) {
                    args.head.asJavaList.asScala.map(_.toDecimal(0)).min.dataCell
                }
                else {
                    args.head.asDecimal(0).dataCell
                }
            }
            else {
                args.map(_.asDecimal(0)).min.dataCell
                //reduce((a, b) => if (a < b) a else b)
            }
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @MIN, expect least 1, actual 0")
        }
    }

    def MAX(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.size == 1) {
                if (args.head.isJavaList) {
                    args.head.asJavaList.asScala.map(_.toDecimal(0)).max.dataCell
                }
                else {
                    args.head.asDecimal(0).dataCell
                }
            }
            else {
                args.map(_.asDecimal(0)).max.dataCell
                //reduce((a, b) => if (a > b) a else b)
            }
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @MAX, expect least 1, actual 0")
        }
    }

    def MOD(args: List[DataCell]): DataCell = {
        if (args.size == 2) {
            DataCell(args.head.asInteger(0) % args(1).asInteger(1), DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @MOD, expect 2, actual " + args.size)
        }
    }

    def POW(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            val m = {
                if (args.size > 1) {
                    args(1).asDecimal(2)
                }
                else {
                    2
                }
            }
            Math.pow(args.head.asDecimal(0), m).dataCell
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @POW, expect 1 or 2, actual 0")
        }
    }

    def POWER(args: List[DataCell]): DataCell = {
        POW(args)
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
            DataCell(Math.random(), DataType.DECIMAL)
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
            throw new SQLParseException(s"Incorrect arguments at @ROUND, expect 1 or 2, actual 0")
        }
    }

    def SIGN(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            val m = args.head.asDecimal(0)
            if (m > 0) {
                DataCell(1, DataType.INTEGER)
            }
            else if (m < 0) {
                DataCell(-1, DataType.INTEGER)
            }
            else {
                DataCell(0, DataType.INTEGER)
            }
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @SIGN, expect 1, actual 0")
        }
    }

    def SIN(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            Math.sin(args.head.asDecimal(0)).dataCell
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @SIN, expect 1, actual 0")
        }
    }

    def SQRT(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            Math.sqrt(args.head.asDecimal(0)).dataCell
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @SQRT, expect 1 or 2, actual 0")
        }
    }

    def TAN(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            Math.tan(args.head.asDecimal(0)).dataCell
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at @TAN, expect 1, actual 0")
        }
    }

    def PI(args: List[DataCell]): DataCell = {
        DataCell(Math.PI, DataType.DECIMAL)
    }

    def AVG(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.size == 1) {
                if (args.head.isJavaList) {
                    val list = args.head.asJavaList.asScala
                    (list.map(_.toDecimal(0)).sum / list.size).dataCell
                }
                else {
                    args.head
                }
            }
            else {
                (args.map(_.value.toDecimal(0)).sum / args.size).dataCell
            }
        }
        else {
            throw new SQLParseException(s"Empty arguments at @AVG, expect 1 or more, actual 0")
        }
    }

    def SUM(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.size == 1) {
                if (args.head.isJavaList) {
                    args.head.asJavaList.asScala.map(_.toDecimal(0)).sum.dataCell
                }
                else {
                    args.head
                }
            }
            else {
                args.map(_.value.toDecimal(0)).sum.dataCell
            }
        }
        else {
            throw new SQLParseException(s"Empty arguments at @SUM, expect 1 or more, actual 0")
        }
    }

    def VAR(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.size == 1) {
                if (args.head.isJavaList) {
                    args.head.asJavaList.asScala.toList.sampleVariance.dataCell
                }
                else {
                    args.head
                }
            }
            else {
                args.map(_.value.toDecimal(0)).sampleVariance.dataCell
            }
        }
        else {
            throw new SQLParseException(s"Empty arguments at @VAR, expect 1 or more, actual 0")
        }
    }

    def VARP(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.size == 1) {
                if (args.head.isJavaList) {
                    args.head.asJavaList.asScala.toList.variance.dataCell
                }
                else {
                    args.head
                }
            }
            else {
                args.map(_.value.toDecimal(0)).variance.dataCell
            }
        }
        else {
            throw new SQLParseException(s"Empty arguments at @VARP, expect 1 or more, actual 0")
        }
    }

    def STDEVP(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.size == 1) {
                if (args.head.isJavaList) {
                    args.head.asJavaList.asScala.toList.deviation.dataCell
                }
                else {
                    args.head
                }
            }
            else {
                args.map(_.value.toDecimal(0)).deviation.dataCell
            }
        }
        else {
            throw new SQLParseException(s"Empty arguments at @STDEV, expect 1 or more, actual 0")
        }
    }

    def STDEV(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.size == 1) {
                if (args.head.isJavaList) {
                    args.head.asJavaList.asScala.toList.sampleDeviation.dataCell
                }
                else {
                    args.head
                }
            }
            else {
                args.map(_.value.toDecimal(0)).sampleDeviation.dataCell
            }
        }
        else {
            throw new SQLParseException(s"Empty arguments at @STDEVP, expect 1 or more, actual 0")
        }
    }

    def CV(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.size == 1) {
                if (args.head.isJavaList) {
                    args.head.asJavaList.asScala.toList.varianceCoefficient.dataCell
                }
                else {
                    args.head
                }
            }
            else {
                args.map(_.value.toDecimal(0)).varianceCoefficient.dataCell
            }
        }
        else {
            throw new SQLParseException(s"Empty arguments at @CV, expect 1 or more, actual 0")
        }
    }

    def CSV(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.size == 1) {
                if (args.head.isJavaList) {
                    args.head.asJavaList.asScala.toList.sampleVarianceCoefficient.dataCell
                }
                else {
                    args.head
                }
            }
            else {
                args.map(_.value.toDecimal(0)).sampleVarianceCoefficient.dataCell
            }
        }
        else {
            throw new SQLParseException(s"Empty arguments at @CVP, expect 1 or more, actual 0")
        }
    }

    //---------------- 日期时间 ----------------

    def DAYOFWEEK(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getDayOfWeek, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @DAYOFWEEK, expect 1, actual 0")
        }
    }

    def DAYOFMONTH(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getDayOfMonth, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @DAYOFMONTH, expect 1, actual 0")
        }
    }

    def DAY(args: List[DataCell]): DataCell = {
        DAYOFMONTH(args)
    }

    def DAYOFYEAR(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getDayOfYear, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @DAYOFYEAR, expect 1, actual 0")
        }
    }

    def DAYNAME(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getWeekName, DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @DAYNAME, expect 1, actual 0")
        }
    }

    def WEEKNAME(args: List[DataCell]): DataCell = {
        DAYNAME(args)
    }

    def FULLDAYNAME(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getFullWeekName, DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @DAYNAME, expect 1, actual 0")
        }
    }

    def FULLWEEKNAME(args: List[DataCell]): DataCell = {
        FULLDAYNAME(args)
    }

    def YEAR(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getYear, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @YEAR, expect 1, actual 0")
        }
    }

    def QUARTER(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getQuarter, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @QUARTER, expect 1, actual 0")
        }
    }

    def QUARTERNAME(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getQuarterName, DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @QUARTERNAME, expect 1, actual 0")
        }
    }

    def FULLQUARTERNAME(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getFullQuarterName, DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @QUARTERNAME, expect 1, actual 0")
        }
    }

    def MONTH(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getMonth, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @MONTH, expect 1, actual 0")
        }
    }

    def MONTHNAME(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getMonthName, DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @MONTHNAME, expect 1, actual 0")
        }
    }

    def FULLMONTHNAME(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getFullMonthName, DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @FULLMONTHNAME, expect 1, actual 0")
        }
    }

    def WEEK(args: List[DataCell]): DataCell = {
        DAYOFWEEK(args)
    }

    def WEEKOFYEAR(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            val first = {
                if (args.length > 1) {
                    args(1).asText("1").take(2).toUpperCase() match {
                        case "1" | "MO" => DayOfWeek.MONDAY
                        case "2" | "TU" => DayOfWeek.TUESDAY
                        case "3" | "WE" => DayOfWeek.WEDNESDAY
                        case "4" | "TH" => DayOfWeek.THURSDAY
                        case "5" | "FR" => DayOfWeek.FRIDAY
                        case "6" | "SA" => DayOfWeek.SATURDAY
                        case "0" | "7" | "SU" => DayOfWeek.SUNDAY
                        case _ => DayOfWeek.MONDAY
                    }
                }
                else {
                    DayOfWeek.MONDAY
                }
            }

            DataCell(args.head.asDateTime.getWeekOfYear(first), DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @WEEK, expect 1 or 2, actual 0")
        }
    }

    def HOUR(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getHour, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @HOUR, expect 1, actual 0")
        }
    }

    def MINUTE(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getMinute, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @MINUTE, expect 1, actual 0")
        }
    }

    def SECOND(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getSecond, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @SECOND, expect 1, actual 0")
        }
    }

    def MILLI(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getMilli, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @MILLI, expect 1, actual 0")
        }
    }

    def MICRO(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getMicro, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @MICRO, expect 1, actual 0")
        }
    }

    def NANO(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(args.head.asDateTime.getNano, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Empty arguments at @MILLI, expect 1, actual 0")
        }
    }

    /*
    + SECOND 秒 SECONDS
    + MINUTE 分钟 MINUTES
    + HOUR 时间 HOURS
    + DAY 天 DAYS
    + MONTH 月 MONTHS
    + YEAR 年 YEARS
    + MINUTE_SECOND 分钟和秒 "MINUTES:SECONDS"
    + HOUR_MINUTE 小时和分钟 "HOURS:MINUTES"
    + DAY_HOUR 天和小时 "DAYS HOURS"
    + YEAR_MONTH 年和月 "YEARS-MONTHS"
    + HOUR_SECOND 小时, 分钟， "HOURS:MINUTES:SECONDS"
    + DAY_MINUTE 天, 小时, 分钟 "DAYS HOURS:MINUTES"
    + DAY_SECOND 天, 小时, 分钟, 秒 "DAYS HOURS:MINUTES:SECONDS"
     */

    def TIMESTAMPDIFF(args: List[DataCell]): DataCell = {
        if (args.size == 3) {
            args.head.asText("SECOND").toUpperCase() match {
                case "MILLI" | "MILLIS" => DataCell(args(2).asDateTime.later(args(1).asDateTime), DataType.INTEGER)
                case "SECOND" | "SECONDS" => DataCell(args(2).asDateTime.later(args(1).asDateTime).toSeconds, DataType.INTEGER)
                case "MINUTE" | "MINUTES" => DataCell(args(2).asDateTime.later(args(1).asDateTime).toMinutes, DataType.INTEGER)
                case "HOUR" | "HOURS" => DataCell(args(2).asDateTime.later(args(1).asDateTime).toHours, DataType.INTEGER)
                case "DAY" | "DAYS" => DataCell(args(2).asDateTime.later(args(1).asDateTime).toDays, DataType.INTEGER)
                case "MONTH" | "MONTHS" =>
                        val time1 = args(1).asDateTime
                        val time2 = args(2).asDateTime
                        var months = (time2.year - time1.year) * 12 + (time2.month - time1.month)
                        if (time1.before(time2)) {
                            if (time1.plusMonths(months).after(time2)) {
                                months -= 1
                            }
                        }
                        else if (time1.after(time2)) {
                            if (time1.plusYears(months).before(time2)) {
                                months += 1
                            }
                        }
                        DataCell(months, DataType.INTEGER)
                case "YEAR" | "YEARS" =>
                        val time1 = args(1).asDateTime
                        val time2 = args(2).asDateTime
                        var years = time2.year - time1.year
                        if (time1.before(time2)) {
                            if (time1.plusYears(years).after(time2)) {
                                years -= 1
                            }
                        }
                        else if (time1.after(time2)) {
                            if (time1.plusYears(years).before(time2)) {
                                years += 1
                            }
                        }
                        DataCell(years, DataType.INTEGER)
                case _ =>
                    throw new SQLParseException("Incorrect argument at @TIMESTAMPDIFF, the first argument must be a string in 'MILLI|SECOND|MINUTE|HOUR|DAY|MONTH|YEAR'.")
            }
        }
        else {
            throw new SQLParseException(s"Empty or miss arguments at @TIMESTAMPDIFF, expect 3, actual " + args.size)
        }
    }

    def DATE_ADD(args: List[DataCell]): DataCell = {
        if (args.size >= 3) {
            DataCell(
                args(2).asText.toUpperCase() match {
                    case "MILLI" | "MILLIS" => args.head.asDateTime.plusMillis(args(1).asInteger(0))
                    case "SECOND" | "SECONDS" => args.head.asDateTime.plusSeconds(args(1).asInteger(0))
                    case "MINUTE" | "MINUTES" => args.head.asDateTime.plusMinutes(args(1).asInteger(0))
                    case "HOUR" | "HOURS" => args.head.asDateTime.plusHours(args(1).asInteger(0))
                    case "DAY" | "DAYS" => args.head.asDateTime.plusDays(args(1).asInteger(0))
                    case "MONTH" | "MONTHS" => args.head.asDateTime.plusMonths(args(1).asInteger(0))
                    case "YEAR" | "YEARS" => args.head.asDateTime.plusYears(args(1).asInteger(0))
                    case _ => throw new SQLParseException("Incorrect argument at @DATE_ADD, the third argument must be a string in 'MILLI|SECOND|MINUTE|HOUR|DAY|MONTH|YEAR'.")
                }, DataType.DATETIME)
        }
        else {
            throw new SQLParseException(s"Empty or miss arguments at @DATE_ADD, expect 3, actual " + args.size)
        }
    }

    def DATE_SUB(args: List[DataCell]): DataCell = {
        if (args.size >= 3) {
            DataCell(
                args(2).asText.toUpperCase() match {
                    case "MILLI" | "MILLIS" => args.head.asDateTime.minusMillis(args(1).asInteger(0))
                    case "SECOND" | "SECONDS" => args.head.asDateTime.minusSeconds(args(1).asInteger(0))
                    case "MINUTE" | "MINUTES" => args.head.asDateTime.minusMinutes(args(1).asInteger(0))
                    case "HOUR" | "HOURS" => args.head.asDateTime.minusHours(args(1).asInteger(0))
                    case "DAY" | "DAYS" => args.head.asDateTime.minusDays(args(1).asInteger(0))
                    case "MONTH" | "MONTHS" => args.head.asDateTime.minusMonths(args(1).asInteger(0))
                    case "YEAR" | "YEARS" => args.head.asDateTime.minusYears(args(1).asInteger(0))
                    case _ => throw new SQLParseException("Incorrect argument at @DATE_ADD, the third argument must be a string in 'SECOND|MINUTE|HOUR|DAY|MONTH|YEAR'.")
                }, DataType.DATETIME)
        }
        else {
            throw new SQLParseException(s"Empty or miss arguments at @DATE_SUB, expect 3, actual " + args.size)
        }
    }

    def ADDDATE(args: List[DataCell]): DataCell = {
        DATE_ADD(args)
    }

    def SUBDATE(args: List[DataCell]): DataCell = {
        DATE_SUB(args)
    }

    def DATE_FORMAT(args: List[DataCell]): DataCell = {
        if (args.size >= 2) {
            DataCell(args.head.asDateTime.format(args(1).asText("yyyy-MM-dd HH:mm:ss")), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Empty or miss arguments at @DATE_FORMAT, expect 2, actual " + args.size)
        }
    }
    def DATETIME_FORMAT(args: List[DataCell]): DataCell = {
        DATE_FORMAT(args)
    }

    def CURDATE(args: List[DataCell]): DataCell = {
        CURRENT_DATE(args)
    }

    def CURRENT_DATE(args: List[DataCell]): DataCell = {
        DataCell(DateTime.now.getString("yyyy-MM-dd"), DataType.TEXT)
    }

    def CURTIME(args: List[DataCell]): DataCell = {
        CURRENT_TIME(args)
    }

    def CURRENT_TIME(args: List[DataCell]): DataCell = {
        DataCell(DateTime.now.getString("HH:mm:ss"), DataType.TEXT)
    }

    def NOW(args: List[DataCell]): DataCell = {
        DataCell(DateTime.now, DataType.DATETIME)
    }

    def CURRENT_DATETIME(args: List[DataCell]): DataCell = {
        DataCell(DateTime.now, DataType.DATETIME)
    }

    def CURRENT_TIMESTAMP(args: List[DataCell]): DataCell = {
        DataCell(DateTime.now.toEpochSecond, DataType.INTEGER)
    }

    def UNIX_TIMESTAMP(args: List[DataCell]): DataCell = {
        if (args.isEmpty) {
            DataCell(DateTime.now.toEpochSecond, DataType.INTEGER)
        }
        else {
            DataCell(args.head.asDateTime.toEpochSecond, DataType.INTEGER)
        }
    }

    def FROM_UNIXTIME(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.size == 1) {
                DataCell(args.head.asDateTime("yyyy-MM-dd HH:mm:ss"), DataType.TEXT)
            }
            else {
                DataCell(args.head.asDateTime(args(1).asText("yyyy-MM-dd HH:mm:ss")), DataType.TEXT)
            }
        }
        else {
            throw new SQLParseException(s"Empty arguments at @TIMESTAMPDIFF, expect 1 or 2, actual 0")
        }
    }

    def SEC_TO_TIME(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            var seconds = args.head.asInteger(-1) % (3600 * 24)
            if (seconds > -1) {
                val hour = seconds / 3600
                seconds = seconds % 3600
                val minute = seconds / 60
                val second = seconds % 60
                DataCell(f"$hour%02d:$minute%02d:$second%02d", DataType.TEXT)
            }
            else {
                throw new SQLParseException(s"Incorrect argument at @SEC_TO_TIME, must be a integer.")
            }
        }
        else {
            throw new SQLParseException(s"Empty arguments at @SEC_TO_TIME, expect 1, actual 0")
        }
    }

    def TIME_TO_SEC(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            "^(\\d{2}):(\\d{2}):(\\d{2})$".r.findFirstMatchIn(args.head.asText) match {
                case Some(m) => DataCell(m.group(1).toInt * 3600 + m.group(2).toInt * 60 + m.group(3).toInt, DataType.INTEGER)
                case None => throw new SQLParseException(s"Incorrect argument at @TIME_TO_SEC, its format must be 'HH:mm:ss'.")
            }
        }
        else {
            throw new SQLParseException(s"Empty arguments at @TIME_TO_SEC, expect 1, actual 0")
        }
    }

    /*

    18. `TO_DAYS(date)` 返回日期 date 是西元 0 年至今多少天，不计算 1582 年以前。
    ```sql
    select TO_DAYS(950501); --728779
    select TO_DAYS('1997-10-07'); --729669
    ```
    19. `FROM_DAYS(N)` 给出西元 0 年至今多少天返回 DATE 值，不计算1582年以前。
    ```sql
    select FROM_DAYS(729669); -- '1997-10-07'
    ```


    21. `TIME_FORMAT(time,format)` 和 DATE_FORMAT() 类似，但 TIME_FORMAT 只处理小时、分钟和秒，其余符号产生一个`NULL`值或`0`。

     14. `PERIOD_ADD(P,N)` 增加 N 个月到时期 P 并返回（P 的格式 YYMM 或 YYYYMM）
    ```sql
    select PERIOD_ADD(9801,2); -- 199803
    ```
    15. `PERIOD_DIFF(P1, P2)` 返回在时期 P1 和 P2 之间月数（P1 和 P2 的格式 YYMM 或 YYYYMM）
    ```sql
    select PERIOD_DIFF(9802,199703); -- 11
    ```

    */

    def REGEXP_LIKE(args: List[DataCell]): DataCell = {
        if (args.size == 2) {
            DataCell(if (args.last.asText.r.findFirstIn(args.head.asText).nonEmpty) 1 else 0, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Empty or miss arguments at @REGEXP_LIKE, expect 2, actual " + args.size)
        }
    }

    def REGEXP_INSTR(args: List[DataCell]): DataCell = {
        if (args.size == 2) {
            val text = args.head.asText
            args.last.asText.r.findFirstIn(text) match {
                case Some(v) => DataCell(text.indexOf(v) + 1, DataType.INTEGER)
                case None => DataCell(0, DataType.INTEGER)
            }
        }
        else {
            throw new SQLParseException(s"Empty or miss arguments at @REGEXP_INSTR, expect 2, actual " + args.size)
        }
    }

    def REGEXP_SUBSTR(args: List[DataCell]): DataCell = {
        if (args.size == 2) {
            args.last.asText.r.findFirstIn(args.head.asText) match {
                case Some(v) => DataCell(v, DataType.TEXT)
                case None => DataCell.NULL
            }
        }
        else {
            throw new SQLParseException(s"Empty or miss arguments at @REGEXP_SUBSTR, expect 2, actual " + args.size)
        }
    }

    def REGEXP_REPLACE(args: List[DataCell]): DataCell = {
        if (args.size == 3) {
            val text = args.head.asText
            args(1).asText.r.findFirstIn(args.head.asText) match {
                case Some(v) => DataCell(text.replace(v, args.last.asText), DataType.TEXT)
                case None => DataCell(text, DataType.TEXT)
            }
        }
        else {
            throw new SQLParseException(s"Empty or miss arguments at @REGEXP_REPLACE, expect 3, actual " + args.size)
        }
    }

    def IFNULL(args: List[DataCell]): DataCell = {
        if (args.size == 2) {
            if (args.head.isNull) {
                args.last
            }
            else {
                args.head
            }
        }
        else {
            throw new SQLParseException(s"Empty or miss arguments at @IFNULL, expect 2, actual " + args.size)
        }
    }

    def NVL(args: List[DataCell]): DataCell = {
        if (args.size == 2) {
            if (args.head.isNull) {
                args.last
            }
            else {
                args.head
            }
        }
        else {
            throw new SQLParseException(s"Empty or miss arguments at @NVL, expect 2, actual " + args.size)
        }
    }
}

class GlobalFunctionDeclaration {

}