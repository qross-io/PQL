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
        "CONCAT", "CHARINDEX", "INSTR", "POSITION", "SUBSTR", "LEFT", "RIGHT", "REPLACE", "LOWER", "UPPER", "TRIM", "SPLIT", "LEN",
        "IFNULL", "NVL",
        "REGEX_LIKE", "REGEX_INSTR", "REGEX_SUBSTR", "REGEX_REPLACE") */

    def CONCAT(args: List[DataCell]): DataCell = {
        DataCell(args.map(s => s.asText).mkString(""), DataType.TEXT)
    }

    def POSITION(args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            val $in = """\sIN\s""".r
            val string = args.head.asText
            if ($in.test(string)) {
                DataCell(string.takeBeforeX($in).trim().indexOf(string.takeAfterX($in)) + 1, DataType.INTEGER)
            }
            else {
                throw new SQLParseException(s"Wrong or empty arguments, correct format is POSITION(strA IN strB) , actual POSITION(${args.map(s => s.asText).mkString("")})")
            }
        }
        else {
            throw new SQLParseException(s"Eempty arguments at @POSITION method")
        }
    }

    def INSTR(args: List[DataCell]): DataCell = {
        if (args.size >= 2) {
            DataCell(args.head.asText.indexOf(args(1).asText) + 1, DataType.INTEGER)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at INSTR, expect 2, actual ${args.size}")
        }
    }

    //CHARINDEX(stringToFind, stringToSearch, startLocation)
    def CHARINDEX(args: List[DataCell]): DataCell = {
        if (args.size == 2) {
            DataCell("")
        }
        else if (args.size == 3) {
            DataCell("")
        }
        else {
            DataCell("")
        }
    }

    //REPLACE(stringToReplace, oldString, newString)
    def REPLACE(args: List[DataCell]): DataCell = {
        if (args.size == 3) {
            DataCell(args.head.asText.replace(args(1).asText, args(2).asText), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at REPLACE, expect 3, actual ${args.size}")
        }
    }

    //REPLACE(stringToReplace, oldString, newString)
    def LEFT(args: List[DataCell]): DataCell = {
        if (args.size == 2) {
            DataCell(args.head.asText.take(args(1).asInteger.toInt), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at REPLACE, expect 2, actual ${args.size}")
        }
    }

    //REPLACE(stringToReplace, oldString, newString)
    def RIGHT(args: List[DataCell]): DataCell = {
        if (args.size == 2) {
            DataCell(args.head.asText.takeRight(args(1).asInteger.toInt), DataType.TEXT)
        }
        else {
            throw new SQLParseException(s"Incorrect arguments at REPLACE, expect 2, actual ${args.size}")
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