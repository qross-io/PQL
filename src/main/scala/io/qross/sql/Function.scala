package io.qross.sql

import io.qross.core.{DataCell, DataType}
import io.qross.sql.Solver._
import io.qross.ext.TypeExt._

object Function {
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
                DataCell(string.takeBefore($in).trim().indexOf(string.takeAfter($in)) + 1, DataType.INTEGER)
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
            throw new SQLParseException(s"Incorrect arguments at REPLACE, expect 2, actual ${args.size}")
        }
    }
}

case class Function(functionName: String) {
    def call(args: List[DataCell]): DataCell = {
        Function.getClass.getDeclaredMethod(functionName).invoke(null, args).asInstanceOf[DataCell]
    }
}

object FunctionNames {
    private val NAMES: Set[String] = Function.getClass.getDeclaredMethods.map(m => m.getName).filter(n => "^[A-Z]".r.test(n)).toSet

    def contains(name: String): Boolean = {
        NAMES.contains(name)
    }
}