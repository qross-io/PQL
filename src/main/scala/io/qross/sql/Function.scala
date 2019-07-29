package io.qross.sql

import io.qross.core.DataCell
import io.qross.sql.Solver._
import io.qross.ext.TypeExt._

object Function {
    /*
        Set(
        "CONCAT", "CHARINDEX", "INSTR", "POSITION", "SUBSTR", "LEFT", "RIGHT", "REPLACE", "LOWER", "UPPER", "TRIM", "SPLIT", "LEN",
        "IFNULL", "NVL",
        "REGEX_LIKE", "REGEX_INSTR", "REGEX_SUBSTR", "REGEX_REPLACE") */

    def CONCAT(args: String*): String = {
        args.mkString("")//.useSingleQuotes()
    }

    def POSITION(args: String*): String = {
        val $in = """\sIN\s""".r
        if ($in.test(args(0))) {
            (args(0).takeBefore($in).trim().indexOf(args(0).takeAfter($in)) + 1).toString
        }
        else {
            throw new SQLParseException("Wrong arguments, correct format is POSITION(strA IN strB) , actual " + args(0))
        }
    }

    def INSTR(args: String*): DataCell = {
        //String.valueOf(args(0).indexOf(args(1)) + 1)
        DataCell("")
    }

    def CHARINDEX(args: String*): DataCell = {
        //""
        DataCell("")
    }

    def REPLACE(args: String*): DataCell = {
        //args(0).replace(args(1), args(2))//.useSingleQuotes()
        DataCell("")
    }

    def main(args: Array[String]): Unit = {

    }
}

case class Function(functionName: String) {
    def call(args: Array[String], PSQL: PSQL): DataCell = {
        Function.getClass.getDeclaredMethod(functionName).invoke(null, args.map(arg => arg.popStash(PSQL)): _*).asInstanceOf[DataCell]
    }
}

object FunctionNames {
    private val NAMES: Set[String] = Function.getClass.getDeclaredMethods.map(m => m.getName).filter(n => "^[A-Z]".r.test(n)).toSet

    def contains(name: String): Boolean = {
        NAMES.contains(name)
    }
}