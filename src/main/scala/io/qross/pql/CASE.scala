package io.qross.pql

import io.qross.core.{DataCell, DataType}
import io.qross.exception.{SQLExecuteException, SQLParseException}
import io.qross.pql.Patterns._
import io.qross.ext.TypeExt._
import io.qross.pql.Solver._

import scala.util.control.Breaks.{break, breakable}

object CASE {
    def parse(sentence: String, PQL: PQL): Unit = {
        $CASE.findFirstIn(sentence) match {
            case Some(m) =>
                val $when = $WHEN$.findFirstIn(sentence).getOrElse("")
                val equivalent = {
                    if ($when != "") {
                        sentence.takeAfter($BLANK).takeBefore($when).trim()
                    }
                    else {
                        sentence.takeAfter(m).trim()
                    }
                }
                val $case: Statement = new Statement("CASE", sentence, new CASE(equivalent))
                PQL.PARSING.head.addStatement($case)
                //只进栈
                PQL.PARSING.push($case)
                //待关闭的控制语句
                PQL.TO_BE_CLOSE.push($case)
                //继续解析WHEN语句
                if ($when != "") {
                    PQL.parseStatement(sentence.substring(sentence.indexOf($when) + 1))
                }
            case None => throw new SQLParseException("Incorrect CASE sentence: " + sentence)
        }
    }

    //CASE短表达式   CASE WHEN $a > 0 THEN 0 ELSE 1 END 或  CASE value WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END
    def express(expression: String, PQL: PQL): DataCell = {

        var sentence = expression
        val words = """\b(CASE|WHEN|THEN|ELSE|END)\b""".r.findAllIn(sentence).toList
        var equivalent = DataCell.UNDEFINED
        var met = false
        var result = ""
        var wrong = ""

        /*
        CASE
            WHEN a THEN
                a
            WHEN b THEN
                b
            ELSE
                c
        END
        */

        breakable {
            for (i <- words.indices) {
                val prev = if (i > 0) words(i - 1).toUpperCase() else ""
                words(i).toUpperCase() match {
                    case "CASE" => sentence = sentence.takeAfter(words(i))
                    case "WHEN" =>
                        if (prev == "CASE") {
                            val equiv = sentence.takeBefore(words(i)).trim()
                            equivalent = if (equiv == "") DataCell(true, DataType.BOOLEAN) else if ($CONDITION.test(equiv)) new ConditionGroup(equiv).evalAll(PQL).toDataCell(DataType.BOOLEAN) else equiv.$eval(PQL)
                        }
                        else if (prev == "THEN") {
                            if (met) {
                                result = sentence.takeBefore(words(i))
                                break
                            }
                        }
                        else {
                            wrong = "miss THEN"
                            break
                        }
                        sentence = sentence.takeAfter(words(i))
                    case "THEN" =>
                        if (prev == "WHEN") {
                            met = {
                                if (equivalent.isBoolean) {
                                    new ConditionGroup(sentence.takeBefore(words(i))).evalAll(PQL) == equivalent.value
                                }
                                else {
                                    sentence.takeBefore(words(i)).$eval(PQL).value == equivalent.value
                                }
                            }
                        }
                        else {
                            wrong = "miss WHEN"
                            break
                        }
                        sentence = sentence.takeAfter(words(i))
                    case "ELSE" =>
                        if (prev == "THEN") {
                            if (met) {
                                result = sentence.takeBefore(words(i))
                                break
                            }
                            else {
                                sentence = sentence.takeAfter(words(i))
                            }
                        }
                        else {
                            wrong = "miss THEN"
                            break
                        }
                    case "END" =>
                        if (prev == "ELSE") {
                            if (!met) {
                                result = sentence.takeBefore(words(i))
                            }
                        }
                        else {
                            wrong = "miss ELSE"
                        }
                }
            }
        }

        if (wrong != "") {
            throw new SQLExecuteException(s"Wrong short expression IF format, $wrong: " + expression)
        }

        result.$eval(PQL)
    }
}

class CASE(equivalent: String) {
    def execute(PQL: PQL, statement: Statement): Unit = {
        PQL.EXECUTING.push(statement)
        PQL.CASE$WHEN.push(
            new Case$When(
                if (equivalent == "") {
                    DataCell(true, DataType.BOOLEAN)
                }
                else {
                    equivalent.$eval(PQL)
                }
            )
        )

        PQL.executeStatements(statement.statements)
    }
}

class Case$When(val equivalent: DataCell, var matched: Boolean = false) {

}
