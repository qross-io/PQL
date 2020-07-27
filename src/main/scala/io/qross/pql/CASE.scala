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
}

class CASE(equivalent: String) {

    //CASE短表达式   CASE WHEN $a > 0 THEN 0 ELSE 1 END 或  CASE value WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END
    def express(PQL: PQL, mode: Int = Solver.FULL): DataCell = {

        //先提取子语句
        var sentence = equivalent.replaceInnerSentence(PQL)
        sentence = {
            mode match {
                case 0 => sentence.$clean(PQL)
                case 1 => sentence.$express(PQL)
                case 2 => sentence
                case _ => sentence.$clean(PQL)
            }
        }

        val links = {
            $END$.findFirstMatchIn(sentence) match {
                case Some(m) =>
                    if (m.group(1).trim() == ARROW) {
                        val sharp = sentence.takeAfter(m.group(0))
                        sentence = sentence.takeBefore(sharp).dropRight(2).trim()
                        sharp
                    }
                    else {
                        ""
                    }
                case None => throw new SQLExecuteException("Wrong CASE expression, keyword END is needed. " + sentence)
            }
        }

        val words = $CASEX.r.findAllIn(sentence).map(_.trim().toUpperCase()).toList
        val sections = sentence.split($CASEX, -1).map(_.trim())
        var equals = DataCell.UNDEFINED
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
                val prev = if (i > 0) words(i - 1) else ""
                words(i) match {
                    case "CASE" =>
                    case "WHEN" =>
                        if (prev == "CASE") {
                            equals = if (sections(i) == "") DataCell(true, DataType.BOOLEAN) else if ($CONDITION.test(sections(i))) new ConditionGroup(sections(i)).evalAll(PQL, replaced = true).toDataCell(DataType.BOOLEAN) else sections(i).$eval(PQL)
                        }
                        else if (prev == "THEN") {
                            if (met) {
                                result = sections(i)
                                break
                            }
                        }
                        else {
                            wrong = "miss THEN"
                            break
                        }
                    case "THEN" =>
                        if (prev == "WHEN") {
                            met = {
                                if (equals.isBoolean) {
                                    new ConditionGroup(sections(i)).evalAll(PQL, replaced = true) == equals.value
                                }
                                else {
                                    sections(i).$eval(PQL).value == equals.value
                                }
                            }
                        }
                        else {
                            wrong = "miss WHEN"
                            break
                        }
                    case "ELSE" =>
                        if (prev == "THEN") {
                            if (met) {
                                result = sections(i)
                                break
                            }
                        }
                        else {
                            wrong = "miss THEN"
                            break
                        }
                    case "END" =>
                        if (prev == "ELSE") {
                            if (!met) {
                                result = sections(i)
                            }
                        }
                        else {
                            wrong = "miss ELSE"
                        }
                }
            }
        }

        if (wrong != "") {
            throw new SQLExecuteException(s"Incorrect short expression CASE format, $wrong: " + equivalent)
        }

        val data = {
            if (result.bracketsWith("~inner[", "]")) {
                result.restoreInnerSentence(PQL).$compute(PQL, mode)
            }
            else {
                result.$sharp(PQL)
            }
        }

        if (links != "") {
            new Sharp(links, data).execute(PQL)
        }
        else {
            data
        }
    }

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
