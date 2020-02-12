package io.qross.pql

import io.qross.core.DataCell
import io.qross.pql.Patterns.$IF
import io.qross.ext.TypeExt._
import scala.util.control.Breaks.{break, breakable}
import io.qross.pql.Solver._

object IF {

    def parse(sentence: String, PQL: PQL): Unit = {
        $IF.findFirstMatchIn(sentence) match {
            case Some(m) =>
                val $if: Statement = new Statement("IF", sentence, new IF(m.group(1)))
                PQL.PARSING.head.addStatement($if)
                //只进栈
                PQL.PARSING.push($if)
                //待关闭的控制语句
                PQL.TO_BE_CLOSE.push($if)
                //继续解析第一条子语句
                val first = sentence.takeAfter(m.group(0)).trim()
                if (first != "") {
                    PQL.parseStatement(first)
                }
            case None => throw new SQLParseException("Incorrect IF sentence: " + sentence)
        }
    }

    //IF短表达式   IF $a > 0 THEN 0 ELSIF $a < 0 THEN 1 ELSE 2 END;
    def express(expression: String, PQL: PQL): DataCell = {

        var sentence = expression
        val words = """\b(IF|THEN|ELSIF|ELSE|END)\b""".r.findAllIn(sentence).toList
        var met = false
        var result = ""
        var wrong = ""

        breakable {
            for (i <- words.indices) {
                val prev = if (i > 0) words(i - 1).toUpperCase() else ""
                words(i).toUpperCase() match {
                    case "IF" => sentence = sentence.takeAfter(words(i))
                    case "THEN" =>
                        if (prev == "IF" || prev == "ELSIF") {
                            met = new ConditionGroup(sentence.takeBefore(words(i))).evalAll(PQL)
                        }
                        else {
                            wrong = "miss IF or ELSIF"
                            break
                        }
                        sentence = sentence.takeAfter(words(i))
                    case "ELSIF" | "ELSE" =>
                        if (prev == "THEN") {
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

class IF(conditions: String) {

    def execute(PQL: PQL, statement: Statement): Unit = {
        if (new ConditionGroup(conditions).evalAll(PQL)) {
            PQL.IF$BRANCHES.push(true)
            PQL.EXECUTING.push(statement)
            PQL.executeStatements(statement.statements)
        }
        else {
            PQL.IF$BRANCHES.push(false)
        }
    }
}
