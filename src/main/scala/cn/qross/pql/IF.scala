package cn.qross.pql

import cn.qross.core.{DataCell, DataType}
import cn.qross.exception.{SQLExecuteException, SQLParseException}
import cn.qross.pql.Patterns._
import cn.qross.ext.TypeExt._

import scala.util.control.Breaks.{break, breakable}
import cn.qross.pql.Solver._

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
}

class IF(conditions: String) {

    //IF短表达式   IF $a > 0 THEN 0 ELSIF $a < 0 THEN 1 ELSE 2 END;
    def evaluate(PQL: PQL, mode: Int = Solver.FULL): DataCell = {
        //先提取子语句
        var sentence = conditions.replaceInnerSentence(PQL)
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
                case None => throw new SQLExecuteException("Wrong IF expression, keyword END is needed. " + sentence)
            }
        }

        val words = $IFX.r.findAllIn(sentence).map(_.trim().toUpperCase()).toList
        val sections = sentence.split($IFX, -1).map(_.trim())
        var met = false
        var result = ""
        var wrong = ""

        breakable {
            for (i <- words.indices) {
                val prev = if (i > 0) words(i - 1) else ""
                words(i) match {
                    case "IF" =>
                    case "THEN" =>
                        if (prev == "IF" || prev == "ELSIF") {
                            met = new ConditionGroup(sections(i)).evalAll(PQL, replaced = true)
                        }
                        else {
                            wrong = "miss IF or ELSIF"
                            break
                        }
                    case "ELSIF" | "ELSE" =>
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
            throw new SQLExecuteException(s"Incorrect short expression IF format, $wrong: " + conditions)
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
