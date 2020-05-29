package io.qross.pql

import java.util.regex.Matcher

import io.qross.core.{DataCell, DataType}
import io.qross.exception.SQLParseException
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns._
import io.qross.pql.Solver._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

class ConditionGroup(expression: String) {

    private val conditions = new ArrayBuffer[Condition]()

    def evalAll(PQL: PQL): Boolean = {

        //解析表达式
        var exp = expression.trim().$clean(PQL)

        //replace SELECT to ~value[n]
        breakable {
            while (true) {
                $SELECT$.findFirstMatchIn(exp) match {
                    case Some(m) =>
                        val select = findOutSelect(exp, m.group(0))
                        exp = exp.replace(select, PQL.$stash(DataCell(
                            PQL.dh.executeJavaList(select.$trim("(", ")").$restore(PQL)),
                            DataType.ARRAY
                        )))
                    case _ => break
                }
            }
        }

        //处理IN (), 因为小括号会与条件分组冲突, 即去掉小括号
        IN$$.findAllIn(exp).foreach(in$$ => {
            exp = exp.replace(in$$, " IN ~u0028" + in$$.takeBetween("(", ")").replaceAll("""\s""", "") + "~u0029")
        })

        // 解析括号中的逻辑 () 并将表达式分步
        breakable {
            while (true) {
                $BRACKET.findFirstMatchIn(exp) match {
                    case Some(m) =>
                        parseBasicExpression(m.group(1).trim)
                        exp = exp.replace(m.group(0), "~condition[" + (this.conditions.size - 1) + "]")
                    case _ => break
                }
            }
        }

        //finally
        parseBasicExpression(exp)

        //最终执行
        for (condition <- this.conditions) {
            val field = condition.field
            val value = condition.value

            condition.eval( if (field == null) {
                                DataCell.NULL
                            }
                            else if ($CONDITION$N.test(field)) {
                                conditions(field.$trim("~condition[", "]").toInt).result.toDataCell(DataType.BOOLEAN)
                            }
                            else if ($VARIABLE.test(field)) {
                                PQL.findVariable(field)
                            }
                            else if (value.equalsIgnoreCase("DEFINED") || value.equalsIgnoreCase("UNDEFINED")) {
                                field.$restore(PQL).toDataCell(DataType.TEXT)
                            }
                            else {
                                field.$sharp(PQL)
                            },
                            if ($CONDITION$N.test(value)) {
                                DataCell(conditions(value.$trim("~condition[", "]").toInt).result, DataType.BOOLEAN)
                            }
                            else if ($VARIABLE.test(value)) {
                                PQL.findVariable(value)
                            }
                            else if (value.equalsIgnoreCase("EMPTY")) {
                                DataCell.EMPTY
                            }
                            else if (value.equalsIgnoreCase("NULL") || value == "()") {
                                DataCell.NULL
                            }
                            else if (condition.operator == "NOT") {
                                value.replace("==", "DOUBLE$EQUALITY$SIGN")
                                    .replace("!=", "INEQUALITY$SIGN")
                                    .replace("=", "==")
                                    .replace("DOUBLE$EQUALITY$SIGN", "==")
                                    .replace("INEQUALITY$SIGN", "!=")
                                    .replace("<>", "!=").$sharp(PQL, "\"")
                            }
                            else {
                                value.$sharp(PQL, "\"")
                            })

            if (PQL.dh.debugging) {
                Output.writeDotLine(" ", if (field != null) field.$restore(PQL, "\"") else "", condition.operator, value.$restore(PQL, "\""), " => ", condition.result)
            }
        }

        val result = conditions.last.result

        conditions.clear()

        result
    }

    private def findOutSelect(expression: String, head: String): String = {
        var start: Int = 0
        val begin: Int = expression.indexOf(head, start) + 1
        var end: Int = expression.indexOf(")", start)

        val brackets = new mutable.ArrayStack[String]
        brackets.push("(")
        start = begin

        while (brackets.nonEmpty && expression.indexOf(")", start) > - 1) {
            val left: Int = expression.indexOf("(", start)
            val right: Int = expression.indexOf(")", start)
            if (left > -1 && left < right) {
                brackets.push("(")
                start = left + 1
            }
            else {
                brackets.pop()
                start = right + 1
                if (right > end) end = right
            }
        }

        if (brackets.nonEmpty) {
            throw new SQLParseException("Can't find closed bracket for SELECT: " + expression)
        }
        else {
            expression.substring(begin - 1, end + 1)
        }
    }

    //解析无括号()的表达式
    def parseBasicExpression(expression: String): Unit = {
        var m: Matcher = null
        var n: Matcher = null
        var left = ""
        var right = ""
        var clause = ""
        var exp = expression

//        A$AND$Z.findAllIn(exp).foreach($and => {
//            exp = exp.replace($and, $and.replace(BLANKS, "\\$"))
//        })

        //SHARP表达式可能包含OR关键词
        A$OR$Z.findAllIn(exp).foreach($or => {
            exp = exp.replace($or, $or.replaceAll(BLANKS, "\\$"))
        })

        //AND
        while ({m = $AND$.matcher(exp); m}.find) {
            clause = m.group(2)
            left = m.group(3)
            right = m.group(4)

            //处理多余的OR
            while ({n = $OR.matcher(clause); n}.find) {
                clause = clause.takeAfter(n.group(0))
                left = left.takeAfter(n.group(0))
            }

            if (!$CONDITION$N.test(left)) {
                exp = exp.replace(left, stash)
                clause = clause.replace(left, stash)
                conditions += new Condition(left.trim)
            }

            if (!$CONDITION$N.test(right)) {
                exp = exp.replace(right, stash)
                clause = clause.replace(right, stash)
                conditions += new Condition(right.trim)
            }

            exp = exp.replace(clause, stash)
            conditions += new Condition(clause.trim) // left AND right

        }
        //OR
        while ({m = $OR$.matcher(exp); m}.find) {
            clause = m.group(2)
            left = m.group(3)
            right = m.group(4)

            if (!$CONDITION$N.test(left)) {
                exp = exp.replace(left, stash)
                clause = clause.replace(left, stash)
                conditions += new Condition(left.trim)
            }

            if (!$CONDITION$N.test(right)) {
                exp = exp.replace(right, stash)
                clause = clause.replace(right, stash)
                conditions += new Condition(right.trim)
            }

            exp = exp.replace(clause, stash)
            this.conditions += new Condition(clause.trim) // left OR right

        }

        //SINGLE
        if (!$CONDITION$N.test(exp)) {
            conditions += new Condition(exp.trim)
        }
    }

    private def stash: String = {
        "~condition[" + this.conditions.size + "]"
    }
}
