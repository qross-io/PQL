package cn.qross.fql

import cn.qross.core.{DataCell, DataRow}
import cn.qross.fql.Patterns.{$AND$, $BRACKET, $CONDITION$N, $OR, $OR$}
import cn.qross.ext.TypeExt._
import cn.qross.script.Script

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

class ConditionGroup(expression: String) {

    private[qross] val conditions = new ArrayBuffer[Condition]()
    private val booleans = new mutable.ArrayBuffer[Boolean]()

    def parse(): ConditionGroup = {
        // 解析括号中的逻辑 () 并将表达式分步
        var exp = expression.trim()
        breakable {
            while (true) {
                $BRACKET.findFirstMatchIn(exp) match {
                    case Some(n) =>
                        parseConditionExpression(n.group(1).trim)
                        exp = exp.replace(n.group(0), "~condition[" + (this.conditions.size - 1) + "]")
                    case _ => break
                }
            }
        }
        parseConditionExpression(exp)

        this
    }

    //解析无括号()的表达式
    private def parseConditionExpression(expression: String): Unit = {

        var left = ""
        var right = ""
        var clause = ""
        var exp = expression

        breakable {
            while (true) {
                $AND$.findFirstMatchIn(exp) match {
                    case Some(m) =>
                        clause = m.group(2)
                        left = m.group(3)
                        right = m.group(4)

                        breakable {
                            while (true) {
                                $OR.findFirstIn(clause) match {
                                    case Some(or) =>
                                        clause = clause.takeAfter(or)
                                        left = left.takeAfter(or)
                                    case None => break
                                }
                            }
                        }

                        if (!$CONDITION$N.test(left)) {
                            exp = exp.replace(left, stashCondition)
                            clause = clause.replace(left, stashCondition)
                            conditions += new Condition(left.trim)
                        }

                        if (!$CONDITION$N.test(right)) {
                            exp = exp.replace(right, stashCondition)
                            clause = clause.replace(right, stashCondition)
                            conditions += new Condition(right.trim)
                        }

                        exp = exp.replace(clause, stashCondition)
                        conditions += new Condition(clause.trim) // left AND right
                    case None => break
                }
            }
        }

        breakable {
            while (true) {
                $OR$.findFirstMatchIn(exp) match {
                    case Some(m) =>
                        clause = m.group(2)
                        left = m.group(3)
                        right = m.group(4)

                        if (!$CONDITION$N.test(left)) {
                            exp = exp.replace(left, stashCondition)
                            clause = clause.replace(left, stashCondition)
                            conditions += new Condition(left.trim)
                        }

                        if (!$CONDITION$N.test(right)) {
                            exp = exp.replace(right, stashCondition)
                            clause = clause.replace(right, stashCondition)
                            conditions += new Condition(right.trim)
                        }

                        exp = exp.replace(clause, stashCondition)
                        this.conditions += new Condition(clause.trim) // left OR right
                    case None => break
                }
            }
        }

        //SINGLE
        if (!$CONDITION$N.test(exp)) {
            conditions += new Condition(exp.trim)
        }
    }

    private def stashCondition: String = {
        "~condition[" + this.conditions.size + "]"
    }

    // 和 SELECT 一样，需要整合代码
    def where(row: DataRow): Boolean = {
        if (conditions.nonEmpty) {
            booleans.clear()

            //最终执行
            for (condition <- conditions) {

                val field = condition.field
                val value = condition.value

                booleans +=
                    Condition.eval(
                        if (field == null) {
                            DataCell.NULL
                        }
                        else if ($CONDITION$N.test(field)) {
                            booleans(field.$trim("~condition[", "]").toInt)
                        }
                        else if (row.contains(field)) {
                            row.getCell(field).value
                        }
                        else {
                            Script.evalJavascript(field)
                        },
                        condition.operator,
                        if ($CONDITION$N.test(value)) {
                            booleans(value.$trim("~condition[", "]").toInt)
                        }
                        else if (value.equalsIgnoreCase("NULL") || value == "()") {
                            null
                        }
                        else if (row.contains(value)) {
                            row.getCell(value).value
                        }
                        else {
                            Script.evalJavascript(value)
                        })
            }

            booleans.last
        }
        else {
            true
        }
    }
}
