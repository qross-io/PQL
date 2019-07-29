package io.qross.sql

import java.util.regex.Matcher

import io.qross.core.{DataCell, DataHub, DataType}
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.sql.Solver._
import io.qross.sql.Patterns._

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class ConditionGroup(expression: String) {

    private val conditions = new ArrayBuffer[Condition]()
    private val ins = new ArrayBuffer[String]()
    private val exists = new ArrayBuffer[String]()
    private val selects = new ArrayBuffer[String]()

    def evalAll(PSQL: PSQL): Boolean = {

        //解析表达式
        var exp = expression.$clean(PSQL)

         //replace SELECT to ~select[n]
        var m: Matcher = null
        while ({m = $SELECT$.matcher(exp); m}.find) {
            val select = findOutSelect(exp, m.group)
            exp = exp.replace(select, "~select[" + selects.size + "]")
            selects += select
        }

        //replace EXISTS () to ~exists[n]
        m = EXISTS$$.matcher(exp)
        while (m.find) {
            exp = exp.replace(m.group(1), "~exists[" + exists.size + "]")
            exists += m.group(1)
        }

        //replace IN () to #[in:n]
        m = IN$$.matcher(exp)
        while(m.find) {
            exp = exp.replace(m.group(1), "~in[" + ins.size + "]")
            ins += m.group(1)
        }

        //以上3步目的是去掉括号

        // 解析括号中的逻辑 () 并将表达式分步
        while ({m = $BRACKET.matcher(exp); m}.find) {
            parseBasicExpression(m.group(1).trim)
            exp = exp.replace(m.group(0), CONDITION + (this.conditions.size - 1) + N)
        }
        //finally
        parseBasicExpression(exp)

        exists.clear()
        ins.clear()

        //最终执行

        //IN (SELECT ...)
        val selectResult = new ArrayBuffer[String]
        for (select <- selects) {
            selectResult += PSQL.dh.executeSingleList(select.popStash(PSQL)).asScala.mkString(",")
        }

        //最终执行
        for (condition <- this.conditions) {
            val field = condition.field
            var value = condition.value

            m = SELECT$N.matcher(value)
            while (m.find) {
                value = value.replace(m.group(0), selectResult(m.group(1).toInt))
            }

            condition.eval( if ({m = CONDITION$.matcher(field); m}.find) {
                                DataCell(conditions(m.group(1).toInt).result, DataType.BOOLEAN)
                            }
                            else if (field.nonEmpty) {
                                field.$sharp(PSQL)
                            }
                            else {
                                DataCell.NULL
                            },
                            if ({m = CONDITION$.matcher(value); m}.find) {
                                DataCell(conditions(m.group(1).toInt).result, DataType.BOOLEAN)
                            }
                            else if (condition.operator == "IN" || condition.operator == "NOT$IN") {
                                DataCell(value.$trim("(", ")").split(",").map(m => m.$sharp(PSQL).value).toList.asJava, DataType.ARRAY)
                            }
                            else if (!value.equalsIgnoreCase("EMPTY") && !value.equalsIgnoreCase("NULL") && value != "()") {
                                value.$sharp(PSQL)
                            }
                            else if (value.equalsIgnoreCase("EMPTY")) {
                                DataCell.EMPTY
                            }
                            else {
                                DataCell.NULL
                            })

            Output.writeDotLine(" ", condition.field, condition.operator, condition.value, " => ", condition.result)
        }

        selects.clear()

        val result = conditions(conditions.size - 1).result

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
            expression.substring(begin, end)
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

        //restore EXISTS
        m = EXISTS$N.matcher(exp)
        while (m.find) {
            exp = exp.replace(m.group(0), exists(m.group(1).toInt))
        }

        //restore IN
        m = IN$N.matcher(exp)
        while (m.find) {
            exp = exp.replace(m.group(0), ins(m.group(1).toInt))
        }

        //AND
        while ({m = $AND.matcher(exp); m}.find) {
            clause = m.group(2)
            left = m.group(3)
            right = m.group(4)

            while ({n = $_OR.matcher(clause); n}.find) {
                clause = clause.substring(clause.indexOf(n.group) + n.group.length)
                left = left.substring(left.indexOf(n.group) + n.group.length)
            }

            if (!left.startsWith(CONDITION)) {
                exp = exp.replace(left, CONDITION + this.conditions.size + N)
                clause = clause.replace(left, CONDITION + this.conditions.size + N)
                conditions += new Condition(left.trim)
            }

            if (!right.startsWith(CONDITION)) {
                exp = exp.replace(right, CONDITION + this.conditions.size + N)
                clause = clause.replace(right, CONDITION + this.conditions.size + N)
                conditions += new Condition(right.trim)
            }

            exp = exp.replace(clause, CONDITION + this.conditions.size + N)
            conditions += new Condition(clause.trim) // left AND right

        }
        //OR
        while ({m = $OR.matcher(expression); m}.find) {
            clause = m.group(2)
            left = m.group(3)
            right = m.group(4)

            if (!left.startsWith(CONDITION)) {
                exp = exp.replace(left, CONDITION + this.conditions.size + N)
                clause = clause.replace(left, CONDITION + this.conditions.size + N)
                conditions += new Condition(left.trim)
            }

            if (!right.startsWith(CONDITION)) {
                exp = exp.replace(right, CONDITION + this.conditions.size + N)
                clause = clause.replace(right, CONDITION + this.conditions.size + N)
                conditions += new Condition(right.trim)
            }

            exp = exp.replace(clause, CONDITION + this.conditions.size + N)
            this.conditions += new Condition(clause.trim) // left OR right

        }

        //SINGLE
        if (!expression.startsWith(CONDITION)) {
            conditions += new Condition(expression.trim)
        }
    }
}
