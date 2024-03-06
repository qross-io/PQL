package cn.qross.pql

import java.util.regex.Matcher

import cn.qross.core.{DataCell, DataType}
import cn.qross.ext.Output
import cn.qross.ext.TypeExt._
import cn.qross.pql.Patterns._
import cn.qross.pql.Solver._

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

class ConditionGroup(expression: String) {

    private[pql] val conditions = new ArrayBuffer[Condition]()

    //replaced 是否已经提取 inner sentence
    def evalAll(PQL: PQL, replaced: Boolean = false): Boolean = {

        //解析表达式
        var exp = {
            if (!replaced) {
                expression.trim().replaceInnerSentence(PQL) //提取 inner sentence
            }
            else {
                expression.trim()
            }
        }.$clean(PQL).replaceInnerSentences(PQL)  //replace ~inner[n] to ~value[n]

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
        val result = conditions.last.eval(PQL, this)

        if (PQL.dh.debugging) {
            Output.writeLine(s"Condition expression { ${expression.$restore(PQL)} } is $result")
        }

        conditions.clear()

        result
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
                exp = exp.replaceFirstOne(left, stash)
                clause = clause.replaceFirstOne(left, stash)
                conditions += new Condition(left.trim)
            }

            if (!$CONDITION$N.test(right)) {
                exp = exp.replaceFirstOne(right, stash)
                clause = clause.replaceFirstOne(right, stash)
                conditions += new Condition(right.trim)
            }

            exp = exp.replaceFirstOne(clause, stash)
            conditions += new Condition(clause.trim) // left AND right

        }
        //OR
        while ({m = $OR$.matcher(exp); m}.find) {
            clause = m.group(2)
            left = m.group(3)
            right = m.group(4)

            if (!$CONDITION$N.test(left)) {
                exp = exp.replaceFirstOne(left, stash)
                clause = clause.replaceFirstOne(left, stash)
                conditions += new Condition(left.trim)
            }

            if (!$CONDITION$N.test(right)) {
                exp = exp.replaceFirstOne(right, stash)
                clause = clause.replaceFirstOne(right, stash)
                conditions += new Condition(right.trim)
            }

            exp = exp.replaceFirstOne(clause, stash)
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
