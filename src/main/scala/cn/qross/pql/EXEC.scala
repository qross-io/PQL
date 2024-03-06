package cn.qross.pql

import cn.qross.core.DataCell
import cn.qross.ext.TypeExt._
import cn.qross.pql.Patterns.{$BLANK, $EXEC}
import cn.qross.pql.Solver._

//only support one sentence/statement 仅支持一条语句, 不支持语句块

object EXEC {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("EXEC", sentence, new EXEC(sentence)))
    }
}

class EXEC(sentence: String) {

    def evaluate(PQL: PQL, express: Int = Solver.FULL): DataCell = {
        sentence.$process(PQL, express, body => {
            //再执行一次
            body.takeAfterX($EXEC).trim().removeQuotes().$compute(PQL, Solver.FULL)
        })
    }

    def execute(PQL: PQL, statement: Statement): Unit = {
        val todo = sentence.takeAfterX($EXEC).trim()
        if (todo.nonEmpty) {
            PQL.PARSING.push(statement)
            PQL.parseStatement(todo.$restore(PQL, "").removeQuotes())
            PQL.executeStatements(PQL.PARSING.head.statements)
            PQL.PARSING.pop()
        }
    }
}
