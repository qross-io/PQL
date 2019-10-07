package io.qross.pql

import io.qross.pql.Patterns.$WHILE

import scala.util.control.Breaks.{break, breakable}

object WHILE {
    def parse(sentence: String, PQL: PQL): Unit = {
        val m = $WHILE.matcher(sentence)
        if (m.find) {
            val $while: Statement = new Statement("WHILE", sentence, new WHILE(m.group(1).trim))
            PQL.PARSING.head.addStatement($while)
            //只进栈
            PQL.PARSING.push($while)
            //待关闭的控制语句
            PQL.TO_BE_CLOSE.push($while)
            //继续解析子语句
            PQL.parseStatement(sentence.substring(m.group(0).length).trim)
        }
        else {
            throw new SQLParseException("Incorrect WHILE sentence: " + sentence)
        }
    }
}

class WHILE(conditions: String) {

    def execute(PQL: PQL, statement: Statement): Unit = {
        val whileCondition: ConditionGroup = new ConditionGroup(conditions)
        PQL.EXECUTING.push(statement)
        breakable {
            while (whileCondition.evalAll(PQL)) {
                if (!PQL.breakCurrentLoop) {
                    PQL.executeStatements(statement.statements)
                }
                else {
                    break
                }
            }
        }
    }

}
