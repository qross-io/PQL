package io.qross.pql

import io.qross.pql.Patterns.$IF
import io.qross.ext.TypeExt._

object IF {

    def parse(sentence: String, PQL: PQL): Unit = {
        val m = $IF.matcher(sentence)
        if (m.find) {
            val $if: Statement = new Statement("IF", sentence, new IF(m.group(1)))
            PQL.PARSING.head.addStatement($if)
            //只进栈
            PQL.PARSING.push($if)
            //待关闭的控制语句
            PQL.TO_BE_CLOSE.push($if)
            //继续解析第一条子语句
            PQL.parseStatement(sentence.takeAfter(m.group(0)).trim())
        }
        else {
            throw new SQLParseException("Incorrect IF sentence: " + sentence)
        }
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
