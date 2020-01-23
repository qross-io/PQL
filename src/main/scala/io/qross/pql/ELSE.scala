package io.qross.pql

import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.{$ELSE, $IF$}

object ELSE {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($ELSE.test(sentence)) {
            val $else: Statement = new Statement("ELSE", "ELSE", new ELSE())
            if (PQL.PARSING.isEmpty || (!(PQL.PARSING.head.caption == "IF") && !(PQL.PARSING.head.caption == "ELSIF"))) {
                throw new SQLParseException("Can't find previous IF or ELSE IF clause: " + sentence)
            }
            //先出栈再进栈
            PQL.PARSING.pop()
            PQL.PARSING.head.addStatement($else)
            PQL.PARSING.push($else)
            //继续解析子语句
            val first = sentence.takeAfter($ELSE).trim()
            if (first != "") {
                PQL.parseStatement(first)
            }
        }
        else {
            throw new SQLParseException("Incorrect ELSE or ELSE IF sentence: " + sentence)
        }
    }
}

class ELSE {

    def execute(PQL: PQL, statement: Statement): Unit = {
        if (!PQL.IF$BRANCHES.head) {
            PQL.IF$BRANCHES.pop()
            PQL.IF$BRANCHES.push(true)
            PQL.EXECUTING.push(statement)

            PQL.executeStatements(statement.statements)
        }
    }

}
