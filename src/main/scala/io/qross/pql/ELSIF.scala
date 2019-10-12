package io.qross.pql

import io.qross.pql.Patterns.$ELSIF

object ELSIF {
    def parse(sentence: String, PQL: PQL): Unit = {
        val m = $ELSIF.matcher(sentence)
        if (m.find) {
            val $elsif: Statement = new Statement("ELSIF", sentence, new ELSIF(m.group(1)))
            if (PQL.PARSING.isEmpty || (!(PQL.PARSING.head.caption == "IF") && !(PQL.PARSING.head.caption == "ELSIF"))) {
                throw new SQLParseException("Can't find previous IF or ELSE IF clause: " + m.group(0))
            }
            //先出栈再进栈
            PQL.PARSING.pop()
            PQL.PARSING.head.addStatement($elsif)
            PQL.PARSING.push($elsif)
            //继续解析子语句
            PQL.parseStatement(sentence.substring(m.group(0).length).trim)
        }
    }
}

class ELSIF(conditions: String) {

    def execute(PQL: PQL, statement: Statement): Unit = {
        if (!PQL.IF$BRANCHES.last) {
            if (new ConditionGroup(conditions).evalAll(PQL)) {
                PQL.IF$BRANCHES.pop()
                PQL.IF$BRANCHES.push(true)
                PQL.EXECUTING.push(statement)

                PQL.executeStatements(statement.statements)
            }
        }
    }
}
