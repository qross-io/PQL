package cn.qross.pql

import cn.qross.exception.SQLParseException
import cn.qross.pql.Patterns.$ELSIF

object ELSIF {
    def parse(sentence: String, PQL: PQL): Unit = {
        $ELSIF.findFirstMatchIn(sentence) match {
            case Some(m) =>
                val $elsif: Statement = new Statement("ELSIF", sentence, new ELSIF(m.group(1)))
                if (PQL.PARSING.isEmpty || (PQL.PARSING.head.caption != "IF" && PQL.PARSING.head.caption != "ELSIF")) {
                    throw new SQLParseException("Can't find previous IF or ELSIF clause: " + m.group(0))
                }
                //先出栈再进栈
                PQL.PARSING.pop()
                PQL.PARSING.head.addStatement($elsif)
                PQL.PARSING.push($elsif)
                //继续解析子语句
                val first = sentence.substring(m.group(0).length).trim()
                if (first != "") {
                    PQL.parseStatement(first)
                }
            case None => throw new SQLParseException("Incorrect ELSIF sentence: " + sentence)
        }
    }
}

class ELSIF(conditions: String) {

    def execute(PQL: PQL, statement: Statement): Unit = {
        if (!PQL.IF$BRANCHES.head) {
            if (new ConditionGroup(conditions).evalAll(PQL)) {
                PQL.IF$BRANCHES.pop()
                PQL.IF$BRANCHES.push(true)
                PQL.EXECUTING.push(statement)

                PQL.executeStatements(statement.statements)
            }
        }
    }
}
