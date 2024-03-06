package cn.qross.pql

import cn.qross.exception.SQLParseException
import cn.qross.ext.TypeExt._
import cn.qross.pql.Patterns.$ELSE

object ELSE {
    def parse(sentence: String, PQL: PQL): Unit = {
        $ELSE.findFirstIn(sentence) match {
            case Some(m) =>
                val $else: Statement = new Statement("ELSE", "ELSE", new ELSE())
                if (PQL.PARSING.isEmpty || (PQL.PARSING.head.caption != "IF" && PQL.PARSING.head.caption != "ELSIF" && PQL.PARSING.head.caption != "WHEN")) {
                    throw new SQLParseException("Can't find previous IF or ELSE IF or WHEN clause: " + sentence)
                }
                //先出栈再进栈
                PQL.PARSING.pop()
                PQL.PARSING.head.addStatement($else)
                PQL.PARSING.push($else)
                //继续解析子语句
                val first = sentence.takeAfter(m).trim()
                if (first != "") {
                    PQL.parseStatement(first)
                }
            case None => throw new SQLParseException("Incorrect ELSE sentence: " + sentence)
        }
    }
}

class ELSE {

    def execute(PQL: PQL, statement: Statement): Unit = {
        //IF 匹配到为 ELSIF 或 IF
        //IF 匹配不到为 ROOT
        //CASE WHEN 匹配到为 WHEN
        //CASE WHEN 匹配不到为 CASE
        if (PQL.EXECUTING.head.caption == "CASE" || PQL.EXECUTING.head.caption == "WHEN") {
            if (!PQL.CASE$WHEN.head.matched) {
                PQL.CASE$WHEN.head.matched = true
                PQL.EXECUTING.push(statement)

                PQL.executeStatements(statement.statements)
            }
        }
        else {
            if (!PQL.IF$BRANCHES.head) {
                PQL.IF$BRANCHES.pop()
                PQL.IF$BRANCHES.push(true)
                PQL.EXECUTING.push(statement)

                PQL.executeStatements(statement.statements)
            }
        }
    }

}
