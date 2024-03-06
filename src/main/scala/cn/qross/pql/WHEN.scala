package cn.qross.pql

import cn.qross.exception.SQLParseException
import cn.qross.pql.Patterns._
import cn.qross.pql.Solver._

object WHEN {
    def parse(sentence: String, PQL: PQL): Unit = {
        $WHEN.findFirstMatchIn(sentence) match {
            case Some(m) =>
                if (PQL.PARSING.isEmpty || (PQL.PARSING.head.caption != "CASE" && PQL.PARSING.head.caption != "WHEN")) {
                    throw new SQLParseException("Can't find previous CASE or WHEN clause: " + m.group(0))
                }

                /*
                    END CASE，第一层，退出第二层，退出第一层
                 ELSE 第二层，替换WHEN
                 WHEN 第二层, 后面的WHEN替换前面的WHEN
                    CASE 第一层
                 */

                val $when: Statement = new Statement("WHEN", sentence, new WHEN(m.group(1)))
                if (PQL.PARSING.head.caption == "WHEN") {
                    PQL.PARSING.pop()
                }
                PQL.PARSING.head.addStatement($when)
                PQL.PARSING.push($when)
                //继续解析下一条语句
                val first = sentence.substring(m.group(0).length).trim()
                if (first != "") {
                    PQL.parseStatement(first)
                }
            case None => throw new SQLParseException("Incorrect WHEN sentence: " + sentence)
        }
    }
}

class WHEN(comparative: String) {
    def execute(PQL: PQL, statement: Statement): Unit = {
        if (!PQL.CASE$WHEN.head.matched) {
            if (
                if (PQL.CASE$WHEN.head.equivalent.isBoolean) {
                    new ConditionGroup(comparative).evalAll(PQL) == PQL.CASE$WHEN.head.equivalent.value
                }
                else {
                    comparative.$sharp(PQL).value == PQL.CASE$WHEN.head.equivalent.value
                }
            ) {
                PQL.CASE$WHEN.head.matched = true
                PQL.EXECUTING.push(statement)
                PQL.executeStatements(statement.statements)
            }
        }
    }
}
