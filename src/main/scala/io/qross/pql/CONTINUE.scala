package io.qross.pql

import io.qross.exception.{SQLExecuteException, SQLParseException}
import io.qross.pql.Patterns.$CONTINUE

import scala.util.control.Breaks.{break, breakable}

object CONTINUE {

    def parse(sentence: String, PQL: PQL): Unit = {
        var contained = false
        breakable {
            for (group <- PQL.PARSING) {
                if (group.caption == "FOR" || group.caption == "WHILE") {
                    contained = true
                    break
                }
            }
        }

        $CONTINUE.findFirstMatchIn(sentence) match {
            case Some(m) =>
                if (contained) {
                    val $continue: Statement = new Statement("CONTINUE", m.group(0), if (m.group(1) != null) new ConditionGroup(m.group(2).trim()) else null)
                    PQL.PARSING.head.addStatement($continue)
                }
                else {
                    throw new SQLParseException("CONTINUE must be contained in FOR or WHILE statement: " + sentence)
                }
            case None => throw new SQLParseException("Incorrect CONTINUE sentence: " + sentence)
        }
    }

    //continue属于exit的子集
    def execute(PQL: PQL, statement: Statement): Boolean = {

        var contained = false
        breakable {
            for (group <- PQL.EXECUTING) {
                if (group.caption == "FOR" || group.caption == "WHILE") {
                    contained = true
                    break
                }
            }
        }

        if (contained) {
            if (statement.instance == null) {
                true
            }
            else {
                statement.instance.asInstanceOf[ConditionGroup].evalAll(PQL)
            }
        }
        else {
            throw new SQLExecuteException("CONTINUE must be contained in FOR or WHILE statement: " + statement.sentence)
        }
    }
}
