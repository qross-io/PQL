package io.qross.pql

import io.qross.pql.Patterns.{$EXIT, $EXIT$CODE}
import io.qross.ext.TypeExt._
import scala.util.control.Breaks.{break, breakable}

object EXIT {

    def parse(sentence: String, PQL: PQL): Unit = {
        if ($EXIT$CODE.test(sentence)) {
            val $exit$code = new Statement("EXIT$CODE", sentence, new EXIT$CODE(sentence.takeAfter($EXIT$CODE).trim()))
            PQL.PARSING.head.addStatement($exit$code)
        }
        else {
            var contained = false
            breakable {
                for (group <- PQL.PARSING) {
                    if (group.caption == "FOR" || group.caption == "WHILE") {
                        contained = true
                        break
                    }
                }
            }

            $EXIT.findFirstMatchIn(sentence) match {
                case Some(m) =>
                    if (contained) {
                        val $exit: Statement = new Statement("EXIT", sentence, if (m.group(1) != null) new ConditionGroup(m.group(2).trim()) else null)
                        PQL.PARSING.head.addStatement($exit)
                    }
                    else {
                        throw new SQLParseException("EXIT must be contained in FOR or WHILE statement: " + sentence)
                    }
                case None => throw new SQLParseException("Incorrect EXIT sentence: " + sentence)
            }
        }
    }

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
            PQL.breakCurrentLoop = CONTINUE.execute(PQL, statement)
            PQL.breakCurrentLoop
        }
        else {
            throw new SQLExecuteException("EXIT must be contained in FOR or WHILE statement: " + statement.sentence)
        }
    }
}
