package cn.qross.pql

import cn.qross.exception.{SQLExecuteException, SQLParseException}
import cn.qross.pql.Patterns.$LET
import cn.qross.ext.TypeExt._
import cn.qross.pql.Solver._

object LET {
    def parse(sentence: String, PQL: PQL): Unit = {
        $LET.findFirstMatchIn(sentence) match {
            case Some(m) =>
                val exp = sentence.takeAfter(m.group(0)).trim()
                if (exp.startsWith("$")) {
                    PQL.PARSING.head.addStatement(
                        new Statement("LET", sentence, new LET(exp.takeBeforeX("\\s".r), exp.takeAfterX("\\s".r)))
                    )
                }
                else {
                    throw new SQLParseException("Incorrect LET sentence, only variable can be updated. " + sentence)
                }
            case None => throw new SQLParseException("Incorrect LET sentence: " + sentence)
        }
    }
}

class LET(variable:String, expression: String) {
    def execute(PQL: PQL): Unit = {
        if (variable.startsWith("$")) {
            val data = PQL.findVariable(variable)
            if (data.defined) {
                new Sharp(expression.$clean(PQL), data).execute(PQL)
            }
        }
        else {
            throw new SQLExecuteException("Only user variable can be updated. " + variable)
        }
    }
}
