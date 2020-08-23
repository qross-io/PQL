package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.pql.Patterns.{$BLANK, $EXEC}
import io.qross.ext.TypeExt._
import io.qross.pql.Solver._

//only support one sentence/statement 仅支持一条语句, 不支持语句块

object EXEC {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("EXEC", sentence, new EXEC(sentence.takeAfterX($EXEC).trim())))
    }
}

class EXEC(sentence: String) {
    def execute(PQL: PQL): Unit = {
        if (sentence.nonEmpty) {
            PQL.PARSING.push(new Statement("EXEC"))
            PQL.parseStatement(sentence.$restore(PQL, "").removeQuotes().trim())
            PQL.executeStatements(PQL.PARSING.head.statements)
            PQL.PARSING.pop()
        }
    }
}
