package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.pql.Patterns.{$BLANK, $USE}
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

object USE {

    def parse(sentence: String, PQL: PQL): Unit = {
        if ($USE.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("USE", sentence, new USE(sentence.takeAfter($BLANK).trim)))
        }
        else {
            throw new SQLParseException("Incorrect USE sentence: " + sentence)
        }
    }
}

class USE(val databaseName: String) {
    def execute(PQL: PQL): Unit = {
        PQL.dh.use(this.databaseName.$eval(PQL).asText)
    }
}
