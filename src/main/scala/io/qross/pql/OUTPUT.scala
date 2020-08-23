package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.pql.Patterns._
import io.qross.pql.Solver._

object OUTPUT {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("OUTPUT", sentence, new OUTPUT(sentence.takeAfterX($OUTPUT).trim())))
    }
}

class OUTPUT(val content: String) {
    def execute(PQL: PQL): Unit = {
        if (content != "") {
            PQL.RESULT += content.$compute(PQL).value
        }
    }
}