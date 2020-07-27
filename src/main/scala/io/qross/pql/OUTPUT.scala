package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.pql.Patterns._
import io.qross.pql.Solver._

object OUTPUT {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($OUTPUT.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("OUTPUT", sentence, new OUTPUT(sentence.takeAfter($OUTPUT).trim)))
        }
        else {
            throw new SQLParseException("Incorrect OUTPUT sentence: " + sentence)
        }
    }
}

class OUTPUT(val content: String) {

    def execute(PQL: PQL): Unit = {
        PQL.RESULT += content.$compute(PQL)
    }
}