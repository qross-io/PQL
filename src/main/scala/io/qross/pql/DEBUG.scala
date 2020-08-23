package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.pql.Patterns.$DEBUG
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

object DEBUG {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("DEBUG", sentence, new DEBUG(sentence.takeAfterX($DEBUG).trim())))
    }
}

class DEBUG(val switch: String) {

    def execute(PQL: PQL): Unit = {
        PQL.dh.debug(this.switch.$eval(PQL).asBoolean(false))
    }
}
