package io.qross.pql

import io.qross.exception.{SQLExecuteException, SQLParseException}
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns.$GET
import io.qross.pql.Solver._

object GET {

    def parse(sentence: String, PQL: PQL): Unit = {
        $GET.findFirstIn(sentence) match {
            case Some(get) => PQL.PARSING.head.addStatement(new Statement("GET", sentence, new GET(sentence.takeAfter(get).trim())))
            case None => throw new SQLParseException("Incorrect GET sentence: " + sentence)
        }
    }
}

class GET(val sentence: String) {

    def execute(PQL: PQL): Unit = {
        //为了支持sharp表达式, 所以用buffer
        PQL.dh.buffer(sentence.$compute(PQL).asTable)
    }
}
