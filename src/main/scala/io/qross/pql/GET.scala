package io.qross.pql

import io.qross.pql.Patterns.$GET
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

object GET {

    def parse(sentence: String, PQL: PQL): Unit = {
        if ($GET.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("GET", sentence, new GET(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect GET sentence: " + sentence)
        }
    }
}

class GET(val selectSQL: String) {

    def execute(PQL: PQL): Unit = {
        //为了支持sharp表达式, 所以用buffer
        PQL.dh.buffer(new SELECT(this.selectSQL).query(PQL).asTable)
        //dh.get($get.selectSQL.$restore(this))
    }
}
