package io.qross.pql

import io.qross.pql.Patterns.$PAGE
import io.qross.ext.TypeExt._
import io.qross.pql.Solver._

object PAGE {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($PAGE.matches(sentence)) {
            val $page = new Statement("PAGE", sentence.takeBefore("#"), new PAGE(sentence.takeAfter("#").trim))
            PQL.PARSING.head.addStatement($page)
        }
        else {
            throw new SQLParseException("Incorrect PAGE sentence: " + sentence)
        }
    }
}

class PAGE(val selectSQL: String) {
    def execute(PQL: PQL): Unit = {
        PQL.dh.page(this.selectSQL.$restore(PQL))
    }
}
