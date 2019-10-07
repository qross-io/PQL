package io.qross.pql

import io.qross.pql.Patterns.$PUT
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

object PUT {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($PUT.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("PUT", sentence, new PUT(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect PUT sentence: " + sentence)
        }
    }
}

class PUT(val nonQuerySQL: String) {
    def execute(PQL: PQL): Unit = {
        PQL.dh.put(this.nonQuerySQL.$restore(PQL))
    }
}
