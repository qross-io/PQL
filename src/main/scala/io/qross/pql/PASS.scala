package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.pql.Patterns.$PASS
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

object PASS {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($PASS.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("PASS", sentence, new PASS(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect PASS sentence: " + sentence)
        }
    }
}

class PASS(val selectSQL: String) {
    def execute(PQL: PQL): Unit = {
        PQL.dh.pass(this.selectSQL.$restore(PQL))
    }
}
