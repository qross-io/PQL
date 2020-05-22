package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.pql.Patterns.$PREP
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

object PREP {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($PREP.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("PREP", sentence, new PREP(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect PREP sentence: " + sentence)
        }
    }
}

class PREP(val nonQuerySQL: String) {
    def execute(PQL: PQL): Unit = {
        PQL.dh.prep(this.nonQuerySQL.$restore(PQL))
    }
}
