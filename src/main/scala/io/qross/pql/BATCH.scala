package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.pql.Patterns.$BATCH
import io.qross.ext.TypeExt._
import io.qross.pql.Solver._

object BATCH {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($BATCH.test(sentence)) {
            val $batch = new Statement("BATCH", sentence.takeBefore("#"), new BATCH(sentence.takeAfter("#").trim))
            PQL.PARSING.head.addStatement($batch)
        }
        else {
            throw new SQLParseException("Incorrect BATCH sentence: " + sentence)
        }
    }
}

class BATCH(nonQuerySQL: String) {
    def execute(PQL: PQL): Unit = {
        PQL.dh.batch(this.nonQuerySQL.$restore(PQL))
    }
}
