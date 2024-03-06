package cn.qross.pql

import cn.qross.exception.SQLParseException
import cn.qross.pql.Patterns.$BATCH
import cn.qross.ext.TypeExt._
import cn.qross.pql.Solver._

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
