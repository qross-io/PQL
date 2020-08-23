package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.pql.Patterns.$PROCESS
import io.qross.ext.TypeExt._
import io.qross.pql.Solver._

object PROCESS {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($PROCESS.test(sentence)) {
            val $process = new Statement("PROCESS", sentence.takeBefore("#"), new PROCESS(sentence.takeAfter("#").trim))
            PQL.PARSING.head.addStatement($process)
        }
        else {
            throw new SQLParseException("Incorrect PROCESS sentence: " + sentence)
        }
    }
}

class PROCESS(selectSQL: String) {
    def execute(PQL: PQL): Unit = {
        PQL.dh.process(this.selectSQL.$restore(PQL))
    }
}