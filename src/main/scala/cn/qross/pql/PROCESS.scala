package cn.qross.pql

import cn.qross.exception.SQLParseException
import cn.qross.pql.Patterns.$PROCESS
import cn.qross.ext.TypeExt._
import cn.qross.pql.Solver._

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