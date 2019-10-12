package io.qross.pql

import io.qross.pql.Patterns.$TEMP
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

object TEMP {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($TEMP.matches(sentence)) {
            val $temp = new Statement("TEMP", sentence.takeBefore("#"), new TEMP($m.group(1).trim, sentence.takeAfter("#").trim))
            PQL.PARSING.head.addStatement($temp)
        }
        else {
            throw new SQLParseException("Incorrect TEMP sentence: " + sentence)
        }
    }
}

class TEMP(val tableName: String, val selectSQL: String) {
    def execute(PQL: PQL): Unit = {
        PQL.dh.get(this.selectSQL.$restore(PQL)).temp(this.tableName.$eval(PQL).asText)
    }
}
