package io.qross.pql

import io.qross.pql.Patterns.$CACHE
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

object CACHE {

    def parse(sentence: String, PQL: PQL): Unit = {
        val m = $CACHE.matcher(sentence)
        if (m.find) {
            val $cache = new Statement("CACHE", sentence.takeBefore("#"), new CACHE(m.group(1).trim, sentence.takeAfter("#").trim))
            PQL.PARSING.head.addStatement($cache)
        }
        else {
            throw new SQLParseException("Incorrect CACHE sentence: " + sentence)
        }
    }
}

class CACHE(val tableName: String, val selectSQL: String) {
    def execute(PQL: PQL): Unit = {
        PQL.dh.get(this.selectSQL.$restore(PQL))
                .cache(this.tableName.$eval(PQL).asText)
    }
}
