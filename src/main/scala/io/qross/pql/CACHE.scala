package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.pql.Patterns.$CACHE
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

object CACHE {
    def parse(sentence: String, PQL: PQL): Unit = {
        $CACHE.findFirstMatchIn(sentence) match {
            case Some(m) =>
                val $cache = new Statement("CACHE", sentence.takeBefore("#"), new CACHE(m.group(1).trim, sentence.takeAfter("#").trim))
                PQL.PARSING.head.addStatement($cache)
            case None => throw new SQLParseException("Incorrect CACHE sentence: " + sentence)
        }
    }
}

class CACHE(val tableName: String, val selectSQL: String) {
    def execute(PQL: PQL): Unit = {
        PQL.dh.get(this.selectSQL.$restore(PQL))
                .cache(this.tableName.$eval(PQL).asText)
    }
}
