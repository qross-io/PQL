package cn.qross.pql

import cn.qross.exception.SQLParseException
import cn.qross.pql.Patterns.$CACHE
import cn.qross.pql.Solver._
import cn.qross.ext.TypeExt._

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
        PQL.dh.buffer(selectSQL.$compute(PQL).asTable) //这种方式支持更多特性
        //PQL.dh.get(selectSQL.$restore(PQL)) //这种方式只支持 SELECT SQL
                .cache(this.tableName.$eval(PQL).asText)
    }
}
