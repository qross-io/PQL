package cn.qross.pql

import cn.qross.exception.SQLParseException
import cn.qross.pql.Patterns.$TEMP
import cn.qross.pql.Solver._
import cn.qross.ext.TypeExt._

object TEMP {
    def parse(sentence: String, PQL: PQL): Unit = {
        $TEMP.findFirstMatchIn(sentence) match {
            case Some(m) =>
                val $temp = new Statement("TEMP", sentence.takeBefore("#"), new TEMP(m.group(1).trim, sentence.takeAfter("#").trim))
                PQL.PARSING.head.addStatement($temp)
            case None => throw new SQLParseException("Incorrect TEMP sentence: " + sentence)
        }
    }
}

class TEMP(val tableName: String, val selectSQL: String) {
    def execute(PQL: PQL): Unit = {
        //PQL.dh.get(this.selectSQL.$restore(PQL))
        PQL.dh.buffer(selectSQL.$compute(PQL).asTable)
            .temp(this.tableName.$eval(PQL).asText)
    }
}
