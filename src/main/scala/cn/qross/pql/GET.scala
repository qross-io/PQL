package cn.qross.pql

import cn.qross.exception.SQLParseException
import cn.qross.ext.TypeExt._
import cn.qross.pql.Patterns.$GET
import cn.qross.pql.Solver._

object GET {

    def parse(sentence: String, PQL: PQL): Unit = {
        $GET.findFirstIn(sentence) match {
            case Some(get) => PQL.PARSING.head.addStatement(new Statement("GET", sentence, new GET(sentence.takeAfter(get).trim())))
            case None => throw new SQLParseException("Incorrect GET sentence: " + sentence)
        }
    }
}

class GET(val sentence: String) {

    def execute(PQL: PQL): Unit = {
        //为了支持sharp表达式, 所以用buffer
        PQL.dh.buffer(sentence.$compute(PQL).asTable)
    }
}
