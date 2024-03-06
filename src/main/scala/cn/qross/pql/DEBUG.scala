package cn.qross.pql

import cn.qross.exception.SQLParseException
import cn.qross.pql.Patterns.$DEBUG
import cn.qross.pql.Solver._
import cn.qross.ext.TypeExt._

object DEBUG {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("DEBUG", sentence, new DEBUG(sentence.takeAfterX($DEBUG).trim())))
    }
}

class DEBUG(val switch: String) {

    def execute(PQL: PQL): Unit = {
        PQL.dh.debug(this.switch.$eval(PQL).asBoolean(false))
    }
}
