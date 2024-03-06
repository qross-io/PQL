package cn.qross.pql

import cn.qross.exception.SQLParseException
import cn.qross.ext.TypeExt._
import cn.qross.net.Json
import cn.qross.pql.Patterns._
import cn.qross.pql.Solver._

object OUTPUT {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("OUTPUT", sentence, new OUTPUT(sentence.takeAfterX($OUTPUT).trim())))
    }
}

class OUTPUT(val content: String) {
    def execute(PQL: PQL): Unit = {
        if (content != "") {
            PQL.RESULT += content.$compute(PQL).value
        }
    }
}