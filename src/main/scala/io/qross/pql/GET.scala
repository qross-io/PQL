package io.qross.pql

import io.qross.exception.{SQLExecuteException, SQLParseException}
import io.qross.pql.Patterns.$GET
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

object GET {

    def parse(sentence: String, PQL: PQL): Unit = {
        if ($GET.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("GET", sentence, new GET(sentence.takeAfter("#").trim())))
        }
        else {
            throw new SQLParseException("Incorrect GET sentence: " + sentence)
        }
    }
}

class GET(val sentence: String) {

    def execute(PQL: PQL): Unit = {
        //为了支持sharp表达式, 所以用buffer
        //dh.get($get.selectSQL.$restore(this)) --旧代码

        (Patterns.$BLANK.findFirstIn(sentence) match {
            case Some(blank) => sentence.takeBefore(blank)
            case None => ""
        }).toUpperCase() match {
            case "SELECT" => PQL.dh.buffer(new SELECT(this.sentence).select(PQL).asTable)
            case "PARSE" => PQL.dh.buffer(new PARSE(this.sentence).doParse(PQL).asTable)
            case "REDIS" =>
            case "FILE" => PQL.dh.buffer(new FILE(sentence).evaluate(PQL).asTable)
            case "DIR" => PQL.dh.buffer(new DIR(sentence).evaluate(PQL).asTable)
            case "" => throw new SQLExecuteException("Incomplete or empty GET sentence: " + sentence)
            case _ => PQL.dh.buffer(new Sharp(sentence.$clean(PQL)).execute(PQL).asTable)
        }
    }
}
