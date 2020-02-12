package io.qross.pql

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

class GET(val selectSQL: String) {

    def execute(PQL: PQL): Unit = {
        //为了支持sharp表达式, 所以用buffer

        //dh.get($get.selectSQL.$restore(this))
        (Patterns.$BLANK.findFirstIn(selectSQL) match {
            case Some(blank) => selectSQL.takeBefore(blank)
            case None => ""
        }).toUpperCase() match {
            case "SELECT" => PQL.dh.buffer(new SELECT(this.selectSQL).select(PQL).asTable)
            case "PARSE" => PQL.dh.buffer(new PARSE(this.selectSQL).parse(PQL).asTable)
            case "" => throw new SQLExecuteException("Incomplete or empty GET sentence: " + selectSQL)
            case _ => throw new SQLExecuteException("Unsupported GET sentence: " + selectSQL)
        }
    }
}
