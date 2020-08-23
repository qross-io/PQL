package io.qross.pql

import io.qross.exception.{SQLExecuteException, SQLParseException}
import io.qross.pql.Patterns.$PUT
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._
import io.qross.net.Redis._

object PUT {
    def parse(sentence: String, PQL: PQL): Unit = {
        $PUT.findFirstIn(sentence) match {
            case Some(put) => PQL.PARSING.head.addStatement(new Statement("PUT", sentence, new PUT(sentence.takeAfter(put).trim())))
            case None => throw new SQLParseException("Incorrect PUT sentence: " + sentence)
        }
    }
}

class PUT(val nonQuerySQL: String) {
    def execute(PQL: PQL): Unit = {
        val sentence = this.nonQuerySQL.$restore(PQL)
        sentence.takeBeforeX(Patterns.$BLANK).toUpperCase() match {
            case "" => throw new SQLExecuteException("Incomplete or empty PUT sentence: " + sentence)
            case "REDIS" => PQL.dh.pipelined(sentence)
            case _ => PQL.dh.put(sentence)
        }
    }
}
