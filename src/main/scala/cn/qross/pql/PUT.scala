package cn.qross.pql

import cn.qross.exception.{SQLExecuteException, SQLParseException}
import cn.qross.pql.Patterns.$PUT
import cn.qross.pql.Solver._
import cn.qross.ext.TypeExt._
import cn.qross.net.Redis._

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
