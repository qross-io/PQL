package io.qross.pql

import io.qross.exception.{SQLExecuteException, SQLParseException}
import io.qross.pql.Patterns.$BLOCK
import io.qross.ext.TypeExt._
import io.qross.pql.Solver._

object BLOCK {
    def parse(sentence: String, PQL: PQL): Unit = {
        $BLOCK.findFirstMatchIn(sentence) match {
            case Some(m) =>
                val $block = new Statement("BLOCK", sentence.takeBefore("#"), new BLOCK(sentence.takeBefore("#").trim(), sentence.takeAfter("#").trim))
                PQL.PARSING.head.addStatement($block)
            case None => throw new SQLParseException("Incorrect BLOCK sentence: " + sentence)
        }
    }
}

class BLOCK(private val sentence: String, SQL: String) {
    def execute(PQL: PQL): Unit = {
        val plan = Syntax("BLOCK").plan(sentence.$restore(PQL))
        val from = plan.get("FROM").getOrElse("0").toLong
        val to = plan.get("TO").getOrElse("0").toLong
        val block = plan.get("PER").getOrElse("10000").toInt
        val caption = """^[a-zA-Z]+\b""".r.findFirstIn(SQL).getOrElse("").toUpperCase()
        if (caption == "SELECT") {
            PQL.dh.block(SQL.$restore(PQL), from, to, block)
        }
        else if (caption == "UPDATE" || caption == "DELETE") {
            PQL.dh.bulk(SQL.$restore(PQL), from, to, block)
        }
        else {
            throw new SQLExecuteException("Only supports SELECT/UPDATE/DELETE sentence in BLOCK: " + sentence)
        }
    }
}
