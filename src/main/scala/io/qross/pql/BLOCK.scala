package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.pql.Patterns.$BLOCK
import io.qross.ext.TypeExt._
import io.qross.pql.Solver._

object BLOCK {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($BLOCK.matches(sentence)) {
            val $block = new Statement("BLOCK", sentence.takeBefore("#"), new BLOCK(sentence.takeBefore("#").trim(), $m.group(1).toUpperCase(), sentence.takeAfter("#").trim))
            PQL.PARSING.head.addStatement($block)
        }
        else {
            throw new SQLParseException("Incorrect BLOCK sentence: " + sentence)
        }
    }
}

class BLOCK(private val sentence: String, caption: String, SQL: String) {
    def execute(PQL: PQL): Unit = {
        val plan = Syntax("BLOCK").plan(sentence.$restore(PQL))
        val from = plan.get("FROM").getOrElse("0").toLong
        val to = plan.get("TO").getOrElse("0").toLong
        val block = plan.get("PER").getOrElse("10000").toInt

        if (caption == "SELECT") {
            PQL.dh.block(SQL.$restore(PQL), from, to, block)
        }
        else {
            PQL.dh.bulk(SQL.$restore(PQL), from, to, block)
        }
    }
}
