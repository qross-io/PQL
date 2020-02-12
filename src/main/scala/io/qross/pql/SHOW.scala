package io.qross.pql

import java.util.regex.Matcher

import io.qross.pql.Patterns.$SHOW
import io.qross.pql.Solver._

object SHOW {
    def parse(sentence: String, PQL: PQL): Unit = {
        $SHOW.findFirstMatchIn(sentence) match {
            case Some(m) => PQL.PARSING.head.addStatement(new Statement("SHOW", sentence, new SHOW(m.group(1))))
            case None => throw new SQLParseException("Incorrect SHOW sentence: " + sentence)
        }
    }
}

class SHOW(val rows: String) {
    def execute(PQL: PQL): Unit = {
        PQL.dh.show(this.rows.$eval(PQL).asInteger(20).toInt)
    }
}
