package io.qross.pql

import io.qross.pql.Patterns.{$BLANK, $ECHO}
import io.qross.ext.TypeExt._

object ECHO {
    def parse(sentence: String, PQL: PQL): Unit = {
        if($ECHO.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("ECHO", sentence, new ECHO(if ($BLANK.test(sentence)) sentence.takeAfter($BLANK) else "")))
        }
        else {
            throw new SQLParseException("Incorrect ECHO sentence: " + sentence)
        }
    }
}

class ECHO(val content: String) {

    def execute(PQL: PQL): Unit = {
        if (content.nonEmpty) {
            PQL.RESULT += content
        }
    }
}
