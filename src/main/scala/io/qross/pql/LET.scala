package io.qross.pql

import io.qross.pql.Patterns.$LET

object LET {
    def parse(sentence: String, PQL: PQL): Unit = {
        $LET.findFirstMatchIn(sentence) match {
            case Some(m) =>
                PQL.PARSING.head.addStatement(
                    new Statement("LET", sentence, new LET(m.group(1).trim(), m.group(2).trim(), m.group(3).trim()))
                )
            case None => throw new SQLParseException("Incorrect LET sentence: " + sentence)
        }
    }
}

class LET {

}
