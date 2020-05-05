package io.qross.pql

import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.pql.Patterns._
import io.qross.pql.Solver._

object OUTPUT {
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($OUTPUT.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("OUTPUT", sentence, new OUTPUT(sentence.takeAfter($OUTPUT).trim)))
        }
        else {
            throw new SQLParseException("Incorrect OUTPUT sentence: " + sentence)
        }
    }
}

class OUTPUT(val content: String) {

    def execute(PQL: PQL): Unit = {
        PQL.RESULT += {
            if ($SELECT.test(content)) {
                new SELECT(content).select(PQL).value
            }
            else if ($PARSE.test(content)) {
                new PARSE(content).doParse(PQL).value
            }
            else if ($INSERT$INTO.test(content)) {
                new INSERT(content).insert(PQL)
            }
            else if ($DELETE.test(content)) {
                new DELETE(content).delete(PQL)
            }
            else if ($NON$QUERY.test(content)) {
                PQL.AFFECTED_ROWS_OF_LAST_NON_QUERY = PQL.dh.executeNonQuery(content.$restore(PQL))
                PQL.AFFECTED_ROWS_OF_LAST_NON_QUERY
            }
            else if (content.bracketsWith("[", "]") || content.bracketsWith("{", "}")) {
                //对象或数组类型不能eval
                Json.fromText(content.$restore(PQL, "\"")).findNode("/")
            }
            else if ("""^~json\[\d+\]$""".r.test(content)) {
                Json.fromText(content.restoreJsons(PQL).$restore(PQL, "\"")).findNode("/")
            }
            else {
                new Sharp(content.$clean(PQL)).execute(PQL).value
            }
        }
    }
}