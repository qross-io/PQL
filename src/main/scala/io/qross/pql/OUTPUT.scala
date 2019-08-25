package io.qross.pql

import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.pql.Patterns._
import io.qross.pql.Solver._


class OUTPUT(val content: String) {

    def execute(PQL: PQL): Any = {

        if ($SELECT.test(content)) {
            new SELECT(content.$restore(PQL)).execute(PQL)
        }
        else if ($PARSE.test(content)) {
            new PARSE(content.takeAfter($PARSE).trim.$eval(PQL).asText).execute(PQL)
        }
        else if ($NON_QUERY.test(content)) {
            PQL.AFFECTED = PQL.dh.executeNonQuery(content.$restore(PQL))
            PQL.AFFECTED
        }
        else if (content.bracketsWith("[", "]") || content.bracketsWith("{", "}")) {
            //对象或数组类型不能eval
            Json.fromText(content.$restore(PQL, "\"")).findNode("/")
        }
        else {
            new SHARP(content.$clean(PQL)).execute(PQL).value
        }
    }
}