package io.qross.pql

import io.qross.core.DataCell
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.net.Json._
import io.qross.pql.Patterns.{$PARSE, ARROW}
import io.qross.pql.Solver._

object PARSE {
    //用于PQL表达式解析
    def parse(sentence: String, PQL: PQL): Unit = {
        $PARSE.findFirstMatchIn(sentence) match {
            case Some(_) => PQL.PARSING.head.addStatement(new Statement("PARSE", sentence, new PARSE(sentence)))
            case None => throw new SQLParseException("Incorrect PARSE sentence: " + sentence)
        }
    }

}

class PARSE(var sentence: String) {

    //sentence包含PARSE关键词

    //express 是否支持嵌入式查询语句, 即 ${{ }}
    def parse(PQL: PQL, express: Boolean = false): DataCell = {

        var body = sentence.takeAfter(Patterns.$BLANK).trim()
        val links = {
            if (body.contains(ARROW)) {
                body = body.takeBefore(ARROW)
                sentence.takeAfter(ARROW)
            }
            else {
                ""
            }
        }

        if (express) {
            body = body.$express(PQL).popStash(PQL)
        }
        else {
            body = body.$restore(PQL)
        }

        val plan = Syntax("PARSE").plan(body)

        val path = plan.headArgs

        val data = {
            plan.head match {
                case "" =>
                    if (plan.size > 1) {
                        plan.last match {
                                case "AS TABLE" => PQL.dh.parseTable(path)
                                case "AS ROW" | "AS MAP" | "AS OBJECT" => PQL.dh.parseRow(path)
                                case "AS LIST" | "AS ARRAY" => PQL.dh.parseList(path)
                                case "AS VALUE" | "AS SINGLE VALUE" => PQL.dh.parseValue(path)
                                case _ => PQL.dh.parseTable(path)
                            }
                    }
                    else {
                        PQL.dh.parseTable(path)
                    }
                case _ => PQL.dh.parseTable(path)
            }
        }.toDataCell

        if (links != "") {
            new Sharp(links, data).execute(PQL)
        }
        else {
            data
        }
    }

    def execute(PQL: PQL): Unit = {
        val data = this.parse(PQL)
        PQL.RESULT += data
        PQL.COUNT_OF_LAST_SELECT = if (data.isTable) data.asTable.size else if (data.isJavaList) data.asJavaList.size() else 1

        if (PQL.dh.debugging) {
            if (data.isTable) {
                data.asTable.show()
            }
            else if (data.isRow) {
                println(data.asRow.toString)
            }
            else if(data.isJavaList) {
                Json.serialize(data.asJavaList).print
            }
            else {
                println(data.asText)
            }
        }
    }
}