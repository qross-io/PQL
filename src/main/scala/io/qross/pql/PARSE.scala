package io.qross.pql

import io.qross.core.{DataCell, DataType}
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.net.Json._
import io.qross.pql.Patterns.{$PARSE, $AS, ARROW}
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

    //express 是否支持嵌入式查询语句, 即 ${{ }} v
    def parse(PQL: PQL, express: Boolean = false): DataCell = {

        val (select, links) = {
            if (sentence.contains(ARROW)) {
                (sentence.takeBefore(ARROW), sentence.takeAfter(ARROW))
            }
            else {
                (sentence, "")
            }
        }

        var dataType = ""
        var path = {
            if ($AS.test(select)) {
                dataType = select.takeAfter($AS).trim()
                select.takeBetween($PARSE, $AS).trim()
            }
            else {
                select.takeAfter($PARSE).trim()
            }
        }

        if (express) {
            path = path.$express(PQL).$sharp(PQL).asText
        }
        else {
            path = path.$eval(PQL).asText
        }

        val data = {
            if (dataType != null) {
                dataType.trim().toUpperCase match {
                    case "TABLE" => PQL.dh.parseTable(path)
                    case "ROW" | "MAP" | "OBJECT" => PQL.dh.parseRow(path)
                    case "LIST" | "ARRAY" => PQL.dh.parseList(path)
                    case "VALUE" | "SINGLE" => PQL.dh.parseValue(path)
                    case _ => PQL.dh.parseTable(path)
                }
            }
            else {
                PQL.dh.parseTable(path)
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
        PQL.COUNT_OF_LAST_QUERY = if (data.isTable) data.asTable.size else if (data.isJavaList) data.asJavaList.size() else 1

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

/*
query = query.takeAfter("""^PARSE\s""".r).trim.removeQuotes()
output match {
    case "TABLE" => DataCell(PQL.dh.parseTable(query), DataType.TABLE)
    case "ROW" | "MAP" | "OBJECT" => DataCell(PQL.dh.parseRow(query), DataType.ROW)
    case "LIST" | "ARRAY" => DataCell(PQL.dh.parseList(query), DataType.ARRAY)
    case "VALUE" => PQL.dh.parseValue(query)
    case "AUTO" => DataCell(PQL.dh.parseNode(query), DataType.JSON)
}*/