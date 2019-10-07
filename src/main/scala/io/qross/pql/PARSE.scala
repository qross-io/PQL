package io.qross.pql

import io.qross.core.{DataCell, DataType}
import io.qross.ext.TypeExt._
import io.qross.net.Json._
import io.qross.pql.Patterns.{$PARSE, ARROW}
import io.qross.pql.Solver._

object PARSE {
    //用于PQL表达式解析
    def parse(sentence: String, PQL: PQL): Unit = {
        if ($PARSE.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("PARSE", sentence, new PARSE(sentence.takeAfter($PARSE).trim())))
        }
        else {
            throw new SQLParseException("Incorrect PARSE sentence: " + sentence)
        }
    }

}

class PARSE(val path: String) {

    def parse(PQL: PQL): DataCell = {

        val (select, links) =
            if (path.contains(ARROW)) {
                (path.takeBefore(ARROW), path.takeAfter(ARROW))
            }
            else {
                (path, "")
            }

        val data = DataCell(PQL.dh.parseTable(select), DataType.TABLE)
        if (links != "") {
            new SHARP(links, data).execute(PQL)
        }
        else {
            data
        }
    }

    def execute(PQL: PQL): Unit = {
        val table = PQL.dh.parseTable(this.path.$eval(PQL).asText)
        PQL.RESULT += table
        PQL.ROWS = table.size
        table.show()
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